use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    convert::Infallible,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::TcpListener,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Notify,
    task::JoinHandle,
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(20);
const HIGH_USAGE: u64 = 1_000_000;
const SUMMARY: &str = "Goal: keep the project anchor.";
const SUMMARY_MARKER: &str = "acp-go-nanocodex:summary:v1\n";
const SUMMARY_PREFIX: &str = "The earlier part of this conversation was compacted";
const SUMMARY_INSTRUCTION: &str = "Context checkpoint.";

struct Helper {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    events: Vec<Value>,
}

impl Helper {
    fn start(workspace: &Path, native_home: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_acp-go-nanocodex-native"))
            .current_dir(workspace)
            .env("CODEX_HOME", native_home)
            .env_remove("OPENAI_API_KEY")
            .env_remove("OPENAI_BASE_URL")
            .env_remove("NANOCODEX_MODEL_ID_PREFIX")
            .env_remove("NANOCODEX_TRANSPORT")
            .env_remove("NANOCODEX_API_KEY_ENV")
            .env("COMPACTION_FIXTURE_KEY", "fixture-token")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("start helper");
        Self {
            input: child.stdin.take().expect("helper stdin"),
            output: BufReader::new(child.stdout.take().expect("helper stdout")).lines(),
            child,
            events: Vec::new(),
        }
    }

    async fn send(&mut self, id: u64, method: &str, params: Value) {
        let mut frame = serde_json::to_vec(&json!({"id":id,"method":method,"params":params}))
            .expect("encode request");
        frame.push(b'\n');
        self.input.write_all(&frame).await.expect("write request");
        self.input.flush().await.expect("flush request");
    }

    async fn next(&mut self) -> Value {
        let line = timeout(DEADLINE, self.output.next_line())
            .await
            .expect("helper frame deadline")
            .expect("read helper frame")
            .expect("helper exited before reply");
        serde_json::from_str(&line).expect("JSONL frame")
    }

    async fn reply(&mut self, id: u64) -> Value {
        loop {
            let frame = self.next().await;
            if frame.get("event").is_some() {
                self.events.push(frame);
            } else {
                assert_eq!(frame["id"], id);
                return frame;
            }
        }
    }

    async fn call(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(id, method, params).await;
        let frame = self.reply(id).await;
        assert!(frame.get("error").is_none(), "helper error: {frame}");
        frame["result"].clone()
    }

    async fn shutdown(mut self, id: u64) {
        self.call(id, "shutdown", json!({})).await;
        assert!(
            timeout(DEADLINE, self.child.wait())
                .await
                .expect("shutdown deadline")
                .expect("wait for helper")
                .success()
        );
    }
}

enum Reply {
    Text(&'static str, u64),
    Tool,
    Summary { text: &'static str, streamed: bool },
    InvalidSummary(&'static str),
    FailedEvent(Value),
    Reject,
    ServerError,
    RetryDelay,
    Hang,
}

struct ProviderState {
    requests: Vec<Value>,
    replies: VecDeque<Reply>,
}

#[derive(Clone)]
struct Fixture {
    state: Arc<Mutex<ProviderState>>,
    requested: Arc<Notify>,
}

struct Provider {
    base_url: String,
    fixture: Fixture,
    server: JoinHandle<()>,
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Provider {
    async fn start(replies: impl IntoIterator<Item = Reply>) -> Self {
        let fixture = Fixture {
            state: Arc::new(Mutex::new(ProviderState {
                requests: Vec::new(),
                replies: replies.into_iter().collect(),
            })),
            requested: Arc::new(Notify::new()),
        };
        let app = Router::new()
            .route("/v1/responses", post(respond))
            .with_state(fixture.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture");
        let base_url = format!(
            "http://{}/v1",
            listener.local_addr().expect("fixture address")
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fixture");
        });
        Self {
            base_url,
            fixture,
            server,
        }
    }

    fn requests(&self) -> Vec<Value> {
        self.fixture
            .state
            .lock()
            .expect("fixture state")
            .requests
            .clone()
    }

    async fn wait_for_requests(&self, count: usize) {
        timeout(DEADLINE, async {
            loop {
                let requested = self.fixture.requested.notified();
                if self.requests().len() >= count {
                    return;
                }
                requested.await;
            }
        })
        .await
        .expect("provider request deadline");
    }
}

fn sse(event: Value) -> String {
    format!("data: {event}\n\n")
}

async fn respond(
    State(fixture): State<Fixture>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    assert_eq!(headers["authorization"], "Bearer fixture-token");
    assert_eq!(body["model"], "provider/gpt-6.1-sol");
    assert_eq!(body["store"], false);
    assert_eq!(body["stream"], true);
    assert!(body.get("previous_response_id").is_none());
    let (reply, number) = {
        let mut state = fixture.state.lock().expect("fixture state");
        state.requests.push(body);
        (state.replies.pop_front(), state.requests.len())
    };
    fixture.requested.notify_one();
    let Some(reply) = reply else {
        return (StatusCode::BAD_REQUEST, "unexpected request").into_response();
    };
    if matches!(reply, Reply::RetryDelay) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "30")],
            Json(json!({"error":{"code":"server_error"}})),
        )
            .into_response();
    }
    if matches!(reply, Reply::ServerError) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":{"code":"server_error"}})),
        )
            .into_response();
    }
    if matches!(reply, Reply::Reject) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"message":"compaction rejected"}})),
        )
            .into_response();
    }
    let mut id = format!("resp_{number}");
    let created =
        sse(json!({"type":"response.created","response":{"id":id,"status":"in_progress"}}));
    if matches!(reply, Reply::Hang) {
        let stream =
            stream::once(async move { Ok::<_, Infallible>(created) }).chain(stream::pending());
        return (
            [("content-type", "text/event-stream")],
            Body::from_stream(stream),
        )
            .into_response();
    }
    if let Reply::FailedEvent(error) = &reply {
        let failed = sse(
            json!({"type":"response.failed","response":{"id":id,"status":"failed","error":error}}),
        );
        return (
            [("content-type", "text/event-stream")],
            [created, failed].concat(),
        )
            .into_response();
    }
    let mut events = vec![created];
    let mut status = "completed";
    let summary_items = |text: &str| {
        vec![
            json!({"type":"reasoning","id":format!("rs_{number}"),"summary":[
                {"type":"summary_text","text":"internal summary reasoning"}
            ]}),
            json!({"type":"message","id":format!("msg_{number}"),"role":"assistant","status":"completed",
                "content":[{"type":"output_text","text":text,"annotations":[]}]}),
        ]
    };
    let (output, total_tokens) = match reply {
        Reply::Text(text, tokens) => (
            json!([{
                "type":"message","id":format!("msg_{number}"),"role":"assistant","status":"completed",
                "content":[{"type":"output_text","text":text,"annotations":[]}]
            }]),
            tokens,
        ),
        Reply::Tool => (
            json!([{
                "type":"function_call","id":"fc_compaction","call_id":"call_compaction","name":"exec_command","status":"completed",
                "arguments":json!({"cmd":"printf tool-committed >> compaction-tool.txt; printf tool-committed","login":false}).to_string()
            }]),
            HIGH_USAGE,
        ),
        Reply::Summary { text, streamed } => {
            let items = summary_items(text);
            events.push(sse(json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"internal summary reasoning"})));
            events.push(sse(
                json!({"type":"response.output_text.delta","output_index":1,"delta":text}),
            ));
            if streamed {
                for (index, item) in items.iter().enumerate() {
                    events.push(sse(json!({"type":"response.output_item.done","output_index":index,"item":item})));
                }
                (json!([]), 15)
            } else {
                (json!(items), 15)
            }
        }
        Reply::InvalidSummary(kind) => {
            let mut output = json!(summary_items(SUMMARY));
            match kind {
                "tool" => {
                    output[1] = json!({
                        "type":"function_call","id":"fc_summary","call_id":"call_summary","name":"exec_command","status":"completed",
                        "arguments":json!({"cmd":"printf summary-tool >> compaction-tool.txt","login":false}).to_string()
                    });
                }
                "empty" => output[1]["content"][0]["text"] = json!(" \n "),
                "missing" => output = json!([output[0].clone()]),
                "empty_id" => id.clear(),
                "incomplete" => status = "incomplete",
                "incomplete_event" => {
                    events.push(sse(json!({
                        "type":"response.incomplete","response":{
                            "id":id,"status":"incomplete","output":output,
                            "incomplete_details":{"reason":"max_output_tokens"}
                        }
                    })));
                    return ([("content-type", "text/event-stream")], events.concat())
                        .into_response();
                }
                _ => panic!("unknown invalid summary fixture"),
            }
            (output, 15)
        }
        Reply::Reject
        | Reply::ServerError
        | Reply::RetryDelay
        | Reply::Hang
        | Reply::FailedEvent(_) => unreachable!(),
    };
    events.push(sse(json!({
        "type":"response.completed","response":{
            "id":id,"status":status,"output":output,
            "usage":{"input_tokens":total_tokens - 3,"output_tokens":3,"total_tokens":total_tokens}
        }
    })));
    ([("content-type", "text/event-stream")], events.concat()).into_response()
}

fn config(provider: &Provider) -> Value {
    json!({
        "sessionId":uuid::Uuid::now_v7().to_string(),"model":"gpt-6.1-sol","thinking":"low",
        "apiBaseUrl":provider.base_url,"transport":"https","modelIdPrefix":"provider","apiKeyEnv":"COMPACTION_FIXTURE_KEY"
    })
}

fn prompt(text: &str) -> Value {
    json!({"content":[{"type":"text","text":text}]})
}

fn input_items(request: &Value, kind: &str) -> Vec<Value> {
    request["input"]
        .as_array()
        .expect("request input")
        .iter()
        .filter(|item| item["type"] == kind)
        .cloned()
        .collect()
}

fn rollout_records(state: &Value) -> Vec<Value> {
    let bytes =
        std::fs::read(state["rolloutPath"].as_str().expect("rollout path")).expect("read rollout");
    let committed = usize::try_from(state["committedBytes"].as_u64().expect("committed bytes"))
        .expect("committed size");
    std::str::from_utf8(&bytes[..committed])
        .expect("rollout UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("rollout record"))
        .collect()
}

fn message_texts(request: &Value, role: &str) -> Vec<String> {
    input_items(request, "message")
        .iter()
        .filter(|item| item["role"] == role)
        .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
        .filter_map(|part| part["text"].as_str().map(str::to_owned))
        .collect()
}

fn summaries(request: &Value) -> Vec<String> {
    message_texts(request, "user")
        .into_iter()
        .filter(|text| text.starts_with(SUMMARY_PREFIX))
        .collect()
}

fn saved_session(rollout: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    std::fs::read_dir(rollout.parent().expect("rollout directory"))
        .expect("read rollout directory")
        .map(|entry| entry.expect("rollout directory entry").path())
        .filter(|path| path.is_file())
        .map(|path| {
            let bytes = std::fs::read(&path).expect("read saved session file");
            (path, bytes)
        })
        .collect()
}

fn restore_into_empty_home(native_home: &Path, saved: &[(PathBuf, Vec<u8>)]) {
    std::fs::remove_dir_all(native_home).unwrap();
    for (path, bytes) in saved {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
}

/// A summary request sends the preceding generation's body and prefix, with
/// tools disabled, an output bound, and the instruction in place of the native
/// trigger.
fn assert_summary_request(request: &Value, generation: &Value) {
    for field in [
        "model",
        "tools",
        "reasoning",
        "include",
        "parallel_tool_calls",
        "store",
        "stream",
    ] {
        assert_eq!(request[field], generation[field], "summary changed {field}");
    }
    let mut bounded = generation.clone();
    bounded["max_output_tokens"] = json!(16_384);
    let keys = |body: &Value| {
        body.as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(request), keys(&bounded));
    assert_eq!(request["max_output_tokens"], 16_384);
    assert!(generation.get("max_output_tokens").is_none());
    assert_eq!(generation["tool_choice"], "auto");
    assert_eq!(request["tool_choice"], "none");
    assert!(input_items(request, "compaction_trigger").is_empty());
    let (instruction, history) = request["input"].as_array().unwrap().split_last().unwrap();
    assert_eq!(instruction["type"], "message");
    assert_eq!(instruction["role"], "user");
    assert!(
        instruction["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with(SUMMARY_INSTRUCTION)
    );
    let prefix = generation["input"].as_array().unwrap();
    assert_eq!(&history[..prefix.len()], prefix.as_slice());
}

fn assert_compacted_request(request: &Value, summary: &str) {
    assert!(input_items(request, "compaction").is_empty());
    assert!(input_items(request, "compaction_trigger").is_empty());
    assert_eq!(request["tool_choice"], "auto");
    assert!(request.get("max_output_tokens").is_none());
    let summaries = summaries(request);
    assert_eq!(summaries.len(), 1, "{request}");
    assert!(summaries[0].ends_with(&format!("<summary>\n{summary}\n</summary>")));
    let body = request.to_string();
    for removed in [
        "old assistant details",
        SUMMARY_INSTRUCTION,
        "internal summary reasoning",
    ] {
        assert!(!body.contains(removed), "compacted request kept {removed}");
    }
}

#[tokio::test]
async fn automatic_compaction_replays_reduced_history_after_restart() {
    for streamed in [true, false] {
        let provider = Provider::start([
            Reply::Text("old assistant details", HIGH_USAGE),
            Reply::Summary {
                text: SUMMARY,
                streamed,
            },
            Reply::Text("continued after compaction", 15),
            Reply::Text("continued after restart", 15),
        ])
        .await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        let mut config = config(&provider);
        let initialized = helper.call(1, "initialize", config.clone()).await;
        helper
            .call(2, "prompt", prompt("remember the project anchor"))
            .await;
        helper.shutdown(3).await;
        let rollout = Path::new(initialized["rolloutPath"].as_str().unwrap());
        let saved = std::fs::read(rollout).unwrap();
        let checkpoint_path = format!("{}.acp-checkpoint.json", rollout.display());
        let checkpoint: Value =
            serde_json::from_slice(&std::fs::read(&checkpoint_path).unwrap()).unwrap();
        assert_eq!(checkpoint["head"]["history"], json!([]));
        assert!(!String::from_utf8_lossy(&saved).contains("acp_checkpoint"));

        restore_into_empty_home(native_home.path(), &saved_session(rollout));
        config["resumeSessionId"] = initialized["nativeSessionId"].clone();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config.clone()).await;
        let completed = helper
            .call(2, "prompt", prompt("continue after compaction"))
            .await;
        assert_eq!(completed["finalMessage"], "continued after compaction");
        assert_eq!(completed["usage"]["totalTokens"], 30);
        assert!(
            helper
                .events
                .iter()
                .any(|event| event["data"]["type"] == "model.compaction.completed")
        );
        let presented = json!(
            helper
                .events
                .iter()
                .filter(|event| event["event"] != "native")
                .collect::<Vec<_>>()
        )
        .to_string();
        assert!(!presented.contains(SUMMARY));
        assert!(!presented.contains("internal summary reasoning"));
        let records = rollout_records(&completed);
        let replacement = records
            .iter()
            .find(|record| record["type"] == "compacted")
            .expect("durable compaction");
        let compactions = replacement["payload"]["replacement_history"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "compaction")
            .collect::<Vec<_>>();
        assert_eq!(compactions.len(), 1);
        assert_eq!(
            compactions[0]["encrypted_content"],
            format!("{SUMMARY_MARKER}{SUMMARY}")
        );
        helper.shutdown(3).await;

        let requests = provider.requests();
        assert_eq!(requests.len(), 3);
        assert_summary_request(&requests[1], &requests[0]);
        assert!(requests[1].to_string().contains("old assistant details"));
        assert_compacted_request(&requests[2], SUMMARY);
        for retained in ["remember the project anchor", "continue after compaction"] {
            assert!(requests[2].to_string().contains(retained));
        }

        restore_into_empty_home(native_home.path(), &saved_session(rollout));
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config).await;
        helper
            .call(2, "prompt", prompt("continue after restart"))
            .await;
        helper.shutdown(3).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 4);
        assert_compacted_request(&requests[3], SUMMARY);
        for field in ["tools", "reasoning", "include"] {
            assert_eq!(requests[3][field], requests[2][field]);
        }
        let compacted = requests[2]["input"].as_array().unwrap();
        let restored = requests[3]["input"].as_array().unwrap();
        assert_eq!(&restored[..compacted.len()], compacted.as_slice());
        let appended = json!(&restored[compacted.len()..]).to_string();
        for continued in ["continued after compaction", "continue after restart"] {
            assert!(appended.contains(continued));
        }
    }
}

#[tokio::test]
async fn repeated_compaction_supersedes_the_previous_summary() {
    let provider = Provider::start([
        Reply::Text("old assistant details", HIGH_USAGE),
        Reply::Summary {
            text: "first checkpoint summary",
            streamed: false,
        },
        Reply::Text("middle assistant details", HIGH_USAGE),
        Reply::Summary {
            text: "second checkpoint summary",
            streamed: true,
        },
        Reply::Text("continued after second compaction", 15),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    helper.call(1, "initialize", config(&provider)).await;
    helper.call(2, "prompt", prompt("first input")).await;
    helper.call(3, "prompt", prompt("second input")).await;
    let completed = helper.call(4, "prompt", prompt("third input")).await;
    assert_eq!(
        completed["finalMessage"],
        "continued after second compaction"
    );
    let records = rollout_records(&completed);
    let compacted = records
        .iter()
        .filter(|record| record["type"] == "compacted")
        .collect::<Vec<_>>();
    assert_eq!(compacted.len(), 2);
    let installed = compacted[1]["payload"]["replacement_history"].to_string();
    assert!(installed.contains("second checkpoint summary"));
    assert!(!installed.contains("first checkpoint summary"));
    helper.shutdown(5).await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 5);
    assert_summary_request(&requests[1], &requests[0]);
    assert_compacted_request(&requests[2], "first checkpoint summary");
    assert_summary_request(&requests[3], &requests[2]);
    assert!(requests[3].to_string().contains("middle assistant details"));
    assert_compacted_request(&requests[4], "second checkpoint summary");
    let latest = requests[4].to_string();
    assert!(!latest.contains("first checkpoint summary"));
    assert!(!latest.contains("middle assistant details"));
    for retained in ["first input", "second input", "third input"] {
        assert!(latest.contains(retained));
    }
}

#[tokio::test]
async fn mid_turn_compaction_includes_completed_tool_output_without_reexecuting_tools() {
    let provider = Provider::start([
        Reply::Tool,
        Reply::Summary {
            text: SUMMARY,
            streamed: true,
        },
        Reply::Text("tool work retained", 15),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    helper.call(1, "initialize", config(&provider)).await;
    let completed = helper
        .call(2, "prompt", prompt("run the tool and retain its result"))
        .await;
    assert_eq!(completed["finalMessage"], "tool work retained");
    helper.shutdown(3).await;
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("compaction-tool.txt")).unwrap(),
        "tool-committed"
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert_summary_request(&requests[1], &requests[0]);
    let output = input_items(&requests[1], "function_call_output");
    assert_eq!(output.len(), 1);
    assert!(output[0].to_string().contains("tool-committed"));
    assert_compacted_request(&requests[2], SUMMARY);
    assert!(input_items(&requests[2], "function_call").is_empty());
    assert!(input_items(&requests[2], "function_call_output").is_empty());
}

#[tokio::test]
async fn failed_or_invalid_summary_preserves_history_for_restart() {
    for (kind, code, message) in [
        ("rejected", "native_error", "provider rejected the request"),
        (
            "tool",
            "native_error",
            "gateway compaction summary requested a tool",
        ),
        (
            "empty",
            "native_error",
            "gateway compaction summary was empty",
        ),
        (
            "missing",
            "native_error",
            "gateway compaction summary was empty",
        ),
        (
            "incomplete",
            "native_error",
            "gateway compaction summary was truncated",
        ),
        (
            "incomplete_event",
            "native_error",
            "gateway compaction summary was truncated",
        ),
        (
            "empty_id",
            "transport_error",
            "gateway response did not complete",
        ),
    ] {
        let provider = Provider::start([
            Reply::Text("old assistant details", HIGH_USAGE),
            if kind == "rejected" {
                Reply::Reject
            } else {
                Reply::InvalidSummary(kind)
            },
            Reply::Summary {
                text: SUMMARY,
                streamed: false,
            },
            Reply::Text("history survived failure", 15),
        ])
        .await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        let mut config = config(&provider);
        let initialized = helper.call(1, "initialize", config.clone()).await;
        helper
            .call(2, "prompt", prompt("retain original user input"))
            .await;
        helper.send(3, "prompt", prompt("trigger compaction")).await;
        let failed = helper.reply(3).await;
        assert_eq!(failed["error"]["code"], code, "{kind}: {failed}");
        assert_eq!(failed["error"]["message"], message, "{kind}: {failed}");
        let state = helper.call(4, "state", json!({})).await;
        assert!(
            !rollout_records(&state)
                .iter()
                .any(|record| record["type"] == "compacted")
        );
        helper.shutdown(5).await;
        assert!(!workspace.path().join("compaction-tool.txt").exists());
        assert_eq!(
            provider.requests().len(),
            2,
            "invalid summary must not retry: {kind}"
        );
        config["resumeSessionId"] = initialized["nativeSessionId"].clone();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config).await;
        helper
            .call(2, "prompt", prompt("resume preserved history"))
            .await;
        helper.shutdown(3).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[2]["tool_choice"], "none");
        for retained in ["retain original user input", "old assistant details"] {
            assert!(
                requests[2].to_string().contains(retained),
                "missing retained history after {kind}"
            );
        }
        assert_compacted_request(&requests[3], SUMMARY);
        assert!(requests[3].to_string().contains("resume preserved history"));
    }
}

#[tokio::test]
async fn transient_summary_failure_is_retried() {
    let provider = Provider::start([
        Reply::Text("old assistant details", HIGH_USAGE),
        Reply::ServerError,
        Reply::Summary {
            text: SUMMARY,
            streamed: false,
        },
        Reply::Text("continued after retry", 15),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    helper.call(1, "initialize", config(&provider)).await;
    helper
        .call(2, "prompt", prompt("retain original user input"))
        .await;
    let completed = helper
        .call(3, "prompt", prompt("continue after retry"))
        .await;
    assert_eq!(completed["finalMessage"], "continued after retry");
    helper.shutdown(4).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[1], requests[2]);
    assert_summary_request(&requests[2], &requests[0]);
    assert_compacted_request(&requests[3], SUMMARY);
}

#[tokio::test]
async fn cancellation_interrupts_compaction_and_keeps_history_for_restart() {
    for retry_delay in [false, true] {
        let provider = Provider::start([
            Reply::Text("old assistant details", HIGH_USAGE),
            if retry_delay {
                Reply::RetryDelay
            } else {
                Reply::Hang
            },
            Reply::Summary {
                text: SUMMARY,
                streamed: false,
            },
            Reply::Text("history survived cancellation", 15),
        ])
        .await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        let mut config = config(&provider);
        let initialized = helper.call(1, "initialize", config.clone()).await;
        helper
            .call(2, "prompt", prompt("retain original user input"))
            .await;
        helper
            .send(3, "prompt", prompt("pending prompt at compaction"))
            .await;
        provider.wait_for_requests(2).await;
        assert_eq!(provider.requests()[1]["tool_choice"], "none");
        tokio::time::sleep(Duration::from_millis(100)).await;
        helper.send(4, "cancel", json!({})).await;
        timeout(Duration::from_secs(3), async {
            let mut cancelled = false;
            let mut completed = false;
            while !cancelled || !completed {
                let frame = helper.next().await;
                if frame.get("event").is_some() {
                    continue;
                }
                assert!(frame.get("error").is_none(), "cancel failed: {frame}");
                match frame["id"].as_u64() {
                    Some(3) => {
                        assert_eq!(frame["result"]["stopReason"], "cancelled");
                        assert!(
                            !rollout_records(&frame["result"])
                                .iter()
                                .any(|record| record["type"] == "compacted")
                        );
                        completed = true;
                    }
                    Some(4) => {
                        assert_eq!(frame["result"]["cancelled"], true);
                        cancelled = true;
                    }
                    _ => panic!("unexpected cancellation reply: {frame}"),
                }
            }
        })
        .await
        .expect("cancellation must interrupt the summary promptly");
        helper.shutdown(5).await;
        assert_eq!(provider.requests().len(), 2);
        config["resumeSessionId"] = initialized["nativeSessionId"].clone();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config).await;
        helper
            .call(2, "prompt", prompt("resume after cancelled compaction"))
            .await;
        helper.shutdown(3).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[2]["tool_choice"], "none");
        for retained in [
            "retain original user input",
            "old assistant details",
            "pending prompt at compaction",
        ] {
            assert!(requests[2].to_string().contains(retained));
        }
        assert_compacted_request(&requests[3], SUMMARY);
        assert!(
            requests[3]
                .to_string()
                .contains("resume after cancelled compaction")
        );
    }
}

#[tokio::test]
async fn unmarked_compaction_items_are_sent_unchanged() {
    // Equal-length edits keep every recorded byte boundary valid.
    const MARKER: &str = r"acp-go-nanocodex:summary:v1\n";
    const PROVIDER_CONTENT: &str = r"native-provider-ciphertext!\n";
    assert_eq!(MARKER.len(), PROVIDER_CONTENT.len());
    let provider = Provider::start([
        Reply::Text("old assistant details", HIGH_USAGE),
        Reply::Summary {
            text: SUMMARY,
            streamed: false,
        },
        Reply::Text("continued after compaction", 15),
        Reply::Text("continued with provider content", 15),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    let mut config = config(&provider);
    let initialized = helper.call(1, "initialize", config.clone()).await;
    helper.call(2, "prompt", prompt("first input")).await;
    helper.call(3, "prompt", prompt("second input")).await;
    helper.shutdown(4).await;
    let rollout = Path::new(initialized["rolloutPath"].as_str().unwrap());
    let mut rewritten = 0;
    for (path, bytes) in saved_session(rollout) {
        let text = String::from_utf8(bytes).unwrap();
        rewritten += text.matches(MARKER).count();
        std::fs::write(path, text.replace(MARKER, PROVIDER_CONTENT)).unwrap();
    }
    assert!(rewritten > 0);

    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    helper.call(1, "initialize", config).await;
    helper.call(2, "prompt", prompt("third input")).await;
    helper.shutdown(3).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    let compactions = input_items(&requests[3], "compaction");
    assert_eq!(compactions.len(), 1);
    assert_eq!(
        compactions[0]["encrypted_content"],
        format!("native-provider-ciphertext!\n{SUMMARY}")
    );
    assert!(summaries(&requests[3]).is_empty());
}

#[tokio::test]
async fn observation_preserves_checkpoint_and_complete_final_records_without_newlines() {
    for native_only in [false, true] {
        let provider = Provider::start([
            Reply::Text("initial response", 15),
            Reply::Text("continued after observation", 15),
        ])
        .await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        let mut config = config(&provider);
        let initialized = helper.call(1, "initialize", config.clone()).await;
        helper
            .call(2, "prompt", prompt("preserve observation history"))
            .await;
        helper.shutdown(3).await;
        let path = Path::new(initialized["rolloutPath"].as_str().unwrap());
        let saved = std::fs::read_to_string(path).unwrap();
        if native_only {
            std::fs::remove_file(format!("{}.acp-checkpoint.json", path.display())).unwrap();
        }
        let unterminated = saved.trim_end_matches('\n');
        std::fs::write(path, unterminated).unwrap();
        config["resumeSessionId"] = initialized["nativeSessionId"].clone();

        for _ in 0..2 {
            let mut helper = Helper::start(workspace.path(), native_home.path());
            helper.call(1, "initialize", config.clone()).await;
            helper.shutdown(2).await;
            let observed = std::fs::read_to_string(path).unwrap();
            assert!(
                observed == format!("{unterminated}\n"),
                "observation changed stored history; native_only={native_only}"
            );
        }
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config).await;
        helper
            .call(2, "prompt", prompt("continue after observation"))
            .await;
        helper.shutdown(3).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[1]
                .to_string()
                .contains("preserve observation history")
        );
        assert!(requests[1].to_string().contains("initial response"));
        for row in std::fs::read_to_string(path).unwrap().lines() {
            serde_json::from_str::<Value>(row).expect("each native record remains complete");
        }
    }
}

#[tokio::test]
async fn later_native_records_invalidate_checkpoint_accounting() {
    let provider = Provider::start([
        Reply::Text("old assistant details", HIGH_USAGE),
        Reply::Text("continued after native change", 15),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    let mut config = config(&provider);
    let initialized = helper.call(1, "initialize", config.clone()).await;
    helper
        .call(2, "prompt", prompt("retain before external continuation"))
        .await;
    helper.shutdown(3).await;
    let path = Path::new(initialized["rolloutPath"].as_str().unwrap());
    let mut saved = std::fs::read_to_string(path).unwrap();
    let checkpoint: Value = serde_json::from_str(saved.lines().last().unwrap()).unwrap();
    saved.push_str(&json!({
        "timestamp":checkpoint["timestamp"],"type":"response_item",
        "payload":{"type":"message","id":"msg_external","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"external native continuation","annotations":[]}]}
    }).to_string());
    std::fs::write(path, saved).unwrap();
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    helper.call(1, "initialize", config).await;
    helper
        .call(2, "prompt", prompt("continue externally updated history"))
        .await;
    helper.shutdown(3).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1]["tool_choice"], "auto");
    assert!(
        requests[1]
            .to_string()
            .contains("external native continuation")
    );
    assert!(requests[1].to_string().contains("old assistant details"));
}

#[tokio::test]
async fn checkpoint_mismatches_fall_back_without_changing_native_history() {
    let provider = Provider::start([Reply::Text("retained response", 15)]).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    let mut config = config(&provider);
    let initialized = helper.call(1, "initialize", config.clone()).await;
    helper
        .call(2, "prompt", prompt("retain checkpoint binding"))
        .await;
    helper.shutdown(3).await;
    let path = Path::new(initialized["rolloutPath"].as_str().unwrap());
    let saved = std::fs::read_to_string(path).unwrap();
    let checkpoint_path = format!("{}.acp-checkpoint.json", path.display());
    let checkpoint: Value =
        serde_json::from_slice(&std::fs::read(&checkpoint_path).unwrap()).unwrap();
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    for field in [
        "version",
        "model",
        "lineage_id",
        "prompt_cache_key",
        "workspace",
        "preceding_bytes",
        "history_items",
        "prefix",
        "missing_head",
        "invalid_json",
    ] {
        let mut invalid = checkpoint.clone();
        match field {
            "version" => invalid["head"][field] = json!(99),
            "preceding_bytes" | "history_items" => invalid[field] = json!(0),
            "prefix" => invalid["prefix"] = json!([{"type":"unknown"}]),
            "missing_head" => invalid["head"].as_object_mut().unwrap().clear(),
            "invalid_json" => {}
            _ => invalid["head"][field] = json!("mismatched checkpoint"),
        }
        let corrupt = if field == "invalid_json" {
            "{".to_string()
        } else {
            invalid.to_string()
        };
        std::fs::write(&checkpoint_path, &corrupt).unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.send(1, "initialize", config.clone()).await;
        let result = helper.reply(1).await;
        assert!(
            result.get("result").is_some(),
            "mismatched {field} prevented native restore: {result}"
        );
        helper.shutdown(2).await;
        assert_eq!(std::fs::read_to_string(path).unwrap(), saved);
    }
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn checkpoint_write_failure_does_not_fail_a_committed_turn() {
    let provider = Provider::start([Reply::Text("committed response", 15)]).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    let initialized = helper.call(1, "initialize", config(&provider)).await;
    let path = Path::new(initialized["rolloutPath"].as_str().unwrap());
    std::fs::create_dir(format!("{}.acp-checkpoint.json", path.display())).unwrap();
    helper
        .call(
            2,
            "prompt",
            prompt("commit despite optional metadata failure"),
        )
        .await;
    helper.shutdown(3).await;
    assert!(
        std::fs::read_to_string(path)
            .unwrap()
            .contains("committed response")
    );
}

#[tokio::test]
async fn writer_lock_survives_until_native_shutdown() {
    let provider = Provider::start([Reply::Text("saved reply", 15)]).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut first = Helper::start(workspace.path(), native_home.path());
    let mut config = config(&provider);
    let initialized = first.call(1, "initialize", config.clone()).await;
    first
        .call(2, "prompt", prompt("keep native writer alive"))
        .await;
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    let mut second = Helper::start(workspace.path(), native_home.path());
    second.send(1, "initialize", config.clone()).await;
    assert_eq!(second.reply(1).await["error"]["code"], "restore_failed");
    second.shutdown(2).await;
    first.shutdown(3).await;
    let mut third = Helper::start(workspace.path(), native_home.path());
    third.call(1, "initialize", config).await;
    third.shutdown(2).await;
}

#[tokio::test]
async fn cancel_and_shutdown_interrupt_compaction_retry_delay() {
    for shutdown in [false, true] {
        let provider = Provider::start([
            Reply::Text("retained history", HIGH_USAGE),
            Reply::RetryDelay,
        ])
        .await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config(&provider)).await;
        helper.call(2, "prompt", prompt("fill context")).await;
        helper
            .send(3, "prompt", prompt("compact before continuing"))
            .await;
        provider.wait_for_requests(2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let method = if shutdown { "shutdown" } else { "cancel" };
        helper.send(4, method, json!({})).await;
        timeout(Duration::from_secs(3), async {
            let mut prompt_done = false;
            let mut control_done = false;
            while !prompt_done || !control_done {
                let frame = helper.next().await;
                if frame.get("event").is_some() {
                    continue;
                }
                assert!(frame.get("error").is_none(), "control failure: {frame}");
                match frame["id"].as_u64() {
                    Some(3) => {
                        assert_eq!(frame["result"]["stopReason"], "cancelled");
                        prompt_done = true;
                    }
                    Some(4) => {
                        if !shutdown {
                            assert_eq!(frame["result"]["cancelled"], true);
                        }
                        control_done = true;
                    }
                    _ => panic!("unexpected reply: {frame}"),
                }
            }
        })
        .await
        .expect("retry delay must be interruptible");
        if shutdown {
            assert!(
                timeout(DEADLINE, helper.child.wait())
                    .await
                    .unwrap()
                    .unwrap()
                    .success()
            );
        } else {
            helper.shutdown(5).await;
        }
        assert_eq!(provider.requests().len(), 2);
    }
}

#[tokio::test]
async fn codeless_failed_summary_event_is_retried_before_history_changes() {
    let provider = Provider::start([
        Reply::Text("old assistant details", HIGH_USAGE),
        Reply::FailedEvent(json!({"message":"summary upstream failed"})),
        Reply::Summary {
            text: SUMMARY,
            streamed: false,
        },
        Reply::Text("continued after summary retry", 15),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    helper.call(1, "initialize", config(&provider)).await;
    helper
        .call(2, "prompt", prompt("retain original user input"))
        .await;
    let completed = helper
        .call(3, "prompt", prompt("continue after summary retry"))
        .await;
    assert_eq!(completed["finalMessage"], "continued after summary retry");
    helper.shutdown(4).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    assert_summary_request(&requests[1], &requests[0]);
    assert_eq!(requests[1], requests[2], "summary retry changed history");
    assert_compacted_request(&requests[3], SUMMARY);
}
