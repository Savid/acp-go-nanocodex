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
    path::Path,
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
const SUMMARY: &str = "opaque-provider-compaction";

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
    Compact {
        streamed: bool,
    },
    CompactWithPresentation {
        streamed: bool,
        completion_items: bool,
    },
    InvalidCompact(&'static str),
    Reject,
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
    let mut events = vec![created];
    let compact =
        json!({"type":"compaction","id":format!("cmp_{number}"),"encrypted_content":SUMMARY});
    let mut status = "completed";
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
        Reply::Compact { streamed } => {
            events.push(sse(json!({"type":"response.output_text.delta","output_index":0,"delta":"internal compaction text"})));
            events.push(sse(json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"internal compaction reasoning"})));
            if streamed {
                events.push(sse(
                    json!({"type":"response.output_item.done","output_index":0,"item":compact}),
                ));
                (json!([]), 15)
            } else {
                (json!([compact]), 15)
            }
        }
        Reply::CompactWithPresentation {
            streamed,
            completion_items,
        } => {
            let items = vec![
                json!({"type":"reasoning","id":"rs_compaction","summary":[
                    {"type":"summary_text","text":"internal compaction reasoning"}
                ]}),
                compact,
                json!({"type":"message","id":"msg_compaction","role":"assistant","content":[
                    {"type":"output_text","text":"internal compaction text","annotations":[]}
                ]}),
            ];
            if streamed {
                events.push(sse(json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"internal compaction reasoning"})));
                events.push(sse(json!({"type":"response.output_text.delta","output_index":2,"delta":"internal compaction text"})));
                for (index, item) in items.iter().enumerate() {
                    events.push(sse(json!({"type":"response.output_item.done","output_index":index,"item":item})));
                }
            }
            (
                if completion_items {
                    json!(items)
                } else {
                    json!([])
                },
                15,
            )
        }
        Reply::InvalidCompact(kind) => {
            let mut output = json!([compact]);
            match kind {
                "missing" => output = json!([]),
                "duplicate" => output = json!([compact, compact]),
                "empty_content" => output[0]["encrypted_content"] = json!(""),
                "empty_id" => id.clear(),
                "incomplete" => status = "incomplete",
                "streamed_duplicate" | "streamed_repeated_index" => {
                    events.push(sse(
                        json!({"type":"response.output_item.done","output_index":0,"item":compact}),
                    ));
                    let index = if kind == "streamed_duplicate" { 1 } else { 0 };
                    events.push(sse(json!({"type":"response.output_item.done","output_index":index,"item":compact})));
                }
                "streamed_conflict" => {
                    let mut streamed = compact.clone();
                    streamed["encrypted_content"] = json!("different-encrypted-context");
                    events.push(sse(json!({"type":"response.output_item.done","output_index":0,"item":streamed})));
                }
                _ => panic!("unknown invalid compaction fixture"),
            }
            (output, 15)
        }
        Reply::Reject | Reply::Hang => unreachable!(),
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

fn assert_compaction_request(request: &Value) {
    assert_eq!(input_items(request, "compaction_trigger").len(), 1);
    assert_eq!(
        request["input"].as_array().unwrap().last().unwrap()["type"],
        "compaction_trigger"
    );
}

fn assert_compacted_request(request: &Value) {
    let summaries = input_items(request, "compaction");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["encrypted_content"], SUMMARY);
    assert!(input_items(request, "compaction_trigger").is_empty());
    assert!(!request.to_string().contains("old assistant details"));
}

#[tokio::test]
async fn automatic_compaction_replays_reduced_history_after_restart() {
    for streamed in [true, false] {
        let provider = Provider::start([
            Reply::Text("old assistant details", HIGH_USAGE),
            Reply::Compact { streamed },
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
        let last: Value =
            serde_json::from_str(std::str::from_utf8(&saved).unwrap().lines().last().unwrap())
                .unwrap();
        assert_eq!(last["type"], "acp_checkpoint");
        assert_eq!(last["payload"]["head"]["history"], json!([]));

        std::fs::remove_dir_all(native_home.path()).unwrap();
        std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        std::fs::write(rollout, saved).unwrap();
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
        assert!(
            helper
                .events
                .iter()
                .filter(|event| event["event"] != "native")
                .all(|event| !event.to_string().contains("internal compaction"))
        );
        let records = rollout_records(&completed);
        let replacement = records
            .iter()
            .find(|record| record["type"] == "compacted")
            .expect("durable compaction");
        assert!(
            replacement["payload"]["replacement_history"]
                .to_string()
                .contains(SUMMARY)
        );
        helper.shutdown(3).await;

        let requests = provider.requests();
        assert_eq!(requests.len(), 3);
        assert_compaction_request(&requests[1]);
        assert!(requests[1].to_string().contains("old assistant details"));
        assert_compacted_request(&requests[2]);
        for retained in ["remember the project anchor", "continue after compaction"] {
            assert!(requests[2].to_string().contains(retained));
        }

        config["resumeSessionId"] = initialized["nativeSessionId"].clone();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config).await;
        helper
            .call(2, "prompt", prompt("continue after restart"))
            .await;
        helper.shutdown(3).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 4);
        assert_compacted_request(&requests[3]);
        for retained in [
            "remember the project anchor",
            "continue after compaction",
            "continue after restart",
        ] {
            assert!(requests[3].to_string().contains(retained));
        }
    }
}

#[tokio::test]
async fn compaction_ignores_accompanying_reasoning_and_assistant_items() {
    for (streamed, completion_items) in [(true, false), (false, true), (true, true)] {
        let provider = Provider::start([
            Reply::Text("old assistant details", HIGH_USAGE),
            Reply::CompactWithPresentation {
                streamed,
                completion_items,
            },
            Reply::Text("continued after compaction", 15),
        ])
        .await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.call(1, "initialize", config(&provider)).await;
        helper.call(2, "prompt", prompt("retain this input")).await;
        let completed = helper.call(3, "prompt", prompt("continue")).await;
        assert_eq!(completed["finalMessage"], "continued after compaction");
        assert!(
            helper
                .events
                .iter()
                .any(|event| event["data"]["type"] == "model.compaction.completed")
        );
        assert!(
            !json!(helper.events)
                .to_string()
                .contains("internal compaction")
        );
        assert!(
            !json!(rollout_records(&completed))
                .to_string()
                .contains("internal compaction")
        );
        helper.shutdown(4).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 3);
        assert_compaction_request(&requests[1]);
        assert_compacted_request(&requests[2]);
        assert!(!requests[2].to_string().contains("internal compaction"));
    }
}

#[tokio::test]
async fn mid_turn_compaction_includes_completed_tool_output_without_reexecuting_tools() {
    let provider = Provider::start([
        Reply::Tool,
        Reply::Compact { streamed: true },
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
    assert_compaction_request(&requests[1]);
    let output = input_items(&requests[1], "function_call_output");
    assert_eq!(output.len(), 1);
    assert!(output[0].to_string().contains("tool-committed"));
    assert_compacted_request(&requests[2]);
    assert!(input_items(&requests[2], "function_call").is_empty());
    assert!(input_items(&requests[2], "function_call_output").is_empty());
}

#[tokio::test]
async fn failed_or_invalid_compaction_preserves_history_for_restart() {
    for kind in [
        "rejected",
        "missing",
        "duplicate",
        "empty_content",
        "empty_id",
        "incomplete",
        "streamed_duplicate",
        "streamed_repeated_index",
        "streamed_conflict",
    ] {
        let provider = Provider::start([
            Reply::Text("old assistant details", HIGH_USAGE),
            if kind == "rejected" {
                Reply::Reject
            } else {
                Reply::InvalidCompact(kind)
            },
            Reply::Compact { streamed: false },
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
        assert!(
            failed.get("error").is_some(),
            "invalid compaction {kind} succeeded: {failed}"
        );
        let state = helper.call(4, "state", json!({})).await;
        assert!(
            !rollout_records(&state)
                .iter()
                .any(|record| record["type"] == "compacted")
        );
        helper.shutdown(5).await;
        assert_eq!(
            provider.requests().len(),
            2,
            "invalid compaction must not retry: {kind}"
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
        assert_compaction_request(&requests[2]);
        for retained in ["retain original user input", "old assistant details"] {
            assert!(
                requests[2].to_string().contains(retained),
                "missing retained history after {kind}"
            );
        }
        assert_compacted_request(&requests[3]);
        assert!(requests[3].to_string().contains("resume preserved history"));
    }
}

#[tokio::test]
async fn cancellation_interrupts_compaction_and_keeps_history_for_restart() {
    let provider = Provider::start([
        Reply::Text("old assistant details", HIGH_USAGE),
        Reply::Hang,
        Reply::Compact { streamed: false },
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
    assert_compaction_request(&provider.requests()[1]);
    helper.send(4, "cancel", json!({})).await;
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
    helper.shutdown(5).await;
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    let mut helper = Helper::start(workspace.path(), native_home.path());
    helper.call(1, "initialize", config).await;
    helper
        .call(2, "prompt", prompt("resume after cancelled compaction"))
        .await;
    helper.shutdown(3).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    assert_compaction_request(&requests[2]);
    for retained in [
        "retain original user input",
        "old assistant details",
        "pending prompt at compaction",
    ] {
        assert!(requests[2].to_string().contains(retained));
    }
    assert_compacted_request(&requests[3]);
    assert!(
        requests[3]
            .to_string()
            .contains("resume after cancelled compaction")
    );
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
        let mut rows = saved.lines().collect::<Vec<_>>();
        if native_only {
            let removed: Value = serde_json::from_str(rows.pop().unwrap()).unwrap();
            assert_eq!(removed["type"], "acp_checkpoint");
        }
        let unterminated = rows.join("\n");
        std::fs::write(path, &unterminated).unwrap();
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
    assert!(input_items(&requests[1], "compaction_trigger").is_empty());
    assert!(
        requests[1]
            .to_string()
            .contains("external native continuation")
    );
    assert!(requests[1].to_string().contains("old assistant details"));
}

#[tokio::test]
async fn checkpoint_identity_and_boundary_mismatches_fail_without_changing_rollout() {
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
    let final_start = saved.trim_end_matches('\n').rfind('\n').unwrap() + 1;
    let checkpoint: Value = serde_json::from_str(&saved[final_start..]).unwrap();
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    for field in [
        "version",
        "model",
        "lineage_id",
        "prompt_cache_key",
        "workspace",
        "preceding_bytes",
        "history_items",
    ] {
        let mut invalid = checkpoint.clone();
        match field {
            "version" => invalid["payload"]["head"][field] = json!(99),
            "preceding_bytes" | "history_items" => invalid["payload"][field] = json!(0),
            _ => invalid["payload"]["head"][field] = json!("mismatched checkpoint"),
        }
        let corrupt = format!("{}{invalid}", &saved[..final_start]);
        std::fs::write(path, &corrupt).unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path());
        helper.send(1, "initialize", config.clone()).await;
        let result = helper.reply(1).await;
        assert!(
            result.get("error").is_some(),
            "mismatched {field} was accepted"
        );
        helper.shutdown(2).await;
        assert_eq!(std::fs::read_to_string(path).unwrap(), corrupt);
    }
    assert_eq!(provider.requests().len(), 1);
}
