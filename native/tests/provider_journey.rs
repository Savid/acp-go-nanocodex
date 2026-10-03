use axum::{
    Json, Router,
    body::{Body, Bytes},
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
    task::JoinHandle,
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(20);

struct Helper {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    events: Vec<Value>,
}

impl Helper {
    fn start(workspace: &Path, native_home: &Path, key_env: &str) -> Self {
        Self::start_with_env(workspace, native_home, key_env, &[])
    }

    fn start_with_env(
        workspace: &Path,
        native_home: &Path,
        key_env: &str,
        extra: &[(&str, &str)],
    ) -> Self {
        Self::spawn(workspace, native_home, key_env, extra, Stdio::inherit())
    }

    fn spawn(
        workspace: &Path,
        native_home: &Path,
        key_env: &str,
        extra: &[(&str, &str)],
        stderr: Stdio,
    ) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_acp-go-nanocodex-native"))
            .current_dir(workspace)
            .env("CODEX_HOME", native_home)
            .env_remove("OPENAI_API_KEY")
            .env_remove("OPENAI_BASE_URL")
            .env_remove("NANOCODEX_MODEL_ID_PREFIX")
            .env_remove("NANOCODEX_TRANSPORT")
            .env_remove("NANOCODEX_API_KEY_ENV")
            .env(key_env, "fixture-bearer-token")
            .envs(extra.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
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
        let mut encoded =
            serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))
                .expect("encode request");
        encoded.push(b'\n');
        self.input.write_all(&encoded).await.expect("write request");
        self.input.flush().await.expect("flush request");
    }

    async fn next(&mut self) -> Value {
        let line = timeout(DEADLINE, self.output.next_line())
            .await
            .expect("helper frame deadline")
            .expect("read helper frame")
            .expect("helper exited before reply");
        serde_json::from_str(&line).expect("helper stdout must contain JSONL only")
    }

    async fn reply(&mut self, id: u64) -> Value {
        loop {
            let frame = self.next().await;
            if frame.get("event").is_some() {
                self.events.push(frame);
                continue;
            }
            assert_eq!(frame["id"], id, "reply ID");
            return frame;
        }
    }

    async fn call(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(id, method, params).await;
        let frame = self.reply(id).await;
        assert!(
            frame.get("error").is_none(),
            "helper rejected request: {frame}; events: {:?}",
            self.events
        );
        frame["result"].clone()
    }

    async fn shutdown(mut self, id: u64) {
        self.call(id, "shutdown", json!({})).await;
        let status = timeout(DEADLINE, self.child.wait())
            .await
            .expect("shutdown deadline")
            .expect("wait for shutdown");
        assert!(status.success(), "helper shutdown failed: {status}");
    }
}

#[derive(Clone, Debug)]
struct ObservedRequest {
    headers: HeaderMap,
    body: Value,
}

enum Reply {
    Text(&'static str),
    TextWithoutCreated(&'static str),
    CompletedText(&'static str),
    IdlessShell,
    IdlessText {
        streamed: bool,
    },
    Shell,
    Hang,
    Rejected,
    HttpError {
        status: StatusCode,
        retry_after: &'static str,
        error: Value,
    },
    HttpBody(Value),
    NonSse(&'static str, &'static str),
    RawSse(Bytes),
    ErrorEvent {
        code: String,
        nested: bool,
    },
    PartialShell,
    Dropped,
    Oversized,
    LineEndings {
        newline: &'static str,
        fragmented: bool,
    },
}

struct ProviderState {
    requests: Vec<ObservedRequest>,
    replies: VecDeque<Reply>,
}

struct Provider {
    base_url: String,
    state: Arc<Mutex<ProviderState>>,
    server: JoinHandle<()>,
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Provider {
    async fn start(replies: impl IntoIterator<Item = Reply>) -> Self {
        let state = Arc::new(Mutex::new(ProviderState {
            requests: Vec::new(),
            replies: replies.into_iter().collect(),
        }));
        let app = Router::new()
            .route("/v1/responses", post(respond).get(|| async {
                (StatusCode::BAD_REQUEST, Json(json!({"error":{"message":"provider body includes fixture-bearer-token"}})))
            }))
            .with_state(state.clone());
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
            state,
            server,
        }
    }

    fn requests(&self) -> Vec<ObservedRequest> {
        self.state.lock().expect("fixture state").requests.clone()
    }
}

fn sse(event: Value) -> Bytes {
    Bytes::from(format!(
        "event: {}\ndata: {event}\n\n",
        event["type"].as_str().expect("event type")
    ))
}

fn complete(id: &str, output: Value) -> Value {
    json!({
        "type": "response.completed",
        "response": {
            "id": id, "status": "completed", "output": output,
            "usage": {
                "input_tokens": 12, "output_tokens": 3, "total_tokens": 15,
                "input_tokens_details": {"cached_tokens": 4},
                "output_tokens_details": {"reasoning_tokens": 1}
            }
        }
    })
}

async fn respond(
    State(state): State<Arc<Mutex<ProviderState>>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let identified_client = headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("acp-go-nanocodex/"));
    if !identified_client {
        return (StatusCode::BAD_REQUEST, "gateway requires client identity").into_response();
    }
    assert!(!headers.contains_key("x-opencode-session"));
    let standard_tools = body["tools"].as_array().is_some_and(|tools| {
        tools.iter().all(|tool| tool["type"] == "function")
            && tools.iter().any(|tool| tool["name"] == "exec_command")
    });
    let standard_input = body["input"].as_array().is_some_and(|items| {
        items.iter().all(|item| {
            matches!(
                item["type"].as_str(),
                Some("message" | "reasoning" | "function_call" | "function_call_output")
            )
        })
    });
    if !standard_tools || !standard_input {
        return (
            StatusCode::BAD_REQUEST,
            "gateway requires top-level function tools and standard Responses items",
        )
            .into_response();
    }
    let (reply, number) = {
        let mut state = state.lock().expect("fixture state");
        state.requests.push(ObservedRequest { headers, body });
        (state.replies.pop_front(), state.requests.len())
    };
    let Some(reply) = reply else {
        return (StatusCode::BAD_REQUEST, "unexpected model request").into_response();
    };
    if let Reply::HttpError {
        status,
        retry_after,
        error,
    } = &reply
    {
        return (
            *status,
            [("retry-after", *retry_after)],
            Json(json!({
                "error": error
            })),
        )
            .into_response();
    }
    if let Reply::NonSse(content_type, body) = &reply {
        return ([("content-type", *content_type)], *body).into_response();
    }
    if let Reply::HttpBody(body) = &reply {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "0")],
            Json(body.clone()),
        )
            .into_response();
    }
    if let Reply::RawSse(body) = &reply {
        return ([("content-type", "text/event-stream")], body.clone()).into_response();
    }
    if matches!(reply, Reply::Rejected) {
        return (StatusCode::BAD_REQUEST, Json(json!({
            "error": {"message":"provider body includes fixture-bearer-token", "type":"invalid_request_error"}
        }))).into_response();
    }
    if let Reply::ErrorEvent { code, nested } = &reply {
        let error = json!({"code":code, "message":"provider body includes fixture-bearer-token"});
        let event = if *nested {
            json!({"type":"response.failed", "response":{"error":error}})
        } else {
            json!({"type":"error", "error":error})
        };
        return (
            [("content-type", "text/event-stream")],
            Body::from(sse(event)),
        )
            .into_response();
    }
    if matches!(reply, Reply::Oversized) {
        let chunks = stream::iter(
            (0..33).map(|_| Ok::<_, Infallible>(Bytes::from(vec![b'x'; 1024 * 1024]))),
        );
        return (
            [("content-type", "text/event-stream")],
            Body::from_stream(chunks),
        )
            .into_response();
    }
    let id = format!("resp_{number}");
    let created =
        sse(json!({"type":"response.created", "response":{"id":id,"status":"in_progress"}}));
    if matches!(reply, Reply::Dropped) {
        // The pause lets the status line and first event reach the client.
        let chunks = stream::iter([
            Ok(created),
            Err(std::io::Error::other("connection dropped")),
        ])
        .then(|chunk| async move {
            if chunk.is_err() {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            chunk
        });
        return (
            [("content-type", "text/event-stream")],
            Body::from_stream(chunks),
        )
            .into_response();
    }
    if let Reply::LineEndings {
        newline,
        fragmented,
    } = reply
    {
        let message = json!({"type":"message", "role":"assistant", "content":[{"type":"output_text", "text":"line endings accepted"}]});
        let completed = complete(&id, json!([message]));
        let body = format!("event: response.completed{newline}data: {completed}{newline}{newline}");
        let pieces = if fragmented {
            body.bytes()
                .map(|byte| Bytes::from(vec![byte]))
                .collect::<Vec<_>>()
        } else {
            vec![Bytes::from(body)]
        };
        return (
            [("content-type", "text/event-stream")],
            Body::from_stream(stream::iter(pieces.into_iter().map(Ok::<_, Infallible>))),
        )
            .into_response();
    }
    let bytes = match reply {
        Reply::Text(text) | Reply::TextWithoutCreated(text) | Reply::CompletedText(text) => {
            let item = json!({"id":format!("msg_{number}"),"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]});
            let mut events = Vec::new();
            let mut output = Vec::new();
            if matches!(reply, Reply::Text(_)) {
                events.push(created);
            }
            if !matches!(reply, Reply::CompletedText(_)) {
                events.push(sse(json!({"type":"response.reasoning_summary_text.delta","item_id":format!("rs_{number}"),"output_index":0,"summary_index":0,"delta":"visible reasoning"})));
                events.push(sse(
                    json!({"type":"response.output_text.delta","item_id":format!("msg_{number}"),"output_index":1,"content_index":0,"delta":text}),
                ));
                output.push(json!({"id":format!("rs_{number}"),"type":"reasoning","summary":[{"type":"summary_text","text":"visible reasoning"}]}));
            }
            output.push(item);
            events.push(sse(complete(&id, json!(output))));
            events
        }
        Reply::IdlessShell => vec![
            created,
            sse(complete(
                &id,
                json!([
                    {"type":"message","role":"assistant","content":[{"type":"output_text","text":"Checking"}]},
                    {"type":"function_call","call_id":"call_idless","name":"exec_command","arguments":"{\"cmd\":\"printf fixture-idless-tool\",\"login\":false}"}
                ]),
            )),
        ],
        Reply::IdlessText { streamed } => {
            let item = json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"Checking complete"}]});
            let mut events = Vec::new();
            if streamed {
                events.push(sse(json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"Checking"})));
                events.push(sse(
                    json!({"type":"response.output_item.done","output_index":0,"item":item}),
                ));
            }
            events.push(sse(complete(&id, json!([item.clone(), item]))));
            events
        }
        Reply::Shell => vec![
            created,
            sse(complete(
                &id,
                json!([{
                    "id":"fc_shell", "type":"function_call", "call_id":"call_shell",
                    "name":"exec_command", "status":"completed",
                    "arguments":json!({"cmd":"printf 'fixture-file-value' > provider-tool.txt; printf 'fixture-tool-output'", "login":false}).to_string()
                }]),
            )),
        ],
        Reply::Hang => {
            let first = stream::iter(vec![
                Ok::<_, Infallible>(created),
                Ok(sse(
                    json!({"type":"response.output_text.delta","item_id":"msg_hang","output_index":0,"content_index":0,"delta":"still working"}),
                )),
            ]);
            return (
                [("content-type", "text/event-stream")],
                Body::from_stream(first.chain(stream::pending())),
            )
                .into_response();
        }
        Reply::PartialShell => vec![
            created,
            sse(json!({
                "type":"response.output_item.done", "output_index":0,
                "item":{"id":"fc_partial", "type":"function_call", "name":"exec_command",
                    "call_id":"call_partial", "arguments":json!({"cmd":"touch must-not-run", "login":false}).to_string()}
            })),
        ],
        Reply::Rejected
        | Reply::HttpError { .. }
        | Reply::HttpBody(_)
        | Reply::NonSse(_, _)
        | Reply::RawSse(_)
        | Reply::ErrorEvent { .. }
        | Reply::Dropped
        | Reply::Oversized
        | Reply::LineEndings { .. } => unreachable!(),
    };
    let stream = stream::iter(bytes.into_iter().map(Ok::<_, Infallible>));
    (
        [("content-type", "text/event-stream")],
        Body::from_stream(stream),
    )
        .into_response()
}

fn initialize(provider: &Provider, key_env: &str, prefix: &str) -> Value {
    json!({
        "sessionId": uuid::Uuid::now_v7().to_string(),
        "model": "gpt-6.1-sol", "thinking": "low", "transport": "https",
        "apiBaseUrl": provider.base_url, "apiKeyEnv": key_env, "modelIdPrefix": prefix,
    })
}

fn prompt(text: &str) -> Value {
    json!({"content":[{"type":"text","text":text}]})
}

fn assert_stateless(request: &ObservedRequest, model: &str) {
    assert_eq!(
        request.headers["authorization"],
        "Bearer fixture-bearer-token"
    );
    assert_eq!(request.body["model"], model);
    assert_eq!(request.body["store"], false);
    assert_eq!(request.body["stream"], true);
    assert!(request.body.get("previous_response_id").is_none());
    assert!(
        request.body.get("type").is_none(),
        "HTTP is not a WebSocket envelope"
    );
}

#[tokio::test]
async fn gateway_response_ids_follow_provider_disclosure_for_each_call() {
    let provider = Provider::start([
        Reply::Text("known response"),
        Reply::TextWithoutCreated("ID disclosed on completion"),
        Reply::CompletedText("completion-only message"),
    ])
    .await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "OPENROUTER_API_KEY", "openai"),
        )
        .await;
    for number in 1..=3 {
        helper.events.clear();
        helper
            .call(number + 1, "prompt", prompt("inspect response attribution"))
            .await;
        let mut text_events = 0;
        let mut message_key = None;
        let mut reasoning_key = None;
        for event in &helper.events {
            let kind = event["event"].as_str().expect("event kind");
            if !matches!(
                kind,
                "assistant_delta" | "reasoning_delta" | "assistant_message"
            ) {
                continue;
            }
            text_events += 1;
            let data = &event["data"];
            if number == 2 && kind != "assistant_message" {
                assert!(
                    data.get("responseId").is_none(),
                    "early chunks must not inherit the previous response ID"
                );
            } else {
                assert_eq!(data["responseId"], format!("resp_{number}"));
            }
            let key = data["itemKey"].as_str().expect("required presentation key");
            assert!(!key.is_empty());
            if kind == "reasoning_delta" {
                reasoning_key = Some(key.to_owned());
            } else if let Some(previous) = &message_key {
                assert_eq!(
                    key, previous,
                    "streamed and completed item keys stay stable"
                );
            } else {
                message_key = Some(key.to_owned());
            }
        }
        assert_eq!(text_events, if number == 3 { 1 } else { 3 });
        let message_key = message_key.expect("message key");
        assert_ne!(Some(&message_key), reasoning_key.as_ref());
    }
    helper.shutdown(5).await;
}

#[tokio::test]
async fn idless_messages_keep_distinct_keys_across_calls_and_output_items() {
    let provider = Provider::start([
        Reply::IdlessShell,
        Reply::IdlessText { streamed: false },
        Reply::IdlessText { streamed: true },
    ])
    .await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "OPENROUTER_API_KEY", "openai"),
        )
        .await;
    helper
        .call(2, "prompt", prompt("check and then report"))
        .await;
    let messages = helper
        .events
        .iter()
        .filter(|event| event["event"] == "assistant_message")
        .collect::<Vec<_>>();
    let first_message = helper
        .events
        .iter()
        .position(|event| event["event"] == "assistant_message")
        .unwrap();
    let first_tool = helper
        .events
        .iter()
        .position(|event| event["data"]["type"] == "tool.call")
        .unwrap();
    assert!(
        first_message < first_tool,
        "model text must precede its tool call"
    );
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0]["data"]["text"], "Checking");
    assert_eq!(messages[1]["data"]["text"], "Checking complete");
    assert_eq!(messages[2]["data"]["text"], "Checking complete");
    let keys = messages
        .iter()
        .map(|event| event["data"]["itemKey"].as_str().expect("item key"))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        keys.len(),
        3,
        "identical idless text still belongs to distinct items"
    );
    helper.events.clear();
    helper.call(3, "prompt", prompt("stream the report")).await;
    let delta = helper
        .events
        .iter()
        .find(|event| event["event"] == "assistant_delta")
        .expect("live delta");
    let messages = helper
        .events
        .iter()
        .filter(|event| event["event"] == "assistant_message")
        .collect::<Vec<_>>();
    assert_eq!(
        messages.len(),
        2,
        "item-done and response completion must not duplicate one item"
    );
    assert_eq!(delta["data"]["itemKey"], messages[0]["data"]["itemKey"]);
    assert_ne!(
        messages[0]["data"]["itemKey"],
        messages[1]["data"]["itemKey"]
    );
    assert!(delta["data"].get("responseId").is_none());
    assert_eq!(messages[1]["data"]["responseId"], "resp_3");
    helper.shutdown(4).await;
}

#[tokio::test]
async fn gateway_shell_tool_and_restored_rollout_continue_the_same_conversation() {
    let provider = Provider::start([
        Reply::Shell,
        Reply::Text("file created"),
        Reply::Text("history retained"),
    ])
    .await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(
        workspace.path(),
        native_home.path(),
        "OMP_AUTH_GATEWAY_TOKEN",
    );
    let config = initialize(&provider, "OMP_AUTH_GATEWAY_TOKEN", "openai-codex");
    let initialized = helper.call(1, "initialize", config.clone()).await;
    let completed = helper
        .call(2, "prompt", prompt("create a file then report completion"))
        .await;
    assert_eq!(completed["stopReason"], "end_turn");
    assert_eq!(completed["finalMessage"], "file created");
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("provider-tool.txt"))
            .expect("tool created file"),
        "fixture-file-value"
    );
    assert_eq!(
        helper.events.first().expect("accepted event")["event"],
        "accepted"
    );
    assert!(
        helper
            .events
            .iter()
            .any(|event| event["data"]["type"] == "tool.call")
    );
    assert!(
        helper
            .events
            .iter()
            .any(|event| event["data"]["type"] == "tool.result")
    );
    assert!(
        helper
            .events
            .iter()
            .any(|event| event["event"] == "assistant_delta")
    );
    assert!(helper.events.iter().all(|event| event["requestId"] == 2));
    let rollout = Path::new(completed["rolloutPath"].as_str().expect("rollout path")).to_owned();
    let committed = completed["committedBytes"]
        .as_u64()
        .expect("committed bytes") as usize;
    let saved = std::fs::read(&rollout).expect("durable rollout")[..committed].to_vec();
    helper.shutdown(3).await;

    // Only the caller's committed mirror survives this simulated native-state loss.
    std::fs::remove_dir_all(native_home.path()).expect("remove native fixture state");
    std::fs::create_dir_all(rollout.parent().expect("rollout parent"))
        .expect("restore rollout parent");
    std::fs::write(&rollout, saved).expect("restore mirrored rollout");
    let mut helper = Helper::start(
        workspace.path(),
        native_home.path(),
        "OMP_AUTH_GATEWAY_TOKEN",
    );
    let mut resume = config;
    resume["resumeSessionId"] = initialized["nativeSessionId"].clone();
    let resumed = helper.call(1, "initialize", resume).await;
    assert_eq!(resumed["nativeSessionId"], initialized["nativeSessionId"]);
    let next = helper
        .call(2, "prompt", prompt("what did you just create?"))
        .await;
    assert_eq!(next["finalMessage"], "history retained");
    helper.shutdown(3).await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    for request in &requests {
        assert_stateless(request, "openai-codex/gpt-6.1-sol");
    }
    let continuation = requests[1].body.to_string();
    assert!(
        continuation.contains("fixture-tool-output"),
        "native tool output reaches provider"
    );
    let resumed = requests[2].body.to_string();
    for retained in [
        "create a file then report completion",
        "fixture-tool-output",
        "file created",
        "what did you just create?",
    ] {
        assert!(
            resumed.contains(retained),
            "restored provider history lost {retained}"
        );
    }
}

#[tokio::test]
async fn openrouter_uses_selected_key_namespace_and_full_history_on_followup() {
    let provider = Provider::start([
        Reply::Text("first response"),
        Reply::Text("second response"),
    ])
    .await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "OPENROUTER_API_KEY", "openai"),
        )
        .await;
    helper
        .call(2, "prompt", prompt("remember the first question"))
        .await;
    let response = helper
        .call(3, "prompt", prompt("answer the next question"))
        .await;
    assert_eq!(response["usage"]["inputTokens"], 12);
    assert_eq!(response["usage"]["cachedInputTokens"], 4);
    for event in &helper.events {
        if event["data"]["type"] == "model.call.completed" {
            assert!(
                event["data"]["payload"]["usage"]["input_tokens_details"]
                    .get("cache_write_tokens")
                    .is_none()
            );
        }
    }
    assert_eq!(response["usage"]["outputTokens"], 3);
    helper.shutdown(4).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_stateless(request, "openai/gpt-6.1-sol");
    }
    let replay = requests[1].body.to_string();
    assert!(replay.contains("remember the first question"));
    assert!(replay.contains("first response"));
    assert!(replay.contains("answer the next question"));
}

#[tokio::test]
async fn cancellation_interrupts_an_open_stream_after_delivering_live_text() {
    let provider = Provider::start([Reply::Hang, Reply::Text("resumed after cancel")]).await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    let config = initialize(&provider, "OPENROUTER_API_KEY", "openai");
    let initialized = helper.call(1, "initialize", config.clone()).await;
    helper.send(2, "prompt", prompt("start a long turn")).await;
    loop {
        let frame = helper.next().await;
        assert!(
            frame.get("event").is_some(),
            "prompt completed before cancellation: {frame}"
        );
        if frame["event"] == "assistant_delta" {
            break;
        }
    }
    helper.send(3, "cancel", json!({})).await;
    let mut cancelled = false;
    let mut completed = false;
    while !cancelled || !completed {
        let frame = helper.next().await;
        if frame.get("event").is_some() {
            continue;
        }
        assert!(frame.get("error").is_none(), "cancellation failed: {frame}");
        match frame["id"].as_u64() {
            Some(2) => {
                assert_eq!(frame["result"]["stopReason"], "cancelled");
                assert!(
                    frame["result"]["committedBytes"]
                        .as_u64()
                        .expect("flush size")
                        > 0
                );
                completed = true;
            }
            Some(3) => {
                assert_eq!(frame["result"]["cancelled"], true);
                cancelled = true;
            }
            _ => panic!("unexpected cancellation reply: {frame}"),
        }
    }
    helper.shutdown(4).await;
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    let mut resume = config;
    resume["resumeSessionId"] = initialized["nativeSessionId"].clone();
    let resumed = helper.call(1, "initialize", resume).await;
    assert_eq!(resumed["nativeSessionId"], initialized["nativeSessionId"]);
    let completed = helper
        .call(2, "prompt", prompt("continue after cancellation"))
        .await;
    assert_eq!(completed["finalMessage"], "resumed after cancel");
    helper.shutdown(3).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].body.to_string().contains("start a long turn"));
    assert!(
        requests[1]
            .body
            .to_string()
            .contains("continue after cancellation")
    );
}

#[tokio::test]
async fn failed_provider_reply_does_not_expose_the_response_body_in_rpc_error() {
    let provider = Provider::start([Reply::Rejected]).await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "OPENROUTER_API_KEY", "openai"),
        )
        .await;
    helper.send(2, "prompt", prompt("rejected prompt")).await;
    let reply = helper.reply(2).await;
    assert_eq!(reply["error"]["code"], "native_error");
    assert_eq!(reply["error"]["statusCode"], 400);
    assert_eq!(reply["error"]["message"], "provider rejected the request");
    assert!(!reply.to_string().contains("fixture-bearer-token"));
    assert!(
        !serde_json::to_string(&helper.events)
            .unwrap()
            .contains("fixture-bearer-token")
    );
    helper.shutdown(3).await;
}

#[tokio::test]
async fn untouched_session_can_be_restored_without_a_native_conversation_yet() {
    let provider = Provider::start([Reply::Text("first real turn")]).await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    let mut config = initialize(&provider, "OPENROUTER_API_KEY", "openai");
    let initialized = helper.call(1, "initialize", config.clone()).await;
    let rollout = Path::new(initialized["rolloutPath"].as_str().expect("rollout path")).to_owned();
    let committed = initialized["committedBytes"]
        .as_u64()
        .expect("committed bytes") as usize;
    let saved = std::fs::read(&rollout).expect("empty rollout")[..committed].to_vec();
    helper.shutdown(2).await;
    std::fs::remove_dir_all(native_home.path()).expect("remove native fixture state");
    std::fs::create_dir_all(rollout.parent().expect("rollout parent")).expect("restore directory");
    std::fs::write(&rollout, &saved).expect("restore empty rollout");

    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    helper.send(1, "initialize", config.clone()).await;
    let reused = helper.reply(1).await;
    assert_eq!(reused["error"]["code"], "invalid_config");
    assert_eq!(reused["error"]["field"], "sessionId");
    config["sessionId"] = json!(uuid::Uuid::now_v7().to_string());
    let resumed = helper.call(2, "initialize", config).await;
    assert_eq!(
        resumed["replacedNativeSessionId"],
        initialized["nativeSessionId"]
    );
    assert_ne!(resumed["nativeSessionId"], initialized["nativeSessionId"]);
    assert_eq!(resumed["model"], initialized["model"]);
    assert_eq!(resumed["thinking"], initialized["thinking"]);
    assert_eq!(std::fs::read(&rollout).expect("old rollout remains"), saved);
    let response = helper
        .call(3, "prompt", prompt("begin after empty restore"))
        .await;
    assert_eq!(response["finalMessage"], "first real turn");
    helper.shutdown(4).await;
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn corrupt_rollout_is_rejected_without_replacing_the_session() {
    let provider = Provider::start([]).await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    let mut config = initialize(&provider, "OPENROUTER_API_KEY", "openai");
    let initialized = helper.call(1, "initialize", config.clone()).await;
    let rollout = Path::new(initialized["rolloutPath"].as_str().expect("rollout path")).to_owned();
    helper.shutdown(2).await;
    let corrupt = b"this is not a rollout\n";
    std::fs::write(&rollout, corrupt).expect("corrupt fixture state");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    helper.send(1, "initialize", config).await;
    let rejected = helper.reply(1).await;
    assert_eq!(rejected["error"]["code"], "restore_failed");
    assert_eq!(
        std::fs::read(&rollout).expect("corrupt state remains"),
        corrupt
    );
    helper.shutdown(2).await;
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn incomplete_provider_stream_cannot_execute_an_uncommitted_tool_call() {
    let provider = Provider::start((0..5).map(|_| Reply::PartialShell)).await;
    let workspace = TempDir::new().expect("workspace");
    let native_home = TempDir::new().expect("native home");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "OPENROUTER_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "OPENROUTER_API_KEY", "openai"),
        )
        .await;
    helper
        .send(2, "prompt", prompt("must not execute partial response"))
        .await;
    let failed = helper.reply(2).await;
    assert_eq!(failed["error"]["code"], "connection_error");
    assert_eq!(
        failed["error"]["message"],
        "gateway stream ended before completion"
    );
    assert!(!workspace.path().join("must-not-run").exists());
    assert!(
        !helper
            .events
            .iter()
            .any(|event| event["data"]["type"] == "tool.call")
    );
    helper.shutdown(3).await;
    assert_eq!(provider.requests().len(), 5);
}

#[tokio::test]
async fn custom_endpoints_require_api_keys_and_remote_tls() {
    let provider = Provider::start([]).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    std::fs::write(
        native_home.path().join("auth.json"),
        br#"{"personal_access_token":"at-fixture-token"}"#,
    )
    .unwrap();
    for (field, url) in [
        ("apiBaseUrl", "https://example.com/v1"),
        ("websocketUrl", "wss://example.com/responses"),
    ] {
        let mut helper = Helper::start(workspace.path(), native_home.path(), "UNUSED_FIXTURE_KEY");
        let mut config = json!({"sessionId":uuid::Uuid::now_v7().to_string()});
        config[field] = json!(url);
        helper.send(1, "initialize", config).await;
        let rejected = helper.reply(1).await;
        assert_eq!(rejected["error"]["code"], "invalid_config");
        assert_eq!(rejected["error"]["field"], field);
        helper.shutdown(2).await;
    }
    let mut helper = Helper::start_with_env(
        workspace.path(),
        native_home.path(),
        "UNUSED_FIXTURE_KEY",
        &[("OPENAI_BASE_URL", provider.base_url.as_str())],
    );
    helper
        .send(
            1,
            "initialize",
            json!({"sessionId":uuid::Uuid::now_v7().to_string()}),
        )
        .await;
    let rejected = helper.reply(1).await;
    assert_eq!(rejected["error"]["code"], "invalid_config");
    assert_eq!(rejected["error"]["field"], "apiBaseUrl");
    assert_eq!(
        rejected["error"]["message"],
        "ChatGPT authentication refuses custom API endpoints; remove apiBaseUrl and OPENAI_BASE_URL or configure an API key"
    );
    assert!(!rejected.to_string().contains(provider.base_url.as_str()));
    assert!(!rejected.to_string().contains("at-fixture-token"));
    helper.shutdown(2).await;
    for (field, url) in [
        ("apiBaseUrl", "http://example.com/v1"),
        ("websocketUrl", "ws://example.com/responses"),
    ] {
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        let mut config = initialize(&provider, "FIXTURE_API_KEY", "openai");
        config[field] = json!(url);
        helper.send(1, "initialize", config).await;
        let rejected = helper.reply(1).await;
        assert_eq!(rejected["error"]["field"], field);
        helper.shutdown(2).await;
    }
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn configured_native_models_outside_the_picker_can_complete_turns() {
    for model in ["kimi-k3", "mimo-v2.6-pro", "@cf/zai-org/glm-5.3"] {
        let provider = Provider::start([Reply::Text("selected model completed")]).await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        let mut config = initialize(&provider, "FIXTURE_API_KEY", "provider");
        config["model"] = json!(model);
        let state = helper.call(1, "initialize", config).await;
        assert_eq!(state["model"], model);
        let catalog = state["models"].as_array().unwrap();
        assert_eq!(catalog.len(), 4);
        let selected = catalog.last().unwrap();
        assert_eq!(selected["id"], model);
        assert!(selected["contextWindow"].as_u64().unwrap() > 0);
        assert!(
            selected["thinking"]
                .as_array()
                .unwrap()
                .contains(&json!("low"))
        );
        assert_eq!(state["helperVersion"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            state["helperFingerprint"],
            env!("NANOCODEX_HELPER_FINGERPRINT")
        );
        let result = helper.call(2, "prompt", prompt("hello")).await;
        assert_eq!(result["finalMessage"], "selected model completed");
        helper.shutdown(3).await;
        assert_stateless(&provider.requests()[0], &format!("provider/{model}"));
    }
}

#[tokio::test]
async fn dropped_gateway_connection_is_a_connection_error() {
    let provider = Provider::start((0..5).map(|_| Reply::Dropped)).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    helper.send(2, "prompt", prompt("dropped stream")).await;
    let result = helper.reply(2).await;
    assert_eq!(result["error"]["code"], "connection_error");
    assert_eq!(
        result["error"]["message"],
        "gateway connection ended before completion"
    );
    helper.shutdown(3).await;
}

#[tokio::test]
async fn oversized_gateway_event_fails_without_retaining_the_unbounded_stream() {
    let provider = Provider::start([
        Reply::Oversized,
        Reply::Text("usable after rejected event"),
        Reply::Text("usable after restart"),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    let initialized = helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    helper.send(2, "prompt", prompt("huge stream")).await;
    let result = helper.reply(2).await;
    assert_eq!(result["error"]["code"], "transport_error");
    assert_eq!(result["error"]["message"], "invalid gateway event stream");
    helper.call(3, "prompt", prompt("small valid prompt")).await;
    helper.shutdown(4).await;
    let mut config = initialize(&provider, "FIXTURE_API_KEY", "openai");
    config["resumeSessionId"] = initialized["nativeSessionId"].clone();
    config["sessionId"] = initialized["nativeSessionId"].clone();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    helper.call(1, "initialize", config).await;
    helper.call(2, "prompt", prompt("continue safely")).await;
    helper.shutdown(3).await;
    assert_eq!(provider.requests().len(), 3);
}

#[tokio::test]
async fn native_transport_failures_redact_response_bodies_from_events_and_errors() {
    let provider = Provider::start([]).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    let mut config = initialize(&provider, "FIXTURE_API_KEY", "openai");
    config["transport"] = json!("websocket");
    config.as_object_mut().unwrap().remove("modelIdPrefix");
    config["websocketUrl"] = json!(format!(
        "{}/responses",
        provider.base_url.replace("http://", "ws://")
    ));
    helper.call(1, "initialize", config).await;
    helper.send(2, "prompt", prompt("rejected handshake")).await;
    let result = helper.reply(2).await;
    assert_eq!(result["error"]["code"], "native_error");
    assert_eq!(result["error"]["statusCode"], 400);
    assert!(!result.to_string().contains("fixture-bearer-token"));
    assert!(
        !serde_json::to_string(&helper.events)
            .unwrap()
            .contains("fixture-bearer-token")
    );
    helper.shutdown(3).await;
}

#[tokio::test]
async fn gateway_sse_accepts_cr_lf_crlf_and_fragmented_line_endings() {
    for newline in ["\r\n", "\r", "\n"] {
        for fragmented in [false, true] {
            let provider = Provider::start([Reply::LineEndings {
                newline,
                fragmented,
            }])
            .await;
            let workspace = TempDir::new().unwrap();
            let native_home = TempDir::new().unwrap();
            let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
            helper
                .call(
                    1,
                    "initialize",
                    initialize(&provider, "FIXTURE_API_KEY", "openai"),
                )
                .await;
            let result = helper.call(2, "prompt", prompt("line endings")).await;
            assert_eq!(result["finalMessage"], "line endings accepted");
            helper.shutdown(3).await;
        }
    }
}

#[tokio::test]
async fn provider_sse_errors_preserve_safe_codes_without_response_bodies_or_invented_status() {
    for (code, nested, expected_code, message) in [
        (
            "rate_limit_exceeded".to_owned(),
            false,
            Some("rate_limit_exceeded"),
            "provider rate limit reached",
        ),
        (
            "insufficient_quota".to_owned(),
            true,
            Some("insufficient_quota"),
            "provider quota exhausted",
        ),
        (
            "vendor.failure-1".to_owned(),
            false,
            Some("vendor.failure-1"),
            "provider response did not complete",
        ),
        (
            "token: fixture-bearer-token".to_owned(),
            false,
            None,
            "provider response did not complete",
        ),
        (
            "x".repeat(129),
            true,
            None,
            "provider response did not complete",
        ),
    ] {
        let attempts = if code == "insufficient_quota" { 1 } else { 5 };
        let provider = Provider::start((0..attempts).map(|_| Reply::ErrorEvent {
            code: code.clone(),
            nested,
        }))
        .await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        helper
            .call(
                1,
                "initialize",
                initialize(&provider, "FIXTURE_API_KEY", "openai"),
            )
            .await;
        helper
            .send(2, "prompt", prompt("classify provider error"))
            .await;
        let reply = helper.reply(2).await;
        assert_eq!(reply["error"]["code"], "native_error");
        assert_eq!(reply["error"]["providerCode"].as_str(), expected_code);
        assert_eq!(reply["error"]["message"], message);
        assert!(reply["error"].get("statusCode").is_none());
        assert!(!reply.to_string().contains("fixture-bearer-token"));
        assert!(
            !serde_json::to_string(&helper.events)
                .unwrap()
                .contains("fixture-bearer-token")
        );
        helper.shutdown(3).await;
    }
}

#[tokio::test]
async fn restored_workspace_mismatch_is_distinct_from_startup_failure() {
    let provider = Provider::start([Reply::Text("saved")]).await;
    let workspace = TempDir::new().unwrap();
    let another_workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let config = initialize(&provider, "FIXTURE_API_KEY", "openai");
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    let state = helper.call(1, "initialize", config.clone()).await;
    helper
        .call(2, "prompt", prompt("save a conversation"))
        .await;
    helper.shutdown(3).await;
    let mut helper = Helper::start(
        another_workspace.path(),
        native_home.path(),
        "FIXTURE_API_KEY",
    );
    let mut restore = config;
    restore["resumeSessionId"] = state["nativeSessionId"].clone();
    helper.send(1, "initialize", restore).await;
    let refused = helper.reply(1).await;
    assert_eq!(refused["error"]["code"], "restore_failed");
    assert_eq!(
        refused["error"]["message"],
        "restored native workspace does not match requested workspace"
    );
    helper.shutdown(2).await;
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn gateway_retries_transient_failures_without_replaying_tools_or_history() {
    let provider = Provider::start([
        Reply::HttpError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            retry_after: "0",
            error: json!({"code":"server_error", "message":"provider body includes fixture-bearer-token"}),
        },
        Reply::ErrorEvent {
            code: "rate_limit_exceeded".to_owned(),
            nested: false,
        },
        Reply::Dropped,
        Reply::RawSse(Bytes::new()),
        Reply::Shell,
        Reply::Text("recovered once"),
        Reply::Text("continued"),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    let result = helper
        .call(2, "prompt", prompt("recover then write the file"))
        .await;
    assert_eq!(result["finalMessage"], "recovered once");
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("provider-tool.txt")).unwrap(),
        "fixture-file-value"
    );
    assert_eq!(
        helper
            .events
            .iter()
            .filter(|event| event["data"]["type"] == "tool.call")
            .count(),
        1
    );
    assert_eq!(
        helper
            .events
            .iter()
            .filter(|event| event["event"] == "assistant_delta")
            .count(),
        1
    );
    helper
        .call(3, "prompt", prompt("continue after recovery"))
        .await;
    helper.shutdown(4).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 7);
    for request in &requests[1..5] {
        assert_eq!(
            request.body, requests[0].body,
            "retry changed the request history"
        );
    }
    assert!(requests[5].body.to_string().contains("fixture-tool-output"));
    let continued = requests[6].body.to_string();
    assert_eq!(continued.matches("recover then write the file").count(), 1);
    assert_eq!(continued.matches("recovered once").count(), 1);
}

#[tokio::test]
async fn gateway_discards_uncommitted_tools_when_retrying_a_truncated_stream() {
    let provider =
        Provider::start([Reply::PartialShell, Reply::Text("recovered without tool")]).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    let result = helper
        .call(2, "prompt", prompt("retry incomplete tool response"))
        .await;
    assert_eq!(result["finalMessage"], "recovered without tool");
    assert!(!workspace.path().join("must-not-run").exists());
    assert!(
        helper
            .events
            .iter()
            .all(|event| event["data"]["type"] != "tool.call")
    );
    helper.shutdown(3).await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body, requests[1].body);
    assert!(!requests[1].body.to_string().contains("call_partial"));
}

#[tokio::test]
async fn gateway_does_not_retry_after_publishing_assistant_or_reasoning_output() {
    for event in [
        json!({"type":"response.output_text.delta", "output_index":0, "delta":"partial text"}),
        json!({"type":"response.reasoning_summary_text.delta", "output_index":0, "delta":"partial reasoning"}),
        json!({"type":"response.output_item.done", "output_index":0, "item":{
            "type":"message", "role":"assistant", "content":[{"type":"output_text", "text":"partial message"}]
        }}),
    ] {
        let provider =
            Provider::start([Reply::RawSse(sse(event)), Reply::Text("must not replay")]).await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        helper
            .call(
                1,
                "initialize",
                initialize(&provider, "FIXTURE_API_KEY", "openai"),
            )
            .await;
        helper
            .send(2, "prompt", prompt("stop after visible output"))
            .await;
        let result = helper.reply(2).await;
        assert_eq!(result["error"]["code"], "connection_error");
        assert_eq!(
            helper
                .events
                .iter()
                .filter(|event| matches!(
                    event["event"].as_str(),
                    Some("assistant_delta" | "reasoning_delta" | "assistant_message")
                ))
                .count(),
            1
        );
        helper.shutdown(3).await;
        assert_eq!(provider.requests().len(), 1);
    }
}

#[tokio::test]
async fn gateway_rejects_permanent_errors_and_invalid_streams_without_retrying() {
    for reply in [
        Reply::Rejected,
        Reply::HttpError {
            status: StatusCode::UNAUTHORIZED,
            retry_after: "0",
            error: json!({"code":"invalid_api_key", "message":"provider body includes fixture-bearer-token"}),
        },
        Reply::HttpError {
            status: StatusCode::FORBIDDEN,
            retry_after: "0",
            error: json!({"code":"unauthorized"}),
        },
        Reply::HttpError {
            status: StatusCode::TOO_MANY_REQUESTS,
            retry_after: "0",
            error: json!({"type":"insufficient_quota", "code":null}),
        },
        Reply::HttpError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            retry_after: "0",
            error: json!({"type":"authentication_error"}),
        },
        Reply::HttpError {
            status: StatusCode::TOO_MANY_REQUESTS,
            retry_after: "0",
            error: json!({"code":"insufficient_quota", "message":"provider body includes fixture-bearer-token"}),
        },
        Reply::HttpError {
            status: StatusCode::TOO_MANY_REQUESTS,
            retry_after: "120",
            error: json!({"code":"rate_limit_exceeded", "message":"provider body includes fixture-bearer-token"}),
        },
        Reply::RawSse(Bytes::from_static(b"data: {invalid json}\n\n")),
        Reply::RawSse(sse(
            json!({"type":"response.incomplete", "response":{"incomplete_details":{"reason":"max_output_tokens"}}}),
        )),
    ] {
        let provider = Provider::start([reply, Reply::Text("must not retry")]).await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        helper
            .call(
                1,
                "initialize",
                initialize(&provider, "FIXTURE_API_KEY", "openai"),
            )
            .await;
        helper.send(2, "prompt", prompt("permanent error")).await;
        let result = timeout(Duration::from_secs(2), helper.reply(2))
            .await
            .expect("permanent failure must be immediate");
        assert!(result.get("error").is_some());
        assert!(!result.to_string().contains("fixture-bearer-token"));
        helper.shutdown(3).await;
        assert_eq!(provider.requests().len(), 1);
    }
}

#[tokio::test]
async fn cancellation_interrupts_retry_after_and_leaves_the_session_usable() {
    let provider = Provider::start([
        Reply::HttpError {
            status: StatusCode::TOO_MANY_REQUESTS,
            retry_after: "10",
            error: json!({"code":"rate_limit_exceeded", "message":"provider body includes fixture-bearer-token"}),
        },
        Reply::Text("continued after cancelled backoff"),
    ])
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    helper.send(2, "prompt", prompt("retry rate limit")).await;
    timeout(DEADLINE, async {
        while provider.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    helper.send(3, "cancel", json!({})).await;
    timeout(Duration::from_secs(2), async {
        let mut replies = 0;
        while replies < 2 {
            let frame = helper.next().await;
            if frame.get("event").is_some() {
                continue;
            }
            assert!(frame.get("error").is_none());
            match frame["id"].as_u64() {
                Some(2) => assert_eq!(frame["result"]["stopReason"], "cancelled"),
                Some(3) => assert_eq!(frame["result"]["cancelled"], true),
                _ => panic!("unexpected cancellation reply: {frame}"),
            }
            replies += 1;
        }
    })
    .await
    .expect("retry sleep must be cancellable");
    let result = helper.call(4, "prompt", prompt("continue now")).await;
    assert_eq!(result["finalMessage"], "continued after cancelled backoff");
    helper.shutdown(5).await;
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn gateway_retries_http_timeouts_conflicts_rate_limits_and_server_errors_with_a_limit() {
    for status in [
        StatusCode::REQUEST_TIMEOUT,
        StatusCode::CONFLICT,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let provider = Provider::start((0..5).map(|_| Reply::HttpError {
            status,
            retry_after: "0",
            error: json!({"code":"server_error", "message":"provider body includes fixture-bearer-token"}),
        })).await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        helper
            .call(
                1,
                "initialize",
                initialize(&provider, "FIXTURE_API_KEY", "openai"),
            )
            .await;
        helper.send(2, "prompt", prompt("exhaust retries")).await;
        let result = helper.reply(2).await;
        assert_eq!(result["error"]["statusCode"], status.as_u16());
        assert_eq!(result["error"]["providerCode"], "server_error");
        assert!(!result.to_string().contains("fixture-bearer-token"));
        helper.shutdown(3).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 5);
        assert!(
            requests
                .iter()
                .all(|request| request.body == requests[0].body)
        );
    }
}

#[tokio::test]
async fn gateway_retries_transient_error_types_even_with_specific_provider_codes() {
    for http in [false, true] {
        let response = json!({"id":"failed-response", "status":"failed", "error":{
            "type":"server_error", "code":"internal_error", "message":"fixture-bearer-token", "retry_after":0
        }});
        let failure = if http {
            Reply::HttpBody(response)
        } else {
            Reply::RawSse(sse(json!({"type":"response.failed", "response":response})))
        };
        let provider =
            Provider::start([failure, Reply::Text("recovered from provider error")]).await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        helper
            .call(
                1,
                "initialize",
                initialize(&provider, "FIXTURE_API_KEY", "openai"),
            )
            .await;
        let result = helper
            .call(2, "prompt", prompt("recover transient provider category"))
            .await;
        assert_eq!(result["finalMessage"], "recovered from provider error");
        assert!(
            !serde_json::to_string(&helper.events)
                .unwrap()
                .contains("fixture-bearer-token")
        );
        helper.shutdown(3).await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].body, requests[1].body);
    }
}

#[tokio::test]
async fn gateway_permanent_error_categories_override_transient_codes_and_statuses() {
    for event_type in ["http", "response.failed", "response.error", "error"] {
        for response in [
            json!({"error_type":"authentication", "error":{"code":"server_error"}}),
            json!({"error_type":"payment_required", "error":{"code":"server_error"}}),
            json!({"error_type":"invalid_request", "error":{"code":"server_error"}}),
            json!({"error":{"type":"invalid_request", "code":"server_error"}}),
            json!({"error":{"type":"server_error", "code":"insufficient_quota"}}),
        ] {
            let failure = match event_type {
                "http" => Reply::HttpBody(response),
                "response.failed" => {
                    Reply::RawSse(sse(json!({"type":event_type, "response":response})))
                }
                _ => {
                    let mut event = response;
                    event["type"] = json!(event_type);
                    Reply::RawSse(sse(event))
                }
            };
            let provider = Provider::start([failure, Reply::Text("must not retry")]).await;
            let workspace = TempDir::new().unwrap();
            let native_home = TempDir::new().unwrap();
            let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
            helper
                .call(
                    1,
                    "initialize",
                    initialize(&provider, "FIXTURE_API_KEY", "openai"),
                )
                .await;
            helper
                .send(2, "prompt", prompt("fail permanent provider category"))
                .await;
            let result = helper.reply(2).await;
            assert_eq!(result["error"]["code"], "native_error");
            helper.shutdown(3).await;
            assert_eq!(provider.requests().len(), 1);
        }
    }
}

#[tokio::test]
async fn non_sse_success_is_never_retried() {
    for (mime, body, code) in [
        (
            "application/json",
            r#"{"id":"billed-generation","status":"completed","output":[]}"#,
            "transport_error",
        ),
        ("text/html", "<html>proxy page</html>", "transport_error"),
        (
            "application/json",
            r#"{"error":{"code":"invalid_api_key","message":"fixture-bearer-token"}}"#,
            "native_error",
        ),
    ] {
        let provider = Provider::start([Reply::NonSse(mime, body)]).await;
        let workspace = TempDir::new().unwrap();
        let native_home = TempDir::new().unwrap();
        let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
        helper
            .call(
                1,
                "initialize",
                initialize(&provider, "FIXTURE_API_KEY", "openai"),
            )
            .await;
        helper.send(2, "prompt", prompt("one request only")).await;
        let failed = helper.reply(2).await;
        assert_eq!(failed["error"]["code"], code);
        assert!(!failed.to_string().contains("fixture-bearer-token"));
        helper.shutdown(3).await;
        assert_eq!(provider.requests().len(), 1);
    }
}

#[tokio::test]
async fn oversized_prompt_leaves_native_history_usable() {
    let provider = Provider::start([Reply::Text("accepted after oversize")]).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    let initialized = helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    helper
        .send(2, "prompt", prompt(&"x".repeat(12 * 1024 * 1024)))
        .await;
    assert_eq!(helper.reply(2).await["error"]["code"], "invalid_request");
    assert!(provider.requests().is_empty());
    helper.call(3, "prompt", prompt("accepted input")).await;
    helper.shutdown(4).await;
    let rollout = std::fs::read_to_string(initialized["rolloutPath"].as_str().unwrap()).unwrap();
    assert!(rollout.len() < 100_000);
    assert!(rollout.contains("accepted after oversize"));
}

fn created_then(events: impl IntoIterator<Item = Value>) -> Reply {
    let mut body = sse(
        json!({"type":"response.created", "response":{"id":"resp_failed", "status":"in_progress"}}),
    )
    .to_vec();
    for event in events {
        body.extend_from_slice(&sse(event));
    }
    Reply::RawSse(Bytes::from(body))
}

fn response_failed(error: Value) -> Value {
    json!({"type":"response.failed", "response":{
        "id":"resp_failed", "status":"failed", "error":error, "incomplete_details":null
    }})
}

async fn prompt_against(replies: Vec<Reply>) -> (Value, Vec<Value>, Vec<ObservedRequest>) {
    let provider = Provider::start(replies).await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let mut helper = Helper::start(workspace.path(), native_home.path(), "FIXTURE_API_KEY");
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    helper
        .send(2, "prompt", prompt("survive a failed provider event"))
        .await;
    let reply = helper.reply(2).await;
    let events = std::mem::take(&mut helper.events);
    helper.shutdown(3).await;
    (reply, events, provider.requests())
}

#[tokio::test]
async fn gateway_retries_codeless_failure_events_before_output() {
    for (name, failure) in [
        (
            "response.failed without code",
            response_failed(json!({"message":"provider body includes fixture-bearer-token"})),
        ),
        (
            "response.failed with null code",
            response_failed(
                json!({"code":null, "message":"provider body includes fixture-bearer-token"}),
            ),
        ),
        (
            "response.failed with null error",
            response_failed(Value::Null),
        ),
        (
            "error event without code",
            json!({"type":"error", "message":"provider body includes fixture-bearer-token"}),
        ),
        (
            "response.incomplete without reason",
            json!({"type":"response.incomplete", "response":{"id":"resp_failed", "status":"incomplete", "incomplete_details":null}}),
        ),
    ] {
        let (reply, events, requests) = prompt_against(vec![
            created_then([failure]),
            Reply::Text("recovered after codeless failure"),
        ])
        .await;
        assert!(
            reply.get("error").is_none(),
            "{name} was not retried: {reply}"
        );
        assert_eq!(
            reply["result"]["finalMessage"], "recovered after codeless failure",
            "{name}"
        );
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains("fixture-bearer-token")
        );
        assert_eq!(requests.len(), 2, "{name}");
        assert_eq!(requests[0].body, requests[1].body, "{name} changed history");
    }
}

#[tokio::test]
async fn gateway_retries_unrecognised_failure_codes_before_output() {
    for (name, failure) in [
        (
            "unknown response.error.code",
            response_failed(json!({"code":"server_error_xyz", "message":"fixture-bearer-token"})),
        ),
        (
            "numeric response.error.code",
            response_failed(json!({"code":502, "message":"fixture-bearer-token"})),
        ),
        (
            "numeric rate-limit status string",
            json!({"type":"error", "error":{"code":"429", "message":"fixture-bearer-token"}}),
        ),
        (
            "unknown response.error.type",
            response_failed(json!({"type":"api_error", "message":"fixture-bearer-token"})),
        ),
        (
            "unknown top-level error code",
            json!({"type":"error", "code":"ERR_SOMETHING", "message":"fixture-bearer-token", "param":null}),
        ),
        (
            "unknown nested error type",
            json!({"type":"error", "error":{"type":"upstream_error", "message":"fixture-bearer-token"}}),
        ),
        (
            "unknown incomplete reason",
            json!({"type":"response.incomplete", "response":{"id":"resp_failed", "status":"incomplete", "incomplete_details":{"reason":"upstream_timeout"}}}),
        ),
    ] {
        let (reply, _, requests) = prompt_against(vec![
            created_then([failure]),
            Reply::Text("recovered after unknown failure"),
        ])
        .await;
        assert!(
            reply.get("error").is_none(),
            "{name} was not retried: {reply}"
        );
        assert_eq!(requests.len(), 2, "{name}");
    }
}

#[tokio::test]
async fn gateway_keeps_authorization_quota_and_request_failure_events_terminal() {
    for (failure, provider_code) in [
        (
            response_failed(json!({"code":"invalid_api_key", "message":"fixture-bearer-token"})),
            "invalid_api_key",
        ),
        (
            response_failed(json!({"code":"insufficient_quota", "message":"fixture-bearer-token"})),
            "insufficient_quota",
        ),
        (
            response_failed(json!({"type":"invalid_request_error", "code":null})),
            "invalid_request_error",
        ),
        (
            response_failed(json!({"code":401, "message":"fixture-bearer-token"})),
            "401",
        ),
        (
            json!({"type":"error", "error":{"code":"403", "message":"fixture-bearer-token"}}),
            "403",
        ),
        (
            json!({"type":"error", "error":{"type":"authentication_error", "message":"fixture-bearer-token"}}),
            "authentication_error",
        ),
        (
            json!({"type":"response.incomplete", "response":{"id":"resp_failed", "status":"incomplete", "incomplete_details":{"reason":"max_output_tokens"}}}),
            "max_output_tokens",
        ),
        (
            json!({"type":"response.incomplete", "response":{"id":"resp_failed", "status":"incomplete", "incomplete_details":{"reason":"content_filter"}}}),
            "content_filter",
        ),
    ] {
        let (reply, _, requests) = prompt_against(vec![
            created_then([failure.clone()]),
            Reply::Text("must not retry"),
        ])
        .await;
        assert_eq!(reply["error"]["code"], "native_error", "{failure}");
        assert_eq!(
            reply["error"]["providerCode"].as_str(),
            Some(provider_code),
            "{failure}"
        );
        assert!(!reply.to_string().contains("fixture-bearer-token"));
        assert_eq!(requests.len(), 1, "terminal failure retried: {failure}");
    }
}

#[tokio::test]
async fn gateway_never_retries_failure_events_after_visible_output() {
    for failure in [
        response_failed(json!({"message":"fixture-bearer-token"})),
        response_failed(json!({"code":"server_error_xyz"})),
        response_failed(json!({"code":"server_error"})),
        json!({"type":"error", "code":"rate_limit_exceeded", "message":"fixture-bearer-token"}),
    ] {
        let (reply, events, requests) = prompt_against(vec![
            created_then([
                json!({"type":"response.output_text.delta", "output_index":0, "delta":"partial text"}),
                failure.clone(),
            ]),
            Reply::Text("must not replay"),
        ])
        .await;
        assert_eq!(reply["error"]["code"], "native_error", "{failure}");
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event"] == "assistant_delta")
                .count(),
            1
        );
        assert_eq!(requests.len(), 1, "retried after output: {failure}");
    }
}

#[tokio::test]
async fn failure_event_provider_code_carries_unrecognised_codes_and_types() {
    for (failure, provider_code) in [
        (
            response_failed(json!({"code":"server_error_xyz", "message":"fixture-bearer-token"})),
            "server_error_xyz",
        ),
        (
            response_failed(json!({"type":"api_error", "message":"fixture-bearer-token"})),
            "api_error",
        ),
        (
            response_failed(json!({"code":502, "message":"fixture-bearer-token"})),
            "502",
        ),
        (
            json!({"type":"error", "error":{"type":"overloaded_error", "message":"fixture-bearer-token"}}),
            "overloaded_error",
        ),
        (
            json!({"type":"response.incomplete", "response":{"id":"resp_failed", "status":"incomplete", "incomplete_details":{"reason":"max_output_tokens"}}}),
            "max_output_tokens",
        ),
    ] {
        let (reply, _, _) =
            prompt_against((0..5).map(|_| created_then([failure.clone()])).collect()).await;
        assert_eq!(reply["error"]["code"], "native_error", "{failure}");
        assert_eq!(
            reply["error"]["providerCode"].as_str(),
            Some(provider_code),
            "missing providerCode for {failure}: {reply}"
        );
        assert!(!reply.to_string().contains("fixture-bearer-token"));
    }
}

#[tokio::test]
async fn gateway_retries_streams_that_end_after_created_without_a_terminal_event() {
    let (reply, _, requests) = prompt_against(vec![
        created_then([]),
        Reply::Text("recovered after truncated stream"),
    ])
    .await;
    assert_eq!(
        reply["result"]["finalMessage"],
        "recovered after truncated stream"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn exhausted_failure_event_retries_report_the_last_provider_code() {
    let (reply, _, requests) = prompt_against(
        [
            "upstream_a",
            "upstream_b",
            "upstream_c",
            "upstream_d",
            "upstream_last",
        ]
        .into_iter()
        .map(|code| {
            created_then([response_failed(
                json!({"code":code, "message":"fixture-bearer-token"}),
            )])
        })
        .chain([Reply::Text("beyond the attempt limit")])
        .collect(),
    )
    .await;
    assert_eq!(requests.len(), 5);
    assert_eq!(reply["error"]["code"], "native_error");
    assert_eq!(reply["error"]["providerCode"], "upstream_last");
    assert_eq!(
        reply["error"]["message"],
        "provider response did not complete"
    );
    assert!(reply["error"].get("statusCode").is_none());
    assert!(!reply.to_string().contains("fixture-bearer-token"));
}

#[tokio::test]
async fn canonical_error_type_decides_retry_and_is_reported() {
    let response = json!({"error_type":"payment_required", "error":{"code":"server_error", "message":"fixture-bearer-token"}});
    let mut event = response.clone();
    event["type"] = json!("error");
    for failure in [
        Reply::HttpBody(response.clone()),
        created_then([json!({"type":"response.failed", "response":response.clone()})]),
        created_then([event]),
    ] {
        let (reply, _, requests) =
            prompt_against(vec![failure, Reply::Text("must not retry")]).await;
        assert_eq!(
            requests.len(),
            1,
            "canonical terminal type retried: {reply}"
        );
        assert_eq!(reply["error"]["code"], "native_error");
        assert_eq!(reply["error"]["providerCode"], "payment_required");
        assert!(!reply.to_string().contains("fixture-bearer-token"));
    }
}

#[tokio::test]
async fn exhausted_http_retries_report_the_status_on_each_stderr_line() {
    let provider = Provider::start((0..5).map(|_| Reply::HttpError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        retry_after: "0",
        error: json!({"code":"server_error", "message":"provider body includes fixture-bearer-token"}),
    }))
    .await;
    let workspace = TempDir::new().unwrap();
    let native_home = TempDir::new().unwrap();
    let log = workspace.path().join("helper-stderr.log");
    let mut helper = Helper::spawn(
        workspace.path(),
        native_home.path(),
        "FIXTURE_API_KEY",
        &[],
        Stdio::from(std::fs::File::create(&log).unwrap()),
    );
    helper
        .call(
            1,
            "initialize",
            initialize(&provider, "FIXTURE_API_KEY", "openai"),
        )
        .await;
    helper
        .send(2, "prompt", prompt("exhaust service retries"))
        .await;
    let reply = helper.reply(2).await;
    helper.shutdown(3).await;
    assert_eq!(reply["error"]["statusCode"], 503);
    assert_eq!(provider.requests().len(), 5);
    let stderr = std::fs::read_to_string(&log).unwrap();
    let lines = stderr
        .lines()
        .filter(|line| line.starts_with("gateway attempt"))
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 5, "{stderr}");
    for (index, line) in lines.iter().enumerate() {
        assert!(
            line.starts_with(&format!(
                "gateway attempt {}/5 failed status=503 providerCode=server_error; ",
                index + 1
            )),
            "{line}"
        );
    }
    assert!(lines[0].contains("; retrying in "));
    assert!(lines[4].ends_with("; not retrying: attempt limit reached"));
    assert!(!stderr.contains("fixture-bearer-token"));
}
