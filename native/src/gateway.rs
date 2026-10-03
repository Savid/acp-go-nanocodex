use crate::{
    retry::{Failure, Retry},
    session::SessionError,
};
use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::StreamExt;
use nanocodex::oai::{
    ResponseError,
    auth::OpenAiAuth,
    responses::{CompletedResponse, ResponseItem},
    tower::{
        CodeCall, CodeCallKind, CompactionOutput, GenerationOutput, ResponsePipelineStats,
        ResponsesAttempt, ResponsesAttemptKind, ResponsesOutput, ResponsesServiceResponse,
    },
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tower::{Layer, Service};

pub struct GatewayConfig {
    pub auth: OpenAiAuth,
    pub base_url: String,
    pub model_id_prefix: Option<String>,
    pub session_id: String,
}

pub struct GatewayEvent {
    pub event: &'static str,
    pub data: Value,
}

#[derive(Clone)]
pub struct GatewayLayer {
    route: Option<Arc<GatewayRoute>>,
}

struct GatewayRoute {
    config: GatewayConfig,
    client: reqwest::Client,
    events: mpsc::Sender<GatewayEvent>,
}

impl GatewayLayer {
    pub fn new(
        config: Option<GatewayConfig>,
    ) -> Result<(Self, mpsc::Receiver<GatewayEvent>), reqwest::Error> {
        nanocodex::oai::transport::install_default_rustls_crypto_provider();
        let (events, receiver) = mpsc::channel(128);
        let route = config
            .map(|config| {
                let client = reqwest::Client::builder()
                    .user_agent(concat!("acp-go-nanocodex/", env!("CARGO_PKG_VERSION")))
                    .connect_timeout(Duration::from_secs(30))
                    .read_timeout(Duration::from_secs(300))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?;
                Ok::<_, reqwest::Error>(Arc::new(GatewayRoute {
                    config,
                    client,
                    events,
                }))
            })
            .transpose()?;
        Ok((Self { route }, receiver))
    }
}

pub struct GatewayService<S> {
    inner: S,
    route: Option<Arc<GatewayRoute>>,
    history: Arc<tokio::sync::Mutex<GatewayHistory>>,
}

#[derive(Default)]
struct GatewayHistory {
    input: Vec<Value>,
    tools: Vec<Value>,
    response_id: Option<String>,
}

impl<S> Layer<S> for GatewayLayer {
    type Service = GatewayService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GatewayService {
            inner,
            route: self.route.clone(),
            history: Arc::default(),
        }
    }
}

impl<S> Service<ResponsesAttempt> for GatewayService<S>
where
    S: Service<ResponsesAttempt, Response = ResponsesServiceResponse>,
    S::Error: Into<ResponseError>,
    S::Future: Send + 'static,
{
    type Response = ResponsesServiceResponse;
    type Error = ResponseError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.route.is_some() {
            Poll::Ready(Ok(()))
        } else {
            self.inner.poll_ready(cx).map_err(Into::into)
        }
    }

    fn call(&mut self, request: ResponsesAttempt) -> Self::Future {
        if let Some(route) = self.route.clone() {
            let history = self.history.clone();
            Box::pin(async move { route.execute(request, history).await })
        } else {
            let future = self.inner.call(request);
            Box::pin(async move { future.await.map_err(Into::into) })
        }
    }
}

fn failure(message: &'static str) -> ResponseError {
    ResponseError::service(SessionError::new("transport_error", message))
}

fn output_index(event: &Value) -> Result<usize, ResponseError> {
    event["output_index"]
        .as_u64()
        .and_then(|index| usize::try_from(index).ok())
        .ok_or_else(|| failure("gateway event requires an output index"))
}

fn item_key(model_call_index: u32, output_index: usize) -> String {
    format!("{model_call_index}:{output_index}")
}

fn public_input(request: &ResponsesAttempt) -> Result<(Vec<Value>, Vec<Value>), ResponseError> {
    let mut input = Vec::new();
    let mut tools = Vec::new();
    for item in request.input_items() {
        let mut item =
            serde_json::to_value(item).map_err(|_| failure("cannot encode provider input"))?;
        match item["type"].as_str() {
            Some("additional_tools") => {
                let definitions = item["tools"]
                    .as_array()
                    .ok_or_else(|| failure("invalid native tool inventory"))?;
                tools.extend(
                    definitions
                        .iter()
                        .filter(|tool| tool["type"] == "function" && tool["name"] != "wait")
                        .cloned(),
                );
            }
            Some("configuration_update") => {}
            Some(
                "message"
                | "reasoning"
                | "function_call"
                | "function_call_output"
                | "compaction"
                | "compaction_trigger",
            ) => {
                if let Some(item) = item.as_object_mut() {
                    for private in [
                        "internal_chat_message_metadata_passthrough",
                        "created_by",
                        "caller",
                    ] {
                        item.remove(private);
                    }
                }
                input.push(item);
            }
            _ => {
                return Err(failure(
                    "native history contains an unsupported gateway item",
                ));
            }
        }
    }
    Ok((input, tools))
}

impl GatewayRoute {
    async fn emit(
        &self,
        event: &'static str,
        text: &str,
        item_key: &str,
        response_id: Option<&str>,
    ) -> Result<(), ResponseError> {
        let mut data = json!({"text":text,"itemKey":item_key});
        if let Some(id) = response_id {
            data["responseId"] = json!(id);
        }
        self.events
            .send(GatewayEvent { event, data })
            .await
            .map_err(|_| failure("provider event consumer closed"))
    }

    async fn execute(
        &self,
        request: ResponsesAttempt,
        history: Arc<tokio::sync::Mutex<GatewayHistory>>,
    ) -> Result<ResponsesServiceResponse, ResponseError> {
        let kind = request.kind();
        if matches!(kind, ResponsesAttemptKind::Warmup) {
            return Err(failure("gateway does not support native warmup"));
        }
        let model_call_index = request
            .model_call_index()
            .ok_or_else(|| failure("gateway request requires a model call index"))?;
        let started = Instant::now();
        let (mut input, mut tools) = public_input(&request)?;
        let mut history = history.lock().await;
        if !request.is_full_replay() {
            if request.previous_response_id() != history.response_id.as_deref() {
                return Err(failure(
                    "gateway continuation does not match committed history",
                ));
            }
            let mut full = history.input.clone();
            full.append(&mut input);
            input = full;
            tools = history.tools.clone();
        }
        let model = match self.config.model_id_prefix.as_deref() {
            Some(prefix) => format!("{prefix}/{}", request.model().as_str()),
            None => request.model().as_str().to_owned(),
        };
        let body = json!({
            "model":model, "input":input, "tools":tools, "stream":true, "store":false,
            "reasoning":{"effort":request.thinking().as_str()},
            "include":["reasoning.encrypted_content"], "tool_choice":"auto", "parallel_tool_calls":false,
        });
        let mut retry = Retry::new(&self.config.session_id, Some(model_call_index));
        loop {
            let mut emitted_output = false;
            match self
                .stream_attempt(&body, kind, model_call_index, started, &mut emitted_output)
                .await
            {
                Ok((output, committed_output)) => {
                    if let ResponsesOutput::Generation(generation) = &output {
                        history.input = input;
                        history.input.extend(committed_output);
                        history.tools = tools;
                        history.response_id = Some(generation.id.clone());
                    } else {
                        // Native compaction installs retained context and forces a full replay.
                        *history = GatewayHistory::default();
                    }
                    return Ok(ResponsesServiceResponse::new(output));
                }
                Err(error) => {
                    if emitted_output || !retry.wait(&error).await {
                        return Err(error.error);
                    }
                }
            }
        }
    }

    async fn response(&self, body: &Value) -> Result<reqwest::Response, Failure> {
        let auth = self
            .config
            .auth
            .snapshot()
            .await
            .map_err(|_| failure("gateway authentication unavailable"))?;
        let mut request = self
            .client
            .post(format!(
                "{}/responses",
                self.config.base_url.trim_end_matches('/')
            ))
            .bearer_auth(auth.bearer())
            .header("accept", "text/event-stream")
            .json(body);
        if sends_session_header(&self.config.base_url) {
            request = request.header("x-opencode-session", &self.config.session_id);
        }
        let response = request.send().await.map_err(Failure::request)?;
        if !response.status().is_success() {
            return Err(Failure::http(response).await);
        }
        Ok(response)
    }

    async fn stream_attempt(
        &self,
        body: &Value,
        kind: ResponsesAttemptKind,
        model_call_index: u32,
        started: Instant,
        emitted_output: &mut bool,
    ) -> Result<(ResponsesOutput, Vec<Value>), Failure> {
        let compacting = matches!(kind, ResponsesAttemptKind::Compaction);
        let response = self.response(body).await?;
        let chunks = futures_util::stream::try_unfold(
            (Box::pin(response.bytes_stream()), StreamBound::default()),
            |(mut input, mut bound)| async move {
                loop {
                    let Some(chunk) = input.next().await else {
                        return if bound.buffered.is_empty() {
                            Ok(None)
                        } else {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "gateway stream ended inside an event",
                            ))
                        };
                    };
                    let chunk = chunk.map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "gateway stream read failed",
                        )
                    })?;
                    if let Some(event) = bound.accept(&chunk)? {
                        return Ok(Some((event, (input, bound))));
                    }
                }
            },
        );
        let mut stream = Box::pin(chunks.eventsource());
        let mut done_items = BTreeMap::new();
        let mut streamed_compaction = None;
        let mut emitted_messages = HashSet::new();
        let mut response_id = None;
        let mut first_event = None;
        let mut first_output = None;
        let mut pipeline = ResponsePipelineStats::default();
        while let Some(frame) = stream.next().await {
            let frame = frame.map_err(|error| match error {
                EventStreamError::Transport(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    Failure::connection("gateway connection ended before completion")
                }
                _ => failure("invalid gateway event stream").into(),
            })?;
            if frame.data == "[DONE]" {
                break;
            }
            first_event.get_or_insert_with(|| elapsed(started));
            pipeline.event_count += 1;
            pipeline.event_bytes += frame.data.len() as u64;
            let event: Value = serde_json::from_str(&frame.data)
                .map_err(|_| failure("invalid gateway event JSON"))?;
            if let Some(id) = event["response"]["id"].as_str().filter(|id| !id.is_empty()) {
                response_id = Some(id.to_owned());
            }
            match event["type"].as_str() {
                Some("response.output_text.delta") if !compacting => {
                    first_output.get_or_insert_with(|| elapsed(started));
                    let text = event["delta"]
                        .as_str()
                        .ok_or_else(|| failure("invalid gateway text delta"))?;
                    self.emit(
                        "assistant_delta",
                        text,
                        &item_key(model_call_index, output_index(&event)?),
                        response_id.as_deref(),
                    )
                    .await?;
                    *emitted_output = true;
                }
                Some(
                    "response.reasoning_summary_text.delta" | "response.reasoning_summary.delta",
                ) if !compacting => {
                    first_output.get_or_insert_with(|| elapsed(started));
                    let text = event["delta"]
                        .as_str()
                        .ok_or_else(|| failure("invalid gateway reasoning delta"))?;
                    self.emit(
                        "reasoning_delta",
                        text,
                        &item_key(model_call_index, output_index(&event)?),
                        response_id.as_deref(),
                    )
                    .await?;
                    *emitted_output = true;
                }
                Some("response.output_item.done") => {
                    let index = output_index(&event)?;
                    let item = event["item"].clone();
                    if compacting {
                        if done_items.contains_key(&index) {
                            return Err(
                                failure("gateway compaction repeated an output index").into()
                            );
                        }
                        if item["type"] == "compaction"
                            && streamed_compaction.replace(item.clone()).is_some()
                        {
                            return Err(failure(
                                "gateway compaction returned multiple compaction items",
                            )
                            .into());
                        }
                    }
                    first_output.get_or_insert_with(|| elapsed(started));
                    if !compacting {
                        *emitted_output |= self
                            .emit_message(
                                &item,
                                &mut emitted_messages,
                                &item_key(model_call_index, index),
                                response_id.as_deref(),
                            )
                            .await?;
                    }
                    done_items.insert(index, item);
                }
                Some("response.completed") => {
                    let mut response = event["response"].clone();
                    if response["output"].as_array().is_none_or(Vec::is_empty) {
                        if done_items.keys().copied().ne(0..done_items.len()) {
                            return Err(failure(
                                "gateway completion has incomplete output indexes",
                            )
                            .into());
                        }
                        response["output"] = json!(done_items.into_values().collect::<Vec<_>>());
                    }
                    if !compacting && let Some(items) = response["output"].as_array() {
                        for (index, item) in items.iter().enumerate() {
                            *emitted_output |= self
                                .emit_message(
                                    item,
                                    &mut emitted_messages,
                                    &item_key(model_call_index, index),
                                    response_id.as_deref(),
                                )
                                .await?;
                        }
                    }
                    let committed_output =
                        response["output"].as_array().cloned().unwrap_or_default();
                    let response: CompletedResponse = serde_json::from_value(response)
                        .map_err(|_| failure("invalid gateway completion"))?;
                    if response.status != "completed" || response.id.trim().is_empty() {
                        return Err(failure("gateway response did not complete").into());
                    }
                    if compacting {
                        let output = compaction_output(
                            response,
                            streamed_compaction,
                            first_event.unwrap_or_default(),
                            first_output.or_else(|| Some(elapsed(started))),
                            pipeline,
                        )?;
                        return Ok((ResponsesOutput::Compaction(output), Vec::new()));
                    }
                    let output = generation_output(
                        response,
                        first_event.unwrap_or_default(),
                        first_output,
                        pipeline,
                    )?;
                    return Ok((ResponsesOutput::Generation(output), committed_output));
                }
                Some("response.failed" | "response.incomplete" | "response.error" | "error") => {
                    return Err(Failure::event(&event));
                }
                _ => {}
            }
        }
        Err(Failure::connection(
            "gateway stream ended before completion",
        ))
    }

    async fn emit_message(
        &self,
        item: &Value,
        emitted: &mut HashSet<String>,
        item_key: &str,
        response_id: Option<&str>,
    ) -> Result<bool, ResponseError> {
        if item["type"] != "message" || item["role"] != "assistant" {
            return Ok(false);
        }
        if !emitted.insert(item_key.to_owned()) {
            return Ok(false);
        }
        let text = item["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|part| part["type"] == "output_text")
            .filter_map(|part| part["text"].as_str())
            .collect::<String>();
        self.emit("assistant_message", &text, item_key, response_id)
            .await?;
        Ok(true)
    }
}

fn elapsed(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn generation_output(
    response: CompletedResponse,
    first_event: u64,
    first_output: Option<u64>,
    pipeline: ResponsePipelineStats,
) -> Result<GenerationOutput, ResponseError> {
    let mut calls = Vec::new();
    let mut final_message = String::new();
    for item in &response.output {
        match item {
            ResponseItem::FunctionCall {
                name,
                arguments,
                call_id,
                namespace,
                ..
            } => {
                calls.push(CodeCall {
                    call_id: call_id.to_string(),
                    name: name.to_string(),
                    namespace: namespace.as_ref().map(ToString::to_string),
                    input: arguments.to_string(),
                    kind: CodeCallKind::Function,
                });
            }
            ResponseItem::Message { .. } => {
                let value =
                    serde_json::to_value(item).map_err(|_| failure("invalid provider message"))?;
                if value["role"] == "assistant" {
                    final_message.clear();
                    for part in value["content"].as_array().into_iter().flatten() {
                        if part["type"] == "output_text" {
                            final_message.push_str(part["text"].as_str().unwrap_or_default());
                        }
                    }
                }
            }
            ResponseItem::Reasoning { .. } => {}
            _ => return Err(failure("gateway returned an unsupported output item")),
        }
    }
    Ok(GenerationOutput {
        id: response.id,
        reported_model: response.model,
        status: response.status,
        end_turn: response.end_turn,
        final_message: (!final_message.is_empty()).then_some(final_message),
        output_items: response.output,
        code_calls: calls,
        usage: response.usage,
        time_to_first_event_ns: first_event,
        time_to_first_output_ns: first_output,
        pipeline_stats: pipeline,
    })
}

fn compaction_output(
    mut response: CompletedResponse,
    streamed: Option<Value>,
    first_event: u64,
    first_output: Option<u64>,
    pipeline: ResponsePipelineStats,
) -> Result<CompactionOutput, ResponseError> {
    response
        .output
        .retain(|item| matches!(item, ResponseItem::Compaction { .. }));
    if !matches!(response.output.as_slice(), [ResponseItem::Compaction { encrypted_content, .. }] if !encrypted_content.trim().is_empty())
    {
        return Err(failure(
            "gateway compaction requires one encrypted compaction item",
        ));
    }
    if let Some(streamed) = streamed {
        let streamed: ResponseItem = serde_json::from_value(streamed)
            .map_err(|_| failure("invalid gateway compaction item"))?;
        match (&streamed, &response.output[0]) {
            (
                ResponseItem::Compaction {
                    id: streamed_id,
                    encrypted_content: streamed_content,
                    ..
                },
                ResponseItem::Compaction {
                    id,
                    encrypted_content,
                    ..
                },
            ) if streamed_content == encrypted_content
                && streamed_id
                    .as_ref()
                    .zip(id.as_ref())
                    .is_none_or(|(left, right)| left == right) => {}
            _ => {
                return Err(failure(
                    "gateway compaction completion conflicts with streamed output",
                ));
            }
        }
    }
    Ok(CompactionOutput {
        id: response.id,
        status: response.status,
        item: response.output.pop().expect("validated compaction item"),
        usage: response.usage,
        time_to_first_event_ns: first_event,
        time_to_first_output_ns: first_output,
        pipeline_stats: pipeline,
    })
}

fn sends_session_header(base_url: &str) -> bool {
    url::Url::parse(base_url).is_ok_and(|url| {
        url.host_str() == Some("opencode.ai") && url.path().trim_end_matches('/') == "/zen/go/v1"
    })
}

#[derive(Default)]
struct StreamBound {
    event_bytes: usize,
    line_bytes: usize,
    previous_cr: bool,
    buffered: Vec<u8>,
}

impl StreamBound {
    fn accept(&mut self, chunk: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let mut boundary = None;
        for &byte in chunk {
            if byte == b'\n' && self.previous_cr {
                self.previous_cr = false;
                continue;
            }
            self.event_bytes += 1;
            if self.event_bytes > crate::MAX_FRAME_BYTES {
                return Err(std::io::Error::other("gateway event exceeds 32 MiB"));
            }
            self.previous_cr = byte == b'\r';
            if byte == b'\r' || byte == b'\n' {
                self.buffered.push(b'\n');
                if self.line_bytes == 0 {
                    self.event_bytes = 0;
                    boundary = Some(self.buffered.len());
                }
                self.line_bytes = 0;
            } else {
                self.buffered.push(byte);
                self.line_bytes += 1;
            }
        }
        Ok(boundary.map(|boundary| {
            let remaining = self.buffered.split_off(boundary);
            std::mem::replace(&mut self.buffered, remaining)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::sends_session_header;

    #[test]
    fn session_identity_is_only_sent_to_the_opencode_go_endpoint() {
        assert!(sends_session_header("https://opencode.ai/zen/go/v1"));
        assert!(sends_session_header("https://opencode.ai/zen/go/v1/"));
        for endpoint in [
            "https://openrouter.ai/api/v1",
            "http://127.0.0.1:4000/v1",
            "https://opencode.ai.example/zen/go/v1",
            "https://opencode.ai/another-service",
        ] {
            assert!(!sends_session_header(endpoint));
        }
    }
}
