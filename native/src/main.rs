mod checkpoint;
mod gateway;
mod retry;
mod session;

use futures_util::StreamExt;
use nanocodex::{NanocodexError, Turn, TurnControl, TurnResult};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashSet, io};
use tokio::io::{AsyncWriteExt, BufWriter, Stdout};
use tokio_util::codec::{FramedRead, LinesCodec};

use gateway::GatewayEvent;
use session::{Session, SessionError};

const MAX_PROMPT_BYTES: usize = 12 * 1024 * 1024;

const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    id: u64,
    method: String,
    #[serde(default = "empty_params")]
    params: Value,
}

fn empty_params() -> Value {
    json!({})
}

struct ActiveTurn {
    id: u64,
    control: TurnControl,
    turn: Turn,
}

struct Server {
    output: BufWriter<Stdout>,
    session: Option<Session>,
    active: Option<ActiveTurn>,
    seen: HashSet<u64>,
}

impl Server {
    async fn write(&mut self, value: Value) -> io::Result<()> {
        let mut frame = serde_json::to_vec(&value)?;
        if frame.len() > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native protocol frame exceeds 32 MiB",
            ));
        }
        frame.push(b'\n');
        self.output.write_all(&frame).await?;
        self.output.flush().await
    }

    async fn reply(&mut self, id: u64, result: Result<Value, SessionError>) -> io::Result<()> {
        self.write(match result {
            Ok(value) => json!({"id": id, "result": value}),
            Err(error) => json!({"id": id, "error": error}),
        })
        .await
    }

    async fn gateway_write(&mut self, id: u64, event: GatewayEvent) -> io::Result<()> {
        self.write(json!({"event":event.event,"requestId":id,"data":event.data}))
            .await
    }

    /// Writes every pending gateway event under the request whose work caused it.
    async fn gateway_drain(&mut self, id: u64) -> io::Result<()> {
        loop {
            let event = self
                .session
                .as_mut()
                .and_then(|session| session.gateway_events.try_recv().ok());
            let Some(event) = event else { return Ok(()) };
            self.gateway_write(id, event).await?;
        }
    }

    async fn native_event(
        &mut self,
        id: u64,
        data: nanocodex::agent::events::AgentEvent,
    ) -> io::Result<()> {
        use nanocodex::agent::events::AgentEventKind;
        if matches!(
            data.kind,
            AgentEventKind::ModelCallCompleted
                | AgentEventKind::ModelCallFailed
                | AgentEventKind::ToolCall
                | AgentEventKind::AssistantMessage
                | AgentEventKind::RunCompleted
                | AgentEventKind::RunFailed
        ) {
            self.gateway_drain(id).await?;
        }
        let mut value = serde_json::to_value(&data)?;
        if data.kind == AgentEventKind::ModelCompactionFailed
            && value["payload"]["error"] == "compaction cancelled"
        {
            value["payload"]["cancelled"] = json!(true);
        }
        if matches!(
            data.kind,
            AgentEventKind::RunError
                | AgentEventKind::RunFailed
                | AgentEventKind::ModelWarmupFailed
                | AgentEventKind::ModelCallFailed
                | AgentEventKind::ModelCompactionFailed
                | AgentEventKind::ModelAttemptFailed
                | AgentEventKind::ModelAttemptRetrying
                | AgentEventKind::ModelConnectionFailed
        ) && let Some(payload) = value["payload"].as_object_mut()
        {
            for name in ["error", "message"] {
                if let Some(message) = payload.get_mut(name) {
                    *message = json!(
                        "native operation failed; see the terminal error for classified details"
                    );
                }
            }
        }
        if let (Some(session), Some(payload)) = (&self.session, value["payload"].as_object_mut()) {
            session.report_gateway_model(payload);
            session.gateway_layer.report_cache_usage(data.kind, payload);
        }
        self.write(json!({"event": "native", "requestId": id, "data": value}))
            .await
    }

    async fn finish(&mut self, result: nanocodex::agent::Result<TurnResult>) -> io::Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        // Native completion precedes its optional stream consumer; drain before
        // publishing the authoritative terminal reply on the same output pipe.
        loop {
            let event = self
                .session
                .as_mut()
                .and_then(|session| session.events.try_recv_timed());
            let Some(event) = event else { break };
            self.native_event(active.id, event.event).await?;
        }
        self.gateway_drain(active.id).await?;
        let session = self.session.as_mut().expect("active turn owns a session");
        let flushed = session.agent.flush_rollout().await;
        let response = match flushed {
            Err(_) => Err(SessionError::persistence()),
            Ok(()) => match result {
                Ok(result) => {
                    let mut value = session.rollout_state();
                    value["stopReason"] = json!("end_turn");
                    value["finalMessage"] = json!(result.final_message());
                    if let Some(usage) = result.usage() {
                        value["usage"] = json!({
                            "inputTokens": usage.input_tokens(),
                            "cachedInputTokens": usage.cached_input_tokens(),
                            "outputTokens": usage.output_tokens(),
                            "reasoningOutputTokens": usage.reasoning_output_tokens(),
                            "totalTokens": usage.total_tokens(),
                        });
                    }
                    Ok(value)
                }
                Err(NanocodexError::TurnCancelled) => {
                    let mut value = session.rollout_state();
                    value["stopReason"] = json!("cancelled");
                    value["finalMessage"] = json!("");
                    Ok(value)
                }
                Err(error) => Err(session.turn_failed(error)),
            },
        };
        self.reply(active.id, response).await
    }

    async fn close(&mut self) -> io::Result<Result<Value, SessionError>> {
        if let Some(active) = &mut self.active {
            let _ = active.control.cancel().await;
            let result = (&mut active.turn).await;
            self.finish(result).await?;
        }
        let result = if let Some(session) = &self.session {
            session.shutdown().await
        } else {
            Ok(())
        };
        self.session.take();
        Ok(result.map(|()| json!({})))
    }

    async fn recovery_event(
        &mut self,
        id: u64,
        event: nanocodex::agent::events::AgentEvent,
    ) -> io::Result<()> {
        use nanocodex::agent::events::AgentEventKind;
        if matches!(
            event.kind,
            AgentEventKind::ModelCompactionStarted
                | AgentEventKind::ModelCompactionCompleted
                | AgentEventKind::ModelCompactionFailed
        ) {
            self.native_event(id, event).await?;
        }
        Ok(())
    }

    async fn drain_recovery(&mut self, id: u64) -> io::Result<()> {
        loop {
            let event = self
                .session
                .as_mut()
                .and_then(|s| s.events.try_recv_timed());
            let Some(event) = event else { return Ok(()) };
            self.recovery_event(id, event.event).await?;
        }
    }

    async fn start_prompt(
        &mut self,
        id: u64,
        params: Value,
    ) -> io::Result<Result<Turn, SessionError>> {
        let prompt = match Session::prepare_prompt(params) {
            Ok(prompt) => prompt,
            Err(error) => return Ok(Err(error)),
        };
        let agent = self
            .session
            .as_mut()
            .expect("initialized session")
            .compaction_agent();
        if let Some(agent) = agent {
            let compact = agent.compact();
            tokio::pin!(compact);
            let result = loop {
                let session = self.session.as_mut().expect("initialized session");
                tokio::select! {
                    result = &mut compact => break result,
                    event = session.events.recv() => {
                        if let Some(event) = event { self.recovery_event(id, event).await?; }
                    }
                    event = session.gateway_events.recv(), if !session.gateway_events.is_closed() => {
                        if let Some(event) = event { self.gateway_write(id, event).await?; }
                    }
                }
            };
            self.drain_recovery(id).await?;
            self.gateway_drain(id).await?;
            if let Err(error) = result {
                return Ok(Err(SessionError::native(error)));
            }
            self.session
                .as_mut()
                .expect("initialized session")
                .compaction_finished();
        }
        Ok(self
            .session
            .as_mut()
            .expect("initialized session")
            .prompt(prompt)
            .await)
    }

    async fn request(&mut self, request: Request) -> io::Result<bool> {
        if request.id == 0 || !self.seen.insert(request.id) || !request.params.is_object() {
            self.reply(request.id, Err(SessionError::invalid_request()))
                .await?;
            return Ok(true);
        }
        let result = match request.method.as_str() {
            "initialize" => {
                if self.session.is_some() {
                    Err(SessionError::new(
                        "already_initialized",
                        "session already initialized",
                    ))
                } else {
                    match Session::open(request.params).await {
                        Ok(session) => {
                            let result = session.state();
                            self.session = Some(session);
                            Ok(result)
                        }
                        Err(error) => Err(error),
                    }
                }
            }
            "shutdown" => {
                let result = self.close().await?;
                self.reply(request.id, result).await?;
                return Ok(false);
            }
            "prompt" => {
                if self.active.is_some() {
                    Err(SessionError::busy())
                } else if self.session.is_some() {
                    match self.start_prompt(request.id, request.params).await? {
                        Ok(turn) => {
                            self.write(json!({"event":"accepted", "requestId":request.id,
                                "data":{"turnId":turn.id()}}))
                                .await?;
                            self.active = Some(ActiveTurn {
                                id: request.id,
                                control: turn.control(),
                                turn,
                            });
                            return Ok(true);
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    Err(SessionError::not_initialized())
                }
            }
            "cancel" => {
                if let Some(active) = &self.active {
                    match active.control.cancel().await {
                        Ok(()) => Ok(json!({"cancelled":true})),
                        Err(NanocodexError::TurnNotCancellable) => Ok(json!({"cancelled":false})),
                        Err(error) => Err(SessionError::native(error)),
                    }
                } else {
                    Ok(json!({"cancelled":false}))
                }
            }
            "state" => {
                if self.active.is_some() {
                    Err(SessionError::busy())
                } else if let Some(session) = &mut self.session {
                    session.flush_state().await
                } else {
                    Err(SessionError::not_initialized())
                }
            }
            _ => Err(SessionError::new("method_not_found", "unknown method")),
        };
        self.reply(request.id, result).await?;
        Ok(true)
    }

    async fn run(&mut self) -> io::Result<()> {
        let mut input = FramedRead::new(
            tokio::io::stdin(),
            LinesCodec::new_with_max_length(MAX_FRAME_BYTES),
        );
        loop {
            let gateway_active = self.active.is_some()
                && self
                    .session
                    .as_ref()
                    .is_some_and(|session| !session.gateway_events.is_closed());
            let (native_events, gateway_events) = match self.session.as_mut() {
                Some(session) => (Some(&mut session.events), Some(&mut session.gateway_events)),
                None => (None, None),
            };
            tokio::select! {
                result = async {
                    match &mut self.active {
                        Some(active) => (&mut active.turn).await,
                        None => std::future::pending().await,
                    }
                } => self.finish(result).await?,
                event = async {
                    match native_events {
                        Some(events) => events.recv().await,
                        None => std::future::pending().await,
                    }
                }, if self.active.is_some() => {
                    if let Some(event) = event {
                        let id = self.active.as_ref().expect("active event stream").id;
                        self.native_event(id, event).await?;
                    }
                }
                event = async {
                    match gateway_events {
                        Some(events) => events.recv().await,
                        None => std::future::pending().await,
                    }
                }, if gateway_active => {
                    if let Some(event) = event {
                        let id = self.active.as_ref().expect("active gateway stream").id;
                        self.gateway_write(id, event).await?;
                    }
                }
                line = input.next() => {
                    match line {
                        None => { self.close().await??; break; }
                        Some(Err(_)) => {
                            self.write(json!({"id":null,"error":SessionError::invalid_request()})).await?;
                            self.close().await??;
                            break;
                        }
                        Some(Ok(line)) => {
                            match serde_json::from_str::<Request>(&line) {
                                Ok(request) => if !self.request(request).await? { break; },
                                Err(_) => self.write(json!({"id":null,"error":SessionError::invalid_request()})).await?,
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl From<SessionError> for io::Error {
    fn from(error: SessionError) -> Self {
        io::Error::other(error.message)
    }
}

#[tokio::main]
async fn main() {
    let mut server = Server {
        output: BufWriter::new(tokio::io::stdout()),
        session: None,
        active: None,
        seen: HashSet::new(),
    };
    if server.run().await.is_err() {
        if let Some(active) = server.active.take() {
            let _ = active.control.cancel().await;
        }
        if let Some(session) = server.session.take() {
            let _ = session.agent.shutdown().await;
        }
        eprintln!("nanocodex helper stopped after a transport or persistence failure");
        std::process::exit(1);
    }
}
