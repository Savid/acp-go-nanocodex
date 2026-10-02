use crate::gateway::{GatewayConfig, GatewayEvent, GatewayLayer};
use nanocodex::{
    AgentEvents, Model, Nanocodex, NanocodexError, OpenAi, Thinking, Turn,
    agent::rollout::RolloutConfig,
    oai::{
        Prompt, UserInput,
        auth::{OpenAiAuth, OpenAiAuthMode, load_chatgpt_auth},
        session::SessionId,
        transport::{ResponsesHistory, ResponsesTransport},
    },
    tools::{ToolExposure, Tools},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env,
    error::Error,
    fmt,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    str::FromStr,
};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionError {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<&'static str>,
    pub code: &'static str,
    pub message: &'static str,
}

impl SessionError {
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message,
            status_code: None,
            provider_code: None,
            field: None,
        }
    }
    pub const fn invalid_request() -> Self {
        Self::new("invalid_request", "invalid request")
    }
    pub const fn invalid_config() -> Self {
        Self::new("invalid_config", "invalid native configuration")
    }
    pub fn config(field: &'static str) -> Self {
        Self {
            field: Some(field),
            ..Self::invalid_config()
        }
    }
    pub fn http(status: u16) -> Self {
        Self {
            status_code: Some(status),
            ..Self::new("native_error", "provider rejected the request")
        }
    }
    pub fn provider_event(event: &Value) -> Self {
        let error = event
            .get("error")
            .filter(|error| error.is_object())
            .or_else(|| {
                event
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .filter(|error| error.is_object())
            })
            .unwrap_or(event);
        let provider_code = error.get("code").and_then(Value::as_str).filter(|code| {
            !code.is_empty()
                && code.len() <= 128
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        });
        let message = match provider_code {
            Some("rate_limit_exceeded" | "too_many_requests" | "rate_limit_error") => {
                "provider rate limit reached"
            }
            Some("insufficient_quota" | "quota_exceeded" | "insufficient_balance") => {
                "provider quota exhausted"
            }
            Some("invalid_api_key" | "authentication_error" | "unauthorized") => {
                "provider authorization failed"
            }
            Some("model_not_found" | "unsupported_model") => "provider model is unavailable",
            Some("context_length_exceeded" | "context_window_exceeded") => {
                "provider context window exceeded"
            }
            Some("server_error" | "internal_server_error") => "provider service failed",
            Some("overloaded_error" | "server_overloaded") => "provider is temporarily overloaded",
            _ => "provider response did not complete",
        };
        Self {
            provider_code: provider_code.map(str::to_owned),
            ..Self::new("native_error", message)
        }
    }
    pub fn native(error: NanocodexError) -> Self {
        let mut source: Option<&(dyn Error + 'static)> = Some(&error);
        while let Some(current) = source {
            if let Some(error) = current.downcast_ref::<Self>() {
                return error.clone();
            }
            source = current.source();
        }
        use nanocodex::oai::transport::ResponsesError;
        if let Some(error) = error.responses_error() {
            return match error {
                ResponsesError::HttpRejected { status, .. }
                | ResponsesError::HandshakeRejected { status, .. } => Self::http(*status),
                ResponsesError::Authorization { .. }
                | ResponsesError::InvalidAuthorization { .. } => {
                    Self::new("native_error", "provider authorization failed")
                }
                ResponsesError::ContextWindowExceeded { .. } => {
                    Self::new("native_error", "provider context window exceeded")
                }
                ResponsesError::InvalidImageRequest { .. } => {
                    Self::new("native_error", "provider rejected image input")
                }
                ResponsesError::InvalidToolSchema { .. } => {
                    Self::new("native_error", "provider rejected a tool schema")
                }
                ResponsesError::Api { event } => serde_json::from_str::<Value>(event).map_or_else(
                    |_| Self::new("native_error", "provider returned an error event"),
                    |event| Self::provider_event(&event),
                ),
                ResponsesError::HandshakeTimeout { .. } | ResponsesError::SendTimeout { .. } => {
                    Self::new("transport_error", "provider connection timed out")
                }
                ResponsesError::HttpRequest { timeout: true, .. } => {
                    Self::new("transport_error", "provider request timed out")
                }
                ResponsesError::Handshake { .. } | ResponsesError::HttpRequest { .. } => {
                    Self::new("transport_error", "provider connection failed")
                }
                ResponsesError::UnexpectedEnd | ResponsesError::Closed { .. } => Self::new(
                    "transport_error",
                    "provider connection ended before completion",
                ),
                _ => Self::new(
                    "transport_error",
                    "provider transport returned invalid data",
                ),
            };
        }
        match error {
            NanocodexError::AgentStopped | NanocodexError::TurnStopped => Self::new(
                "transport_error",
                "native agent stopped before turn completion",
            ),
            NanocodexError::PersistRollout { .. } | NanocodexError::InitializeRollout { .. } => {
                Self::persistence()
            }
            NanocodexError::InvalidRequest(_) => Self::invalid_request(),
            NanocodexError::MalformedResponse { .. }
            | NanocodexError::InvalidAttemptState { .. } => Self::new(
                "transport_error",
                "provider response violated the native protocol",
            ),
            _ => Self::new("native_error", "native agent could not complete the turn"),
        }
    }
    pub const fn restore(message: &'static str) -> Self {
        Self::new("restore_failed", message)
    }
    pub const fn persistence() -> Self {
        Self::new("persistence", "native persistence failed")
    }
    pub const fn busy() -> Self {
        Self::new("busy", "a turn is already active")
    }
    pub const fn not_initialized() -> Self {
        Self::new("not_initialized", "initialize a session first")
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}
impl Error for SessionError {}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    session_id: String,
    model: Option<String>,
    thinking: Option<String>,
    api_base_url: Option<String>,
    websocket_url: Option<String>,
    model_id_prefix: Option<String>,
    transport: Option<String>,
    api_key_env: Option<String>,
    auth_file: Option<PathBuf>,
    resume_session_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptParams {
    content: Vec<UserInput>,
}

pub struct Session {
    pub agent: Nanocodex,
    pub events: AgentEvents,
    pub gateway_events: tokio::sync::mpsc::Receiver<GatewayEvent>,
    model: Model,
    thinking: Thinking,
    text_events: bool,
    replaced_native_session_id: Option<String>,
}

fn context_window(model: Model) -> u64 {
    nanocodex::oai::tower::ResponsesServiceConfig::default()
        .context_window_tokens
        .min(model.max_context_window_tokens())
}

fn env_value(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn native_home(workspace: &Path) -> Result<PathBuf, SessionError> {
    let home = env::var_os("CODEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|value| PathBuf::from(value).join(".codex"))
        })
        .ok_or_else(SessionError::invalid_config)?;
    Ok(if home.is_absolute() {
        home
    } else {
        workspace.join(home)
    })
}

fn verified_empty_rollout(home: &Path, id: &str, workspace: &Path) -> Result<bool, SessionError> {
    let suffix = format!("-{id}.jsonl");
    let mut directories = vec![home.join("sessions"), home.join("archived_sessions")];
    let mut found = None;
    while let Some(directory) = directories.pop() {
        let entries = match std::fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(SessionError::restore("native rollout could not be read")),
        };
        for entry in entries {
            let entry =
                entry.map_err(|_| SessionError::restore("native rollout could not be read"))?;
            let kind = entry
                .file_type()
                .map_err(|_| SessionError::restore("native rollout could not be read"))?;
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file()
                && entry.file_name().to_string_lossy().ends_with(&suffix)
                && found.replace(entry.path()).is_some()
            {
                return Ok(false);
            }
        }
    }
    let Some(path) = found else { return Ok(false) };
    let file =
        File::open(path).map_err(|_| SessionError::restore("native rollout could not be read"))?;
    let mut lines = BufReader::new(file).lines();
    let Some(Ok(line)) = lines.next() else {
        return Ok(false);
    };
    if lines.next().is_some() {
        return Ok(false);
    }
    let Ok(row) = serde_json::from_str::<Value>(&line) else {
        return Ok(false);
    };
    Ok(row["type"] == "session_meta"
        && row["payload"]["id"] == id
        && row["payload"]["originator"] == "nanocodex"
        && row["payload"]["cwd"]
            .as_str()
            .is_some_and(|cwd| Path::new(cwd) == workspace))
}

fn endpoint(value: &str, websocket: bool, field: &'static str) -> Result<(), SessionError> {
    let url = url::Url::parse(value).map_err(|_| SessionError::config(field))?;
    let allowed = if websocket {
        matches!(url.scheme(), "wss" | "ws")
    } else {
        matches!(url.scheme(), "https" | "http")
    };
    let loopback = match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    if !allowed
        || (matches!(url.scheme(), "http" | "ws") && !loopback)
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(SessionError::config(field));
    }
    Ok(())
}

impl Session {
    pub async fn open(params: Value) -> Result<Self, SessionError> {
        let mut config: Config =
            serde_json::from_value(params).map_err(|_| SessionError::invalid_request())?;
        config.api_base_url = config.api_base_url.or_else(|| env_value("OPENAI_BASE_URL"));
        config.model_id_prefix = config
            .model_id_prefix
            .or_else(|| env_value("NANOCODEX_MODEL_ID_PREFIX"));
        config.transport = config
            .transport
            .or_else(|| env_value("NANOCODEX_TRANSPORT"));
        config.api_key_env = config
            .api_key_env
            .or_else(|| env_value("NANOCODEX_API_KEY_ENV"));
        let workspace = env::current_dir().map_err(|_| SessionError::invalid_config())?;
        let home = native_home(&workspace)?;
        let mut model = config
            .model
            .as_deref()
            .map(Model::from_str)
            .transpose()
            .map_err(|_| SessionError::config("model"))?
            .unwrap_or_default();
        let mut replaced_native_session_id = None;
        let resume = if let Some(id) = config.resume_session_id.as_deref() {
            match RolloutConfig::new(&home).load_session(id) {
                Ok(session) => Some(session),
                Err(error) => {
                    let no_user_history = matches!(error.get_ref().and_then(|source| source.downcast_ref::<NanocodexError>()),
                        Some(NanocodexError::InvalidSessionSnapshot(detail)) if detail == "rollout does not contain a user message");
                    if !no_user_history || !verified_empty_rollout(&home, id, &workspace)? {
                        return Err(SessionError::restore(
                            "native rollout could not be restored",
                        ));
                    }
                    replaced_native_session_id = Some(id.to_owned());
                    None
                }
            }
        } else {
            None
        };
        if let Some(resume) = &resume {
            if config.model.is_some() && model != resume.model() {
                return Err(SessionError::config("model"));
            }
            if Path::new(resume.workspace()) != workspace {
                return Err(SessionError::restore(
                    "restored native workspace does not match requested workspace",
                ));
            }
            model = resume.model();
        }
        if replaced_native_session_id.as_deref() == Some(config.session_id.as_str()) {
            return Err(SessionError::config("sessionId"));
        }
        let session_id = config
            .session_id
            .parse::<SessionId>()
            .map_err(|_| SessionError::config("sessionId"))?;
        if resume
            .as_ref()
            .is_some_and(|resume| resume.thread_id() != session_id.to_string())
        {
            return Err(SessionError::config("sessionId"));
        }
        let thinking = config
            .thinking
            .as_deref()
            .map(Thinking::from_str)
            .transpose()
            .map_err(|_| SessionError::config("thinking"))?
            .unwrap_or_else(|| model.default_thinking());
        if !model.supports_thinking(thinking) {
            return Err(SessionError::config("thinking"));
        }
        let api_key_env = config.api_key_env.as_deref().unwrap_or("OPENAI_API_KEY");
        if api_key_env.is_empty() || api_key_env.contains(['=', '\0']) {
            return Err(SessionError::config("apiKeyEnv"));
        }
        let auth = match env::var(api_key_env)
            .ok()
            .filter(|key| !key.trim().is_empty())
        {
            Some(key) => OpenAiAuth::api_key(key),
            None if config.api_key_env.is_some() => {
                return Err(SessionError::new(
                    "authentication",
                    "configured API key is unavailable",
                ));
            }
            None => load_chatgpt_auth(config.auth_file.unwrap_or_else(|| home.join("auth.json")))
                .map_err(|_| {
                SessionError::new("authentication", "native authentication is unavailable")
            })?,
        };
        if auth.mode() == OpenAiAuthMode::ChatGpt {
            if config.api_base_url.is_some() {
                return Err(SessionError {
                    message: "ChatGPT authentication refuses custom API endpoints; remove apiBaseUrl and OPENAI_BASE_URL or configure an API key",
                    ..SessionError::config("apiBaseUrl")
                });
            }
            if config.websocket_url.is_some() {
                return Err(SessionError {
                    message: "ChatGPT authentication refuses custom WebSocket endpoints; remove websocketUrl or configure an API key",
                    ..SessionError::config("websocketUrl")
                });
            }
        }
        if let Some(url) = &config.api_base_url {
            endpoint(url, false, "apiBaseUrl")?;
        }
        if let Some(url) = &config.websocket_url {
            endpoint(url, true, "websocketUrl")?;
        }
        let transport = config
            .transport
            .as_deref()
            .unwrap_or("https")
            .parse::<ResponsesTransport>()
            .map_err(|_| SessionError::config("transport"))?;
        if let Some(prefix) = config.model_id_prefix.as_deref()
            && (prefix.is_empty()
                || prefix.split('/').any(str::is_empty)
                || prefix
                    .chars()
                    .any(|character| character.is_whitespace() || character.is_control())
                || auth.mode() != OpenAiAuthMode::ApiKey
                || transport != ResponsesTransport::Https)
        {
            return Err(SessionError::config("modelIdPrefix"));
        }
        let gateway =
            if transport == ResponsesTransport::Https && auth.mode() == OpenAiAuthMode::ApiKey {
                config.api_base_url.as_ref().map(|base_url| GatewayConfig {
                    auth: auth.clone(),
                    session_id: session_id.to_string(),
                    base_url: base_url.clone(),
                    model_id_prefix: config
                        .model_id_prefix
                        .as_ref()
                        .map(|prefix| prefix.trim().to_owned()),
                })
            } else {
                None
            };
        let text_events = gateway.is_some();
        let (gateway_layer, gateway_events) =
            GatewayLayer::new(gateway).map_err(|_| SessionError::invalid_config())?;
        let mut openai = OpenAi::builder(auth)
            .model(model)
            .thinking(thinking)
            .context_window_tokens(context_window(model))
            .transport(transport)
            .store(false)
            .websocket_warmup(false)
            .raw_api_events(false);
        if transport == ResponsesTransport::Https {
            openai = openai.history(ResponsesHistory::FullReplay);
        }
        if let Some(url) = config.api_base_url {
            openai = openai.api_base_url(url);
        }
        if let Some(url) = config.websocket_url {
            openai = openai.websocket_url(url);
        }
        if let Some(prefix) = config.model_id_prefix {
            openai = openai.model_id_prefix(prefix);
        }
        let openai = openai
            .layer(gateway_layer)
            .build()
            .map_err(|_| SessionError::invalid_config())?;
        let tools = Tools::builder()
            .exposure(ToolExposure::DirectOnly)
            .web_search(false)
            .image_generation(false)
            .build()
            .map_err(|_| SessionError::invalid_config())?;
        let mut builder = Nanocodex::builder(openai)
            .session_id(session_id)
            .workspace(&workspace)
            .codex_home(&home)
            .tools(tools);
        if let Some(resume) = resume {
            let (_, snapshot, rollout) = resume.into_parts();
            builder = builder.resume(snapshot).rollout(rollout);
        } else {
            builder = builder.rollout(RolloutConfig::new(&home));
        }
        let (agent, events) = builder.build().map_err(SessionError::native)?;
        if agent.flush_rollout().await.is_err() {
            let _ = agent.shutdown().await;
            return Err(SessionError::persistence());
        }
        Ok(Self {
            agent,
            events,
            gateway_events,
            model,
            thinking,
            text_events,
            replaced_native_session_id,
        })
    }

    pub fn rollout_state(&self) -> Value {
        let rollout = self.agent.rollout().expect("rollout recording enabled");
        json!({"nativeSessionId": self.agent.session_id(), "rolloutPath":rollout.path(), "committedBytes":rollout.committed_bytes()})
    }

    pub fn state(&self) -> Value {
        let mut state = self.rollout_state();
        state["protocolVersion"] = json!(1);
        state["helperVersion"] = json!(env!("CARGO_PKG_VERSION"));
        state["helperFingerprint"] = json!(env!("NANOCODEX_HELPER_FINGERPRINT"));
        if let Some(id) = &self.replaced_native_session_id {
            state["replacedNativeSessionId"] = json!(id);
        }
        state["model"] = json!(self.model.as_str());
        state["textEvents"] = json!(self.text_events);
        state["thinking"] = json!(self.thinking.as_str());
        let mut models = Model::ALL.to_vec();
        if !models.contains(&self.model) {
            models.push(self.model);
        }
        state["models"] = json!(models.iter().map(|model| json!({
            "id": model.as_str(), "name": model.as_str(), "defaultThinking": model.default_thinking().as_str(),
            "contextWindow": context_window(*model),
            "thinking": Thinking::ALL.iter().filter(|thinking| model.supports_thinking(**thinking)).map(|thinking| thinking.as_str()).collect::<Vec<_>>()
        })).collect::<Vec<_>>());
        state
    }

    pub async fn prompt(&mut self, params: Value) -> Result<Turn, SessionError> {
        let params: PromptParams =
            serde_json::from_value(params).map_err(|_| SessionError::invalid_request())?;
        if params
            .content
            .iter()
            .any(|input| !matches!(input, UserInput::Text { .. } | UserInput::Image { .. }))
        {
            return Err(SessionError::invalid_request());
        }
        let prompt = Prompt::content(params.content);
        prompt
            .validate()
            .map_err(|_| SessionError::invalid_request())?;
        let turn = self
            .agent
            .prompt(prompt)
            .await
            .map_err(SessionError::native)?;
        Ok(turn)
    }

    pub async fn flush_state(&self) -> Result<Value, SessionError> {
        self.agent
            .flush_rollout()
            .await
            .map_err(|_| SessionError::persistence())?;
        Ok(self.state())
    }
}
