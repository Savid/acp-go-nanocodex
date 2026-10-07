use crate::{
    checkpoint,
    gateway::{GatewayConfig, GatewayEvent, GatewayLayer, gateway_identity},
};
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
    #[serde(skip)]
    source: Option<std::sync::Arc<nanocodex::oai::transport::ResponsesError>>,
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
            source: None,
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
    /// Reports the first terminal classification candidate, else the first
    /// one, so the reported code matches the retry decision.
    pub fn provider_event(event: &Value) -> Self {
        let codes = crate::retry::provider_codes(event)
            .into_iter()
            .filter(|code| {
                !code.is_empty()
                    && code.len() <= 128
                    && code.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                    })
            })
            .collect::<Vec<_>>();
        let provider_code = codes
            .iter()
            .find(|code| crate::retry::is_terminal(code))
            .or(codes.first())
            .cloned();
        let message = match provider_code.as_deref() {
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
            provider_code,
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
                    Self::connection("provider connection timed out")
                }
                ResponsesError::HttpRequest { timeout: true, .. } => {
                    Self::connection("provider request timed out")
                }
                ResponsesError::Handshake { .. } | ResponsesError::HttpRequest { .. } => {
                    Self::connection("provider connection failed")
                }
                ResponsesError::UnexpectedEnd | ResponsesError::Closed { .. } => {
                    Self::connection("provider connection ended before completion")
                }
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
    /// A provider or gateway connection failed, timed out, or ended before
    /// the response completed.
    pub const fn connection(message: &'static str) -> Self {
        Self::new("connection_error", message)
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
impl Error for SessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|error| error as &(dyn Error + 'static))
    }
}

impl SessionError {
    pub fn with_provider_source(mut self) -> Self {
        use nanocodex::oai::transport::ResponsesError;
        let event = json!({"error":{"code":self.provider_code}}).to_string();
        self.source = match self.provider_code.as_deref() {
            Some("context_length_exceeded" | "context_window_exceeded") => {
                Some(std::sync::Arc::new(ResponsesError::ContextWindowExceeded {
                    event,
                }))
            }
            Some(
                "invalid_image"
                | "image_too_large"
                | "image_too_small"
                | "unsupported_image_format"
                | "image_not_found",
            ) => Some(std::sync::Arc::new(ResponsesError::InvalidImageRequest {
                event,
            })),
            _ => None,
        };
        self
    }
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    session_id: String,
    model: Option<String>,
    base_model: Option<String>,
    context_window: Option<u64>,
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
    pub gateway_layer: GatewayLayer,
    /// The selected native model, or the base model whose settings a gateway
    /// model uses.
    model: Model,
    gateway_model: Option<String>,
    /// The configured window; without one the native runtime uses the
    /// model's default window.
    context_window: Option<u64>,
    thinking: Thinking,
    text_events: bool,
    replaced_native_session_id: Option<String>,
    prompted: bool,
    compact_next: bool,
    _writer_locks: Vec<File>,
}

fn context_window(model: Model) -> u64 {
    nanocodex::oai::tower::ResponsesServiceConfig::default()
        .context_window_tokens
        .min(model.max_context_window_tokens())
}

fn has_space_or_control(text: &str) -> bool {
    text.chars()
        .any(|character| character.is_whitespace() || character.is_control())
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

fn writer_locks(home: &Path, config: &Config) -> Result<Vec<File>, SessionError> {
    use std::os::unix::fs::OpenOptionsExt;
    let directory = home.join("nanocodex/acp-locks");
    std::fs::create_dir_all(&directory).map_err(|_| SessionError::persistence())?;
    let mut locks = Vec::new();
    let mut identities = vec![config.session_id.as_str()];
    if let Some(id) = config.resume_session_id.as_deref()
        && id != config.session_id
    {
        identities.push(id);
    }
    for id in identities {
        if !uuid::Uuid::parse_str(id).is_ok_and(|parsed| parsed.to_string() == id) {
            return Err(SessionError::invalid_config());
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join(format!("{id}.writer.lock")))
            .map_err(|_| SessionError::persistence())?;
        file.try_lock()
            .map_err(|_| SessionError::restore("native rollout writer is active"))?;
        locks.push(file);
    }
    Ok(locks)
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
        if config.context_window.is_none() {
            config.context_window = env_value("NANOCODEX_CONTEXT_WINDOW")
                .map(|value| value.parse())
                .transpose()
                .map_err(|_| SessionError::config("contextWindow"))?;
        }
        let workspace = env::current_dir().map_err(|_| SessionError::invalid_config())?;
        let home = native_home(&workspace)?;
        let writer_locks = writer_locks(&home, &config)?;
        // A model outside the native catalog is a gateway model, which runs
        // with the settings of a native base model.
        let mut model = Model::default();
        let mut gateway_model = None;
        if let Some(id) = config.model.as_deref() {
            match Model::from_str(id) {
                Ok(native) => model = native,
                Err(_) if id.is_empty() || has_space_or_control(id) => {
                    return Err(SessionError::config("model"));
                }
                Err(_) => gateway_model = Some(id.to_owned()),
            }
        }
        let transport = config
            .transport
            .as_deref()
            .unwrap_or("https")
            .parse::<ResponsesTransport>()
            .map_err(|_| SessionError::config("transport"))?;
        // Only an HTTPS route with a configured endpoint can serve a gateway
        // model.
        if gateway_model.is_some()
            && (config.api_base_url.is_none() || transport != ResponsesTransport::Https)
        {
            return Err(SessionError::config("model"));
        }
        let base_model = match config.base_model.as_deref() {
            Some(_) if gateway_model.is_none() => return Err(SessionError::config("baseModel")),
            Some(id) => Some(Model::from_str(id).map_err(|_| SessionError::config("baseModel"))?),
            None => None,
        };
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
            if gateway_model.is_none() && config.model.is_some() && model != resume.model() {
                return Err(SessionError::config("model"));
            }
            if base_model.is_some_and(|base| base != resume.model()) {
                return Err(SessionError::config("baseModel"));
            }
            if Path::new(resume.workspace()) != workspace {
                return Err(SessionError::restore(
                    "restored native workspace does not match requested workspace",
                ));
            }
            model = resume.model();
        } else if gateway_model.is_some() {
            model = match base_model {
                Some(base) => base,
                None => env_value("NANOCODEX_BASE_MODEL")
                    .as_deref()
                    .map(Model::from_str)
                    .transpose()
                    .map_err(|_| SessionError::config("baseModel"))?
                    .unwrap_or(Model::Luna),
            };
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
        // The native runtime caps a larger window silently, so refuse it instead.
        if config
            .context_window
            .is_some_and(|tokens| tokens == 0 || tokens > model.max_context_window_tokens())
        {
            return Err(SessionError::config("contextWindow"));
        }
        if let Some(gateway_model) = &gateway_model {
            let config = nanocodex::oai::tower::ResponsesServiceConfig {
                model,
                ..Default::default()
            };
            if gateway_identity(&config.system_prompt(), gateway_model).is_none() {
                return Err(SessionError::config("baseModel"));
            }
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
            if gateway_model.is_some() {
                return Err(SessionError::config("model"));
            }
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
        if let Some(prefix) = config.model_id_prefix.as_deref()
            && (prefix.is_empty()
                || prefix.split('/').any(str::is_empty)
                || has_space_or_control(prefix)
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
                    model: gateway_model.clone(),
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
            .context_window_tokens(
                config
                    .context_window
                    .unwrap_or_else(|| context_window(model)),
            )
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
            .layer(gateway_layer.clone())
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
        let mut compact_next = false;
        if let Some(resume) = resume {
            let path = resume.rollout_path().to_owned();
            let (_, snapshot, rollout) = resume.into_parts();
            let (snapshot, overflowed) = checkpoint::restore(&path, snapshot)
                .map_err(|_| SessionError::restore("native checkpoint could not be restored"))?;
            compact_next = overflowed;
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
            gateway_layer,
            model,
            gateway_model,
            context_window: config.context_window,
            thinking,
            text_events,
            replaced_native_session_id,
            prompted: false,
            compact_next,
            _writer_locks: writer_locks,
        })
    }

    /// Reports a gateway model in place of its base model in a native event
    /// payload and drops the cost estimates priced at the base model's rates.
    pub fn report_gateway_model(&self, payload: &mut serde_json::Map<String, Value>) {
        let Some(gateway_model) = &self.gateway_model else {
            return;
        };
        if let Some(model) = payload.get_mut("model")
            && model.as_str() == Some(self.model.as_str())
        {
            *model = json!(gateway_model);
        }
        for name in ["estimated_cost", "cost_usd", "cost_status"] {
            payload.remove(name);
        }
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
        state["textEvents"] = json!(self.text_events);
        state["thinking"] = json!(self.thinking.as_str());
        let row = |id: &str, model: Model, window: Option<u64>| {
            let mut row = json!({
                "id": id, "name": id, "defaultThinking": model.default_thinking().as_str(),
                "thinking": Thinking::ALL.iter().filter(|thinking| model.supports_thinking(**thinking)).map(|thinking| thinking.as_str()).collect::<Vec<_>>()
            });
            if let Some(window) = window {
                row["contextWindow"] = json!(window);
            }
            row
        };
        let native_window = |model: Model| {
            let selected = self.gateway_model.is_none() && model == self.model;
            Some(
                self.context_window
                    .filter(|_| selected)
                    .unwrap_or_else(|| context_window(model)),
            )
        };
        let mut models = Model::ALL
            .iter()
            .map(|model| row(model.as_str(), *model, native_window(*model)))
            .collect::<Vec<_>>();
        // The selected row is the gateway model when there is one, else the
        // native model. A gateway model's row carries its base model's
        // efforts and reports only a configured window, since its real window
        // is unknown.
        if let Some(id) = &self.gateway_model {
            state["model"] = json!(id);
            models.push(row(id, self.model, self.context_window));
        } else {
            state["model"] = json!(self.model.as_str());
            if !Model::ALL.contains(&self.model) {
                models.push(row(
                    self.model.as_str(),
                    self.model,
                    native_window(self.model),
                ));
            }
        }
        state["models"] = json!(models);
        state
    }

    pub async fn prompt(&mut self, params: Value) -> Result<Turn, SessionError> {
        if serde_json::to_vec(&params)
            .map_err(|_| SessionError::invalid_request())?
            .len()
            > crate::MAX_PROMPT_BYTES
        {
            return Err(SessionError::new(
                "invalid_request",
                "prompt content exceeds 12 MiB",
            ));
        }
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
        if self.compact_next {
            // Compaction changes native state, so shutdown refreshes the checkpoint.
            self.prompted = true;
            if let Err(error) = self.agent.compact().await {
                // These events precede acceptance, so no prompt can carry them.
                while self.events.try_recv_timed().is_some() {}
                return Err(SessionError::native(error));
            }
            self.compact_next = false;
        }
        let turn = self
            .agent
            .prompt(prompt)
            .await
            .map_err(SessionError::native)?;
        self.prompted = true;
        Ok(turn)
    }

    /// Classifies a failed turn. A context-window overflow makes the next
    /// prompt compact first, including in a later helper.
    pub fn turn_failed(&mut self, error: NanocodexError) -> SessionError {
        let overflow = error
            .responses_error()
            .is_some_and(nanocodex::oai::transport::ResponsesError::is_context_window_exceeded);
        let error = SessionError::native(error);
        self.compact_next |= overflow
            || matches!(
                error.provider_code.as_deref(),
                Some("context_length_exceeded" | "context_window_exceeded")
            );
        error
    }

    pub async fn flush_state(&self) -> Result<Value, SessionError> {
        self.agent
            .flush_rollout()
            .await
            .map_err(|_| SessionError::persistence())?;
        Ok(self.state())
    }

    pub async fn shutdown(&self) -> Result<(), SessionError> {
        let snapshot = if self.prompted {
            Some(self.agent.snapshot().await)
        } else {
            None
        };
        self.agent
            .shutdown()
            .await
            .map_err(|_| SessionError::persistence())?;
        match snapshot {
            Some(Ok(snapshot)) => {
                if checkpoint::save(
                    self.agent
                        .rollout()
                        .expect("rollout recording enabled")
                        .path(),
                    snapshot,
                    self.compact_next,
                )
                .is_err()
                {
                    eprintln!(
                        "native checkpoint could not be saved; native history remains committed"
                    );
                }
            }
            None | Some(Err(NanocodexError::ForkBeforeCompletedTurn)) => {}
            Some(Err(_)) => {
                eprintln!("native checkpoint unavailable; native history remains committed")
            }
        }
        Ok(())
    }
}
