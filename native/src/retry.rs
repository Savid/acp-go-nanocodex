use crate::session::SessionError;
use nanocodex::oai::ResponseError;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::{
    error::Error,
    hash::{BuildHasher, RandomState},
    time::{Duration, SystemTime},
};

const MAX_ATTEMPTS: u32 = 5;
const MAX_SERVER_DELAY: Duration = Duration::from_secs(60);
const MAX_ERROR_BYTES: usize = 64 * 1024;
const ERROR_BODY_TIMEOUT: Duration = Duration::from_secs(2);

/// Provider classifications that a new attempt cannot change.
#[rustfmt::skip]
const TERMINAL: &[&str] = &[
    // Quota and billing.
    "insufficient_quota", "quota_exceeded", "insufficient_balance", "usage_not_included",
    "credit_balance_exhausted", "organization_spend_limit_exceeded",
    "project_spend_limit_exceeded", "payment_required",
    // Authorization and model access.
    "invalid_api_key", "authentication_error", "authentication", "unauthorized",
    "permission_denied", "model_not_found", "unsupported_model",
    // Invalid requests and context limits.
    "invalid_request_error", "invalid_request", "invalid_prompt", "not_found",
    "precondition_failed", "payload_too_large", "unprocessable", "string_too_long",
    "data_residency_mismatch", "context_length_exceeded", "context_window_exceeded",
    "max_tokens_exceeded", "token_limit_exceeded",
    // Image input.
    "invalid_image", "invalid_image_format", "invalid_base64_image", "invalid_image_url",
    "invalid_image_mode", "image_parse_error", "image_too_large", "image_too_small",
    "image_file_too_large", "unsupported_image_format", "unsupported_image_media_type",
    "empty_image_file", "image_not_found", "image_file_not_found", "failed_to_download_image",
    "image_download_failed",
    // Content policy.
    "content_policy_violation", "image_content_policy_violation", "refusal", "cyber_policy",
    "misalignment_policy_violation", "bio_policy",
    // Deterministic incomplete reasons.
    "max_output_tokens", "content_filter",
];

/// HTTP statuses that a new attempt may avoid.
fn transient_status(status: u16) -> bool {
    matches!(status, 408 | 409 | 429 | 500..=599)
}

/// A three-digit code is an HTTP status; any other code is terminal only when
/// listed. Unrecognized codes are upstream failures that may not recur.
pub fn is_terminal(code: &str) -> bool {
    if code.len() == 3 && code.bytes().all(|byte| byte.is_ascii_digit()) {
        return code.parse().is_ok_and(|status| !transient_status(status));
    }
    TERMINAL.contains(&code)
}

/// The failure's error object: the top-level `error`, else `response.error`.
fn error_object(event: &Value) -> Option<&Value> {
    let nested = event
        .get("response")
        .and_then(|response| response.get("error"));
    [event.get("error"), nested]
        .into_iter()
        .flatten()
        .find(|error| error.is_object())
}

/// Classification candidates in precedence order: the canonical `error_type`,
/// the error object's `code` (a string or an integer), its `type`, then the
/// incomplete reason. Without an error object, a top-level `code` is used. The
/// event's own `type` names the SSE event and is never a candidate.
pub fn provider_codes(event: &Value) -> Vec<String> {
    let response = event.get("response").unwrap_or(event);
    let error = error_object(event);
    let canonical = response
        .get("error_type")
        .or_else(|| event.get("error_type"));
    let kind = error.and_then(|error| error.get("type"));
    let reason = response
        .get("incomplete_details")
        .and_then(|details| details.get("reason"));
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
    let code = match error.unwrap_or(event).get("code") {
        Some(Value::Number(code)) if code.is_u64() || code.is_i64() => Some(code.to_string()),
        code => text(code),
    };
    [text(canonical), code, text(kind), text(reason)]
        .into_iter()
        .flatten()
        .collect()
}

fn summary(origin: String, provider_code: Option<&str>) -> String {
    origin
        + &provider_code
            .map(|code| format!(" providerCode={code}"))
            .unwrap_or_default()
}

pub struct Failure {
    pub error: ResponseError,
    retryable: bool,
    retry_after: Option<Duration>,
    /// Redacted attempt details for stderr: status, event type, and code.
    summary: String,
}

impl From<ResponseError> for Failure {
    fn from(error: ResponseError) -> Self {
        Self {
            error,
            retryable: false,
            retry_after: None,
            summary: String::new(),
        }
    }
}

impl Failure {
    pub fn request(error: reqwest::Error) -> Self {
        Self {
            retryable: error.is_connect()
                || error.is_timeout()
                || error.is_body()
                || error.is_request(),
            ..Self::connection("gateway connection failed")
        }
    }

    pub fn connection(message: &'static str) -> Self {
        let error = ResponseError::service(SessionError::connection(message));
        Self {
            retryable: true,
            ..error.into()
        }
    }

    pub async fn http(mut response: reqwest::Response) -> Self {
        let status = response.status().as_u16();
        let retry_after = retry_after(response.headers(), SystemTime::now());
        let mut retryable = transient_status(status)
            && response
                .headers()
                .get("x-should-retry")
                .is_none_or(|value| value != "false");
        let mut error = SessionError::http(status);
        let mut body = Vec::new();
        let _ = tokio::time::timeout(ERROR_BODY_TIMEOUT, async {
            while let Ok(Some(chunk)) = response.chunk().await {
                if body.len().saturating_add(chunk.len()) > MAX_ERROR_BYTES {
                    body.clear();
                    break;
                }
                body.extend_from_slice(&chunk);
            }
        })
        .await;
        if let Ok(event) = serde_json::from_slice::<Value>(&body) {
            let classified = SessionError::provider_event(&event);
            if let Some(code) = classified.provider_code {
                retryable &= !is_terminal(&code);
                error.provider_code = Some(code);
                if classified.message != "provider response did not complete" {
                    error.message = classified.message;
                }
            }
        }
        Self {
            summary: summary(format!(" status={status}"), error.provider_code.as_deref()),
            error: ResponseError::service(error.with_provider_source()),
            retryable,
            retry_after,
        }
    }

    pub async fn non_sse(response: reqwest::Response) -> Self {
        let mut failure = Self::http(response).await;
        failure.retryable = false;
        let session = failure
            .error
            .source()
            .and_then(|source| source.downcast_ref::<SessionError>());
        if session.is_none_or(|error| error.provider_code.is_none()) {
            failure.error = ResponseError::service(SessionError::new(
                "transport_error",
                "gateway requires a text/event-stream response",
            ));
        }
        failure
    }

    /// A `response.failed`, `response.incomplete`, or error event retries
    /// unless its reported classification is terminal.
    pub fn event(event: &Value) -> Self {
        let error = SessionError::provider_event(event);
        let kind = event["type"].as_str().unwrap_or_default();
        Self {
            summary: summary(format!(" event={kind}"), error.provider_code.as_deref()),
            retryable: !error.provider_code.as_deref().is_some_and(is_terminal),
            error: ResponseError::service(error.with_provider_source()),
            retry_after: error_object(event)
                .unwrap_or(event)
                .get("retry_after")
                .and_then(Value::as_f64)
                .filter(|seconds| *seconds >= 0.0)
                .map(|seconds| Duration::try_from_secs_f64(seconds).unwrap_or(Duration::MAX)),
        }
    }
}

/// Parses a non-negative decimal count of `unit`; a count too large to
/// represent is `Duration::MAX`.
fn decimal_delay(value: &str, unit: Duration) -> Option<Duration> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let count = value.parse::<u32>().ok();
    Some(
        count
            .and_then(|count| unit.checked_mul(count))
            .unwrap_or(Duration::MAX),
    )
}

fn retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let header = |name| headers.get(name).and_then(|value| value.to_str().ok());
    let milliseconds =
        header("retry-after-ms").and_then(|value| decimal_delay(value, Duration::from_millis(1)));
    milliseconds.or_else(|| {
        let value = header("retry-after")?;
        decimal_delay(value, Duration::from_secs(1)).or_else(|| {
            let date = httpdate::parse_http_date(value).ok()?;
            Some(date.duration_since(now).unwrap_or_default())
        })
    })
}

pub struct Retry {
    attempt: u32,
}

impl Retry {
    pub fn new() -> Self {
        Self { attempt: 1 }
    }

    /// Returns the delay before the next attempt, or why there is none.
    /// Output already delivered to the client is never replayed.
    fn next_delay(
        &self,
        failure: &Failure,
        output_delivered: bool,
    ) -> Result<Duration, &'static str> {
        if output_delivered {
            return Err("output already delivered");
        }
        if !failure.retryable {
            return Err("terminal failure");
        }
        if self.attempt >= MAX_ATTEMPTS {
            return Err("attempt limit reached");
        }
        let server = failure.retry_after.unwrap_or_default();
        if server > MAX_SERVER_DELAY {
            return Err("server retry delay exceeds 60 seconds");
        }
        let base_ms = 200_u64 << (self.attempt - 1);
        let jitter = 75 + RandomState::new().hash_one(self.attempt) % 51;
        Ok(Duration::from_millis(base_ms * jitter / 100).max(server))
    }

    /// Writes one redacted stderr line for the failed attempt and waits when
    /// another attempt follows.
    pub async fn wait(&mut self, failure: &Failure, output_delivered: bool) -> bool {
        let (attempt, summary) = (self.attempt, &failure.summary);
        match self.next_delay(failure, output_delivered) {
            Ok(delay) => {
                eprintln!(
                    "gateway attempt {attempt}/{MAX_ATTEMPTS} failed{summary}; retrying in {} ms",
                    delay.as_millis()
                );
                self.attempt += 1;
                tokio::time::sleep(delay).await;
                true
            }
            Err(reason) => {
                eprintln!(
                    "gateway attempt {attempt}/{MAX_ATTEMPTS} failed{summary}; not retrying: {reason}"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_repair_errors_keep_safe_metadata_and_drop_provider_messages() {
        for code in [
            "context_length_exceeded",
            "context_window_exceeded",
            "invalid_image",
        ] {
            let failure = Failure::event(
                &serde_json::json!({"error":{"code":code,"message":"secret provider body"}}),
            );
            let typed = failure
                .error
                .responses_error()
                .expect("native repair source");
            assert_eq!(typed.is_context_window_exceeded(), code != "invalid_image");
            assert!(!format!("{typed:?}").contains("secret"));
            let safe = failure
                .error
                .source()
                .unwrap()
                .downcast_ref::<SessionError>()
                .unwrap();
            assert_eq!(safe.provider_code.as_deref(), Some(code));
        }
    }

    #[test]
    fn retry_delays_are_bounded_and_server_delays_are_respected() {
        let mut retry = Retry::new();
        let mut failure = Failure::connection("connection failed");
        for attempt in 1..MAX_ATTEMPTS {
            retry.attempt = attempt;
            let base_ms = 200_u128 << (attempt - 1);
            let delay = retry.next_delay(&failure, false).unwrap().as_millis();
            assert!((base_ms * 75 / 100..=base_ms * 125 / 100).contains(&delay));
        }
        retry.attempt = MAX_ATTEMPTS;
        assert!(retry.next_delay(&failure, false).is_err());
        retry.attempt = 1;
        assert!(retry.next_delay(&failure, true).is_err());
        failure.retry_after = Some(Duration::from_secs(60));
        assert_eq!(retry.next_delay(&failure, false).ok(), failure.retry_after);
        failure.retry_after = Some(Duration::from_secs(61));
        assert!(retry.next_delay(&failure, false).is_err());
    }

    #[test]
    fn retry_after_accepts_seconds_milliseconds_and_http_dates() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "2".parse().unwrap());
        assert_eq!(retry_after(&headers, now), Some(Duration::from_secs(2)));
        headers.insert("retry-after-ms", "200".parse().unwrap());
        assert_eq!(retry_after(&headers, now), Some(Duration::from_millis(200)));
        headers.remove("retry-after-ms");
        headers.insert(
            "retry-after",
            httpdate::fmt_http_date(now + Duration::from_secs(2))
                .parse()
                .unwrap(),
        );
        assert_eq!(retry_after(&headers, now), Some(Duration::from_secs(2)));
        headers.insert("retry-after", "99999999999999999999".parse().unwrap());
        assert!(retry_after(&headers, now).unwrap() > MAX_SERVER_DELAY);
        headers.insert("retry-after-ms", "99999999999999999999".parse().unwrap());
        assert!(retry_after(&headers, now).unwrap() > MAX_SERVER_DELAY);
        headers.remove("retry-after-ms");
        let mut failure = Failure::connection("connection failed");
        failure.retry_after = Some(Duration::ZERO);
        assert!(Retry::new().next_delay(&failure, false).unwrap() >= Duration::from_millis(150));
        assert!(
            Retry::new()
                .next_delay(
                    &Failure::event(
                        &serde_json::json!({"error":{"code":"rate_limit_exceeded", "retry_after":1e20}})
                    ),
                    false
                )
                .is_err()
        );
        for value in ["-1", "NaN", "infinity", "invalid", "1e20", "1.5", "5, 10"] {
            headers.insert("retry-after", value.parse().unwrap());
            assert_eq!(retry_after(&headers, now), None);
        }
    }
}
