use crate::session::SessionError;
use nanocodex::oai::ResponseError;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::{
    collections::hash_map::DefaultHasher,
    error::Error,
    hash::{Hash, Hasher},
    time::{Duration, SystemTime},
};

const MAX_ATTEMPTS: u32 = 5;
const MAX_SERVER_DELAY: Duration = Duration::from_secs(60);
const MAX_ERROR_BYTES: usize = 64 * 1024;
const ERROR_BODY_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Failure {
    pub error: ResponseError,
    retryable: bool,
    retry_after: Option<Duration>,
}

impl From<ResponseError> for Failure {
    fn from(error: ResponseError) -> Self {
        Self {
            error,
            retryable: false,
            retry_after: None,
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
        Self {
            error: ResponseError::service(SessionError::connection(message)),
            retryable: true,
            retry_after: None,
        }
    }

    pub async fn http(mut response: reqwest::Response) -> Self {
        let status = response.status().as_u16();
        let retry_after = retry_after(response.headers(), SystemTime::now());
        let mut retryable = matches!(status, 408 | 409 | 429 | 500..=599)
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
            retryable &= provider_retry(&event) != Some(false);
            let classified = SessionError::provider_event(&event);
            if classified.provider_code.is_some() {
                error.provider_code = classified.provider_code;
                if classified.message != "provider response did not complete" {
                    error.message = classified.message;
                }
            }
        }
        Self {
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

    pub fn event(event: &Value) -> Self {
        let error = SessionError::provider_event(event);
        let detail = error_detail(event);
        Self {
            error: ResponseError::service(error.with_provider_source()),
            retryable: provider_retry(event) == Some(true),
            retry_after: detail
                .get("retry_after")
                .and_then(Value::as_f64)
                .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
                .map(|seconds| Duration::from_secs_f64(seconds.min(61.0))),
        }
    }
}

fn error_detail(event: &Value) -> &Value {
    event
        .get("error")
        .filter(|value| value.is_object())
        .or_else(|| {
            event
                .get("response")
                .and_then(|response| response.get("error"))
                .filter(|value| value.is_object())
        })
        .unwrap_or(event)
}

fn provider_retry(event: &Value) -> Option<bool> {
    let canonical = event
        .get("response")
        .and_then(|response| response.get("error_type"))
        .or_else(|| event.get("error_type"))
        .and_then(Value::as_str);
    if canonical.is_some_and(terminal_code) {
        return Some(false);
    }
    let detail = error_detail(event);
    let codes = [
        detail.get("code").and_then(Value::as_str),
        detail.get("type").and_then(Value::as_str),
    ];
    if codes.into_iter().flatten().any(terminal_code) {
        return Some(false);
    }
    if canonical.is_some_and(transient_code) || codes.into_iter().flatten().any(transient_code) {
        return Some(true);
    }
    None
}

fn transient_code(code: &str) -> bool {
    matches!(
        code,
        "rate_limit_exceeded"
            | "too_many_requests"
            | "rate_limit_error"
            | "server_error"
            | "internal_server_error"
            | "overloaded_error"
            | "server_overloaded"
            | "server_is_overloaded"
            | "slow_down"
            | "provider_overloaded"
            | "provider_unavailable"
            | "server"
            | "timeout"
    )
}

fn terminal_code(code: &str) -> bool {
    matches!(
        code,
        "insufficient_quota"
            | "quota_exceeded"
            | "insufficient_balance"
            | "usage_not_included"
            | "invalid_api_key"
            | "authentication_error"
            | "unauthorized"
            | "model_not_found"
            | "unsupported_model"
            | "invalid_request_error"
            | "context_length_exceeded"
            | "context_window_exceeded"
            | "invalid_prompt"
            | "authentication"
            | "permission_denied"
            | "payment_required"
            | "max_tokens_exceeded"
            | "token_limit_exceeded"
            | "string_too_long"
            | "invalid_request"
            | "not_found"
            | "precondition_failed"
            | "payload_too_large"
            | "unprocessable"
            | "content_policy_violation"
            | "image_content_policy_violation"
            | "refusal"
            | "invalid_image"
            | "image_too_large"
            | "image_too_small"
            | "unsupported_image_format"
            | "image_not_found"
            | "cyber_policy"
            | "misalignment_policy_violation"
            | "bio_policy"
    )
}

fn decimal_delay(value: &str, milliseconds: bool) -> Option<Duration> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let maximum = if milliseconds { 61_000 } else { 61 };
    let count = value.bytes().fold(0_u64, |count, byte| {
        count
            .saturating_mul(10)
            .saturating_add(u64::from(byte - b'0'))
            .min(maximum)
    });
    Some(if milliseconds {
        Duration::from_millis(count)
    } else {
        Duration::from_secs(count)
    })
}

fn retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    if let Some(delay) = headers
        .get("retry-after-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| decimal_delay(value, true))
    {
        return Some(delay);
    }
    let value = headers.get("retry-after")?.to_str().ok()?;
    decimal_delay(value, false).or_else(|| {
        httpdate::parse_http_date(value)
            .ok()
            .map(|date| date.duration_since(now).unwrap_or_default())
    })
}

pub struct Retry {
    attempt: u32,
    seed: u64,
}

impl Retry {
    pub fn new(session_id: &str, call_index: Option<u32>) -> Self {
        let mut hasher = DefaultHasher::new();
        (session_id, call_index).hash(&mut hasher);
        Self {
            attempt: 1,
            seed: hasher.finish(),
        }
    }

    fn delay(&self, failure: &Failure) -> Option<Duration> {
        if !failure.retryable || self.attempt >= MAX_ATTEMPTS {
            return None;
        }
        if failure
            .retry_after
            .is_some_and(|delay| delay > MAX_SERVER_DELAY)
        {
            return None;
        }
        let base_ms = 200_u64 << self.attempt.saturating_sub(1).min(4);
        let jitter = 75 + self.seed.wrapping_add(u64::from(self.attempt) * 31) % 51;
        Some(
            Duration::from_millis(base_ms * jitter / 100)
                .max(failure.retry_after.unwrap_or_default()),
        )
    }

    pub async fn wait(&mut self, failure: &Failure) -> bool {
        let Some(delay) = self.delay(failure) else {
            if failure
                .retry_after
                .is_some_and(|delay| delay > MAX_SERVER_DELAY)
            {
                eprintln!(
                    "gateway stopped after {} attempt(s): server retry delay exceeds 60 seconds",
                    self.attempt
                );
            } else {
                eprintln!("gateway stopped after {} attempt(s)", self.attempt);
            }
            return false;
        };
        self.attempt += 1;
        eprintln!(
            "gateway retry attempt {}/{} in {} ms",
            self.attempt,
            MAX_ATTEMPTS,
            delay.as_millis()
        );
        tokio::time::sleep(delay).await;
        true
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
        let mut retry = Retry::new("session", Some(1));
        let mut failure = Failure::connection("connection failed");
        for attempt in 1..MAX_ATTEMPTS {
            retry.attempt = attempt;
            let base_ms = 200_u128 << (attempt - 1);
            let delay = retry.delay(&failure).unwrap().as_millis();
            assert!((base_ms * 75 / 100..=base_ms * 125 / 100).contains(&delay));
        }
        retry.attempt = MAX_ATTEMPTS;
        assert!(retry.delay(&failure).is_none());
        retry.attempt = 1;
        failure.retry_after = Some(Duration::from_secs(60));
        assert_eq!(retry.delay(&failure), failure.retry_after);
        failure.retry_after = Some(Duration::from_secs(61));
        assert!(retry.delay(&failure).is_none());
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
        assert!(Retry::new("session", None).delay(&failure).unwrap() >= Duration::from_millis(150));
        assert!(
            Retry::new("session", None)
                .delay(&Failure::event(
                    &serde_json::json!({"error":{"code":"rate_limit_exceeded", "retry_after":1e20}})
                ))
                .is_none()
        );
        for value in ["-1", "NaN", "infinity", "invalid", "1e20", "1.5", "5, 10"] {
            headers.insert("retry-after", value.parse().unwrap());
            assert_eq!(retry_after(&headers, now), None);
        }
    }
}
