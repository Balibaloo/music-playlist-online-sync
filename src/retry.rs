use anyhow::Result;
use std::future::Future;
use std::time::Duration;

/// Outcome of a single attempt inside `retry_with_backoff`.
pub enum RetryAction {
    /// The operation succeeded with a value.
    Ok,
    /// A permanent failure – stop retrying immediately.
    Fail(anyhow::Error),
    /// A transient / rate-limited failure – keep retrying.
    /// An optional `retry_after` hint (seconds) can be provided; when `None`
    /// the helper falls back to exponential backoff.
    Retry {
        error: anyhow::Error,
        retry_after: Option<u64>,
    },
}

/// Configuration for `retry_with_backoff`.
pub struct RetryConfig {
    /// Maximum number of attempts (including the first one).
    pub max_attempts: u32,
    /// Cap on the exponential back-off sleep (seconds).
    pub max_backoff_secs: u64,
    /// Label used in log messages so the caller can distinguish different
    /// retry sites in the output.
    pub label: String,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            max_backoff_secs: 60,
            label: String::new(),
        }
    }
}

/// Parse a `retry_after=Some(5)` or `retry_after=None` token from an error
/// string, as emitted by the Spotify / Tidal provider wrappers.
pub fn parse_retry_after(error_string: &str) -> Option<u64> {
    error_string
        .split("retry_after=")
        .nth(1)
        .and_then(|rest| {
            let token = rest.trim();
            if token.starts_with("Some(") {
                token.trim_start_matches("Some(").split(')').next()
            } else if token.starts_with("None") {
                None
            } else {
                // take leading digits
                Some(
                    token
                        .split(|c: char| !c.is_ascii_digit())
                        .next()
                        .unwrap_or("")
                        .trim(),
                )
            }
        })
        .and_then(|s| s.parse::<u64>().ok())
}

/// Determine whether an error string looks like a rate-limit response.
pub fn is_rate_limited(error_string: &str) -> bool {
    error_string.contains("rate_limited")
        || error_string.contains("429")
        || parse_retry_after(error_string).is_some()
}

/// Compute the exponential back-off duration for a given `attempt`
/// (1-based), capped at `max_secs`.
pub fn backoff_duration(attempt: u32, max_secs: u64) -> Duration {
    let exp = 2u64.saturating_pow(attempt.min(6));
    Duration::from_secs(exp.min(max_secs))
}

/// Generic retry helper with exponential back-off and rate-limit awareness.
///
/// `op` is an async closure that runs the operation and returns a
/// `RetryAction` indicating whether to retry.  When `op` returns
/// `RetryAction::Ok`, `retry_with_backoff` returns `Ok(())`.
///
/// On permanent failure (`Fail`) or exhausted retries the last error is
/// returned.
pub async fn retry_with_backoff<F, Fut>(cfg: &RetryConfig, mut op: F) -> Result<()>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = RetryAction>,
{
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=cfg.max_attempts {
        match op(attempt).await {
            RetryAction::Ok => return Ok(()),
            RetryAction::Fail(e) => {
                log::error!(
                    "[retry:{}] permanent failure on attempt {}: {}",
                    cfg.label,
                    attempt,
                    e
                );
                return Err(e);
            }
            RetryAction::Retry { error, retry_after } => {
                if attempt >= cfg.max_attempts {
                    log::error!(
                        "[retry:{}] giving up after {} attempts: {}",
                        cfg.label,
                        attempt,
                        error
                    );
                    return Err(error);
                }
                let wait = if let Some(secs) = retry_after {
                    Duration::from_secs(secs + 1)
                } else {
                    backoff_duration(attempt, cfg.max_backoff_secs)
                };
                log::warn!(
                    "[retry:{}] attempt {} failed, retrying in {:?}: {}",
                    cfg.label,
                    attempt,
                    wait,
                    error
                );
                last_error = Some(error);
                tokio::time::sleep(wait).await;
            }
        }
    }

    // Should not be reached, but just in case:
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("[retry:{}] no attempts made", cfg.label)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn immediate_success() {
        let cfg = RetryConfig {
            max_attempts: 3,
            max_backoff_secs: 1,
            label: "test".into(),
        };
        let counter = AtomicU32::new(0);
        let result = retry_with_backoff(&cfg, |_attempt| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { RetryAction::Ok }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn permanent_failure_stops_immediately() {
        let cfg = RetryConfig {
            max_attempts: 5,
            max_backoff_secs: 1,
            label: "test".into(),
        };
        let counter = AtomicU32::new(0);
        let result = retry_with_backoff(&cfg, |_attempt| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { RetryAction::Fail(anyhow::anyhow!("permanent")) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let cfg = RetryConfig {
            max_attempts: 4,
            max_backoff_secs: 1,
            label: "test".into(),
        };
        let counter = AtomicU32::new(0);
        let result = retry_with_backoff(&cfg, |attempt| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 3 {
                    RetryAction::Retry {
                        error: anyhow::anyhow!("transient"),
                        retry_after: Some(0),
                    }
                } else {
                    RetryAction::Ok
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausts_retries() {
        let cfg = RetryConfig {
            max_attempts: 2,
            max_backoff_secs: 1,
            label: "test".into(),
        };
        let counter = AtomicU32::new(0);
        let result = retry_with_backoff(&cfg, |_attempt| {
            counter.fetch_add(1, Ordering::SeqCst);
            async {
                RetryAction::Retry {
                    error: anyhow::anyhow!("still failing"),
                    retry_after: Some(0),
                }
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn parse_retry_after_some() {
        assert_eq!(
            parse_retry_after("rate_limited retry_after=Some(5)"),
            Some(5)
        );
    }

    #[test]
    fn parse_retry_after_none() {
        assert_eq!(parse_retry_after("rate_limited retry_after=None"), None);
    }

    #[test]
    fn parse_retry_after_bare() {
        assert_eq!(parse_retry_after("retry_after=10 rest"), Some(10));
    }

    #[test]
    fn parse_retry_after_absent() {
        assert_eq!(parse_retry_after("some other error"), None);
    }

    #[test]
    fn is_rate_limited_detects_429() {
        assert!(is_rate_limited("HTTP 429 Too Many Requests"));
    }

    #[test]
    fn is_rate_limited_detects_keyword() {
        assert!(is_rate_limited("rate_limited"));
    }

    #[test]
    fn is_rate_limited_negative() {
        assert!(!is_rate_limited("some other error 200 OK"));
    }

    #[test]
    fn backoff_duration_capped() {
        let d = backoff_duration(10, 60);
        assert_eq!(d, Duration::from_secs(60));
    }

    #[test]
    fn backoff_duration_grows() {
        let d1 = backoff_duration(1, 120);
        let d2 = backoff_duration(2, 120);
        assert!(d2 > d1);
    }
}
