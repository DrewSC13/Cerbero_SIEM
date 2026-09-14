#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use uuid::Uuid;

use crate::NormalizerError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryFailure {
    pub consumer_name: String,
    pub message_key: String,
    pub message_id: Option<Uuid>,
    pub occurred_at: SystemTime,
    pub retry_budget: u32,
    pub delivery_attempt: i64,
    pub error_code: String,
    pub error_message: String,
    pub error_retryable: bool,
    pub next_retry_at: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryState {
    pub consumer_name: String,
    pub message_key: String,
    pub message_id: Option<Uuid>,
    pub first_failure_at: SystemTime,
    pub last_failure_at: SystemTime,
    pub failure_count: u32,
    pub retry_budget: u32,
    pub last_delivery_attempt: i64,
    pub last_error_code: String,
    pub last_error_message: String,
    pub last_error_retryable: bool,
    pub next_retry_at: Option<SystemTime>,
}

impl RetryState {
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.failure_count >= self.retry_budget
    }

    #[must_use]
    pub fn isolation_required(&self) -> bool {
        self.exhausted() || !self.last_error_retryable
    }
}

#[async_trait]
pub trait RetryStateStore: Send + Sync {
    async fn load(
        &self,
        consumer_name: &str,
        message_key: &str,
    ) -> Result<Option<RetryState>, NormalizerError>;

    async fn record_failure(&self, failure: RetryFailure) -> Result<RetryState, NormalizerError>;

    async fn clear(&self, consumer_name: &str, message_key: &str) -> Result<(), NormalizerError>;
}

#[cfg(test)]
fn advance_retry_state(
    existing: Option<&RetryState>,
    failure: &RetryFailure,
) -> Result<RetryState, NormalizerError> {
    validate_failure(failure)?;

    if let Some(existing) = existing {
        validate_state(existing)?;
        if existing.consumer_name != failure.consumer_name
            || existing.message_key != failure.message_key
        {
            return Err(retry_state_error(
                "retry failure key differs from existing lifecycle",
            ));
        }
        if existing.isolation_required() {
            return Err(retry_state_error(
                "cannot record another processing failure after isolation became required",
            ));
        }
        if let (Some(existing_id), Some(failure_id)) = (existing.message_id, failure.message_id)
            && existing_id != failure_id
        {
            return Err(retry_state_error(
                "retry message_id changed within one delivery lifecycle",
            ));
        }

        let failure_count = existing.failure_count.saturating_add(1);
        if failure_count > existing.retry_budget {
            return Err(retry_state_error(
                "cannot record another processing failure after retry budget exhaustion",
            ));
        }
        let last_failure_at = later(existing.last_failure_at, failure.occurred_at);
        let isolation_required = !failure.error_retryable || failure_count >= existing.retry_budget;
        let next_retry_at = if isolation_required {
            None
        } else {
            Some(later(last_failure_at, failure.next_retry_at))
        };

        let state = RetryState {
            consumer_name: existing.consumer_name.clone(),
            message_key: existing.message_key.clone(),
            message_id: existing.message_id.or(failure.message_id),
            first_failure_at: existing.first_failure_at,
            last_failure_at,
            failure_count,
            retry_budget: existing.retry_budget,
            last_delivery_attempt: failure.delivery_attempt,
            last_error_code: failure.error_code.clone(),
            last_error_message: failure.error_message.clone(),
            last_error_retryable: failure.error_retryable,
            next_retry_at,
        };
        validate_state(&state)?;
        return Ok(state);
    }

    let state = RetryState {
        consumer_name: failure.consumer_name.clone(),
        message_key: failure.message_key.clone(),
        message_id: failure.message_id,
        first_failure_at: failure.occurred_at,
        last_failure_at: failure.occurred_at,
        failure_count: 1,
        retry_budget: failure.retry_budget,
        last_delivery_attempt: failure.delivery_attempt,
        last_error_code: failure.error_code.clone(),
        last_error_message: failure.error_message.clone(),
        last_error_retryable: failure.error_retryable,
        next_retry_at: (failure.error_retryable && failure.retry_budget > 1)
            .then_some(failure.next_retry_at),
    };
    validate_state(&state)?;
    Ok(state)
}

pub(crate) fn validate_failure(failure: &RetryFailure) -> Result<(), NormalizerError> {
    validate_key(&failure.consumer_name, &failure.message_key)?;
    if failure
        .message_id
        .is_some_and(|message_id| message_id.is_nil())
    {
        return Err(retry_state_error(
            "retry message_id must not be the nil UUID when present",
        ));
    }
    if failure.retry_budget == 0 {
        return Err(retry_state_error("retry budget must be greater than zero"));
    }
    if failure.delivery_attempt < 1 {
        return Err(retry_state_error(
            "delivery attempt must be greater than zero",
        ));
    }
    if failure.error_code.trim().is_empty() {
        return Err(retry_state_error("retry error code is required"));
    }
    if failure.next_retry_at < failure.occurred_at {
        return Err(retry_state_error(
            "next retry time must not precede failure time",
        ));
    }
    Ok(())
}

pub(crate) fn validate_state(state: &RetryState) -> Result<(), NormalizerError> {
    validate_key(&state.consumer_name, &state.message_key)?;
    if state
        .message_id
        .is_some_and(|message_id| message_id.is_nil())
    {
        return Err(retry_state_error(
            "stored retry message_id must not be the nil UUID when present",
        ));
    }
    if state.failure_count == 0 {
        return Err(retry_state_error(
            "retry failure count must be greater than zero",
        ));
    }
    if state.retry_budget == 0 {
        return Err(retry_state_error("retry budget must be greater than zero"));
    }
    if state.failure_count > state.retry_budget {
        return Err(retry_state_error(
            "retry failure count exceeds captured retry budget",
        ));
    }
    if state.last_delivery_attempt < 1 {
        return Err(retry_state_error(
            "delivery attempt must be greater than zero",
        ));
    }
    if state.last_error_code.trim().is_empty() {
        return Err(retry_state_error("retry error code is required"));
    }
    if state.last_failure_at < state.first_failure_at {
        return Err(retry_state_error(
            "last failure time must not precede first failure time",
        ));
    }

    match (state.isolation_required(), state.next_retry_at) {
        (true, Some(_)) => Err(retry_state_error(
            "isolating retry state must not have a next retry time",
        )),
        (false, None) => Err(retry_state_error(
            "active retry state requires a next retry time",
        )),
        (false, Some(next_retry_at)) if next_retry_at < state.last_failure_at => Err(
            retry_state_error("next retry time must not precede last failure time"),
        ),
        _ => Ok(()),
    }
}

pub(crate) fn validate_key(consumer_name: &str, message_key: &str) -> Result<(), NormalizerError> {
    if consumer_name.trim().is_empty() {
        return Err(retry_state_error("retry consumer name is required"));
    }
    if message_key.trim().is_empty() {
        return Err(retry_state_error("retry message key is required"));
    }
    if message_key.len() > 256 {
        return Err(retry_state_error("retry message key exceeds 256 bytes"));
    }
    Ok(())
}

#[cfg(test)]
fn later(left: SystemTime, right: SystemTime) -> SystemTime {
    if left >= right { left } else { right }
}

pub(crate) fn retry_state_error(message: impl Into<String>) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-RETRY-STATE",
        message: message.into(),
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[derive(Default)]
    struct MemoryRetryStateStore {
        states: Mutex<BTreeMap<(String, String), RetryState>>,
    }

    #[async_trait]
    impl RetryStateStore for MemoryRetryStateStore {
        async fn load(
            &self,
            consumer_name: &str,
            message_key: &str,
        ) -> Result<Option<RetryState>, NormalizerError> {
            validate_key(consumer_name, message_key)?;
            Ok(self
                .states
                .lock()
                .expect("retry-state mutex")
                .get(&(consumer_name.to_string(), message_key.to_string()))
                .cloned())
        }

        async fn record_failure(
            &self,
            failure: RetryFailure,
        ) -> Result<RetryState, NormalizerError> {
            let key = (failure.consumer_name.clone(), failure.message_key.clone());
            let mut states = self.states.lock().expect("retry-state mutex");
            let state = advance_retry_state(states.get(&key), &failure)?;
            states.insert(key, state.clone());
            Ok(state)
        }

        async fn clear(
            &self,
            consumer_name: &str,
            message_key: &str,
        ) -> Result<(), NormalizerError> {
            validate_key(consumer_name, message_key)?;
            self.states
                .lock()
                .expect("retry-state mutex")
                .remove(&(consumer_name.to_string(), message_key.to_string()));
            Ok(())
        }
    }

    fn failure(
        message_key: &str,
        message_id: Option<Uuid>,
        occurred_at: SystemTime,
        retry_budget: u32,
        delivery_attempt: i64,
        error_retryable: bool,
    ) -> RetryFailure {
        RetryFailure {
            consumer_name: "normalizer".to_string(),
            message_key: message_key.to_string(),
            message_id,
            occurred_at,
            retry_budget,
            delivery_attempt,
            error_code: "CER-NORM-TEST".to_string(),
            error_message: "integration failure".to_string(),
            error_retryable,
            next_retry_at: occurred_at + Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn preserves_first_failure_and_captured_budget() {
        let store = MemoryRetryStateStore::default();
        let message_id = Uuid::now_v7();
        let first_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let second_at = first_at + Duration::from_secs(5);

        let first = store
            .record_failure(failure("stream:41", Some(message_id), first_at, 3, 1, true))
            .await
            .unwrap();
        let second = store
            .record_failure(failure(
                "stream:41",
                Some(message_id),
                second_at,
                9,
                2,
                true,
            ))
            .await
            .unwrap();

        assert_eq!(first.failure_count, 1);
        assert_eq!(second.failure_count, 2);
        assert_eq!(second.retry_budget, 3);
        assert_eq!(second.first_failure_at, first_at);
        assert_eq!(second.last_failure_at, second_at);
        assert_eq!(second.message_id, Some(message_id));
        assert!(!second.isolation_required());
    }

    #[tokio::test]
    async fn budget_exhaustion_removes_next_retry() {
        let store = MemoryRetryStateStore::default();
        let first_at = SystemTime::UNIX_EPOCH + Duration::from_secs(20);

        store
            .record_failure(failure("stream:42", None, first_at, 2, 1, true))
            .await
            .unwrap();
        let exhausted = store
            .record_failure(failure(
                "stream:42",
                None,
                first_at + Duration::from_secs(2),
                99,
                2,
                true,
            ))
            .await
            .unwrap();

        assert_eq!(exhausted.failure_count, 2);
        assert_eq!(exhausted.retry_budget, 2);
        assert!(exhausted.exhausted());
        assert!(exhausted.isolation_required());
        assert_eq!(exhausted.next_retry_at, None);
    }

    #[tokio::test]
    async fn permanent_failure_requires_isolation_without_faking_attempt_count() {
        let store = MemoryRetryStateStore::default();
        let failed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);

        let state = store
            .record_failure(failure("payload:abc", None, failed_at, 5, 1, false))
            .await
            .unwrap();

        assert_eq!(state.failure_count, 1);
        assert_eq!(state.retry_budget, 5);
        assert!(!state.exhausted());
        assert!(state.isolation_required());
        assert_eq!(state.next_retry_at, None);
    }

    #[tokio::test]
    async fn clear_removes_completed_lifecycle() {
        let store = MemoryRetryStateStore::default();
        let failed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(40);

        store
            .record_failure(failure("stream:43", None, failed_at, 3, 1, true))
            .await
            .unwrap();
        store.clear("normalizer", "stream:43").await.unwrap();

        assert_eq!(store.load("normalizer", "stream:43").await.unwrap(), None);
    }
}
