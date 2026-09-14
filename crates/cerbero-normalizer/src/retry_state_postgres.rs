use std::sync::Arc;

use async_trait::async_trait;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::NormalizerError;
use crate::retry_state::{
    RetryFailure, RetryState, RetryStateStore, retry_state_error, validate_failure, validate_key,
    validate_state,
};

const RETRY_STATE_SELECT: &str = r"
SELECT
    consumer_name,
    message_key,
    message_id::text AS message_id,
    first_failure_at,
    last_failure_at,
    failure_count,
    retry_budget,
    last_delivery_attempt,
    last_error_code,
    last_error_message,
    last_error_retryable,
    next_retry_at
FROM system.normalizer_retry_state
WHERE consumer_name = $1
  AND message_key = $2
";

const RETRY_STATE_UPSERT: &str = r"
INSERT INTO system.normalizer_retry_state AS state (
    consumer_name,
    message_key,
    message_id,
    first_failure_at,
    last_failure_at,
    failure_count,
    retry_budget,
    last_delivery_attempt,
    last_error_code,
    last_error_retryable,
    last_error_message,
    next_retry_at
) VALUES (
    $1,
    $2,
    CAST($3 AS text)::uuid,
    $4,
    $4,
    1,
    $5,
    $6,
    $7,
    $8,
    $9,
    CASE
        WHEN NOT $8::boolean OR $5::bigint <= 1 THEN NULL
        ELSE $10::timestamptz
    END
)
ON CONFLICT (consumer_name, message_key) DO UPDATE
SET
    message_id = COALESCE(state.message_id, EXCLUDED.message_id),
    last_failure_at = GREATEST(state.last_failure_at, EXCLUDED.last_failure_at),
    failure_count = state.failure_count + 1,
    last_delivery_attempt = EXCLUDED.last_delivery_attempt,
    last_error_code = EXCLUDED.last_error_code,
    last_error_retryable = EXCLUDED.last_error_retryable,
    last_error_message = EXCLUDED.last_error_message,
    next_retry_at = CASE
        WHEN NOT EXCLUDED.last_error_retryable
            OR state.failure_count + 1 >= state.retry_budget
        THEN NULL
        ELSE GREATEST(
            state.last_failure_at,
            EXCLUDED.last_failure_at,
            $10::timestamptz
        )
    END,
    updated_at = now()
WHERE state.failure_count < state.retry_budget
  AND state.last_error_retryable
RETURNING
    consumer_name,
    message_key,
    message_id::text AS message_id,
    first_failure_at,
    last_failure_at,
    failure_count,
    retry_budget,
    last_delivery_attempt,
    last_error_code,
    last_error_message,
    last_error_retryable,
    next_retry_at
";

#[derive(Clone)]
pub struct PostgresRetryStateStore {
    client: Arc<Client>,
}

impl PostgresRetryStateStore {
    #[must_use]
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RetryStateStore for PostgresRetryStateStore {
    async fn load(
        &self,
        consumer_name: &str,
        message_key: &str,
    ) -> Result<Option<RetryState>, NormalizerError> {
        validate_key(consumer_name, message_key)?;
        let row = self
            .client
            .query_opt(RETRY_STATE_SELECT, &[&consumer_name, &message_key])
            .await
            .map_err(|error| postgres_error("load retry state", &error))?;
        row.as_ref().map(row_to_state).transpose()
    }

    async fn record_failure(&self, failure: RetryFailure) -> Result<RetryState, NormalizerError> {
        validate_failure(&failure)?;

        let message_id_text = failure.message_id.map(|message_id| message_id.to_string());
        let retry_budget = i64::from(failure.retry_budget);
        let row = self
            .client
            .query_opt(
                RETRY_STATE_UPSERT,
                &[
                    &failure.consumer_name,
                    &failure.message_key,
                    &message_id_text,
                    &failure.occurred_at,
                    &retry_budget,
                    &failure.delivery_attempt,
                    &failure.error_code,
                    &failure.error_retryable,
                    &failure.error_message,
                    &failure.next_retry_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("record retry failure", &error))?;

        if let Some(row) = row {
            return row_to_state(&row);
        }

        match self
            .load(&failure.consumer_name, &failure.message_key)
            .await?
        {
            Some(state) if state.isolation_required() => Err(retry_state_error(
                "cannot record another processing failure after isolation became required",
            )),
            Some(_) => Err(postgres_state_error(
                "retry-state upsert returned no row for an active lifecycle",
            )),
            None => Err(postgres_state_error(
                "retry-state upsert returned no row and no durable state exists",
            )),
        }
    }

    async fn clear(&self, consumer_name: &str, message_key: &str) -> Result<(), NormalizerError> {
        validate_key(consumer_name, message_key)?;
        self.client
            .execute(
                r"
DELETE FROM system.normalizer_retry_state
WHERE consumer_name = $1
  AND message_key = $2
",
                &[&consumer_name, &message_key],
            )
            .await
            .map_err(|error| postgres_error("clear retry state", &error))?;
        Ok(())
    }
}

fn row_to_state(row: &Row) -> Result<RetryState, NormalizerError> {
    let message_id_text: Option<String> = row
        .try_get("message_id")
        .map_err(|error| postgres_error("decode retry message ID", &error))?;
    let message_id = message_id_text
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|error| {
            retry_state_error(format!("stored retry message ID is invalid: {error}"))
        })?;

    let failure_count = u32_from_bigint(
        row.try_get("failure_count")
            .map_err(|error| postgres_error("decode retry failure count", &error))?,
        "failure_count",
    )?;
    let retry_budget = u32_from_bigint(
        row.try_get("retry_budget")
            .map_err(|error| postgres_error("decode retry budget", &error))?,
        "retry_budget",
    )?;

    let state = RetryState {
        consumer_name: row
            .try_get("consumer_name")
            .map_err(|error| postgres_error("decode retry consumer", &error))?,
        message_key: row
            .try_get("message_key")
            .map_err(|error| postgres_error("decode retry message key", &error))?,
        message_id,
        first_failure_at: row
            .try_get("first_failure_at")
            .map_err(|error| postgres_error("decode first failure time", &error))?,
        last_failure_at: row
            .try_get("last_failure_at")
            .map_err(|error| postgres_error("decode last failure time", &error))?,
        failure_count,
        retry_budget,
        last_delivery_attempt: row
            .try_get("last_delivery_attempt")
            .map_err(|error| postgres_error("decode delivery attempt", &error))?,
        last_error_code: row
            .try_get("last_error_code")
            .map_err(|error| postgres_error("decode retry error code", &error))?,
        last_error_message: row
            .try_get("last_error_message")
            .map_err(|error| postgres_error("decode retry error message", &error))?,
        last_error_retryable: row
            .try_get("last_error_retryable")
            .map_err(|error| postgres_error("decode retryable flag", &error))?,
        next_retry_at: row
            .try_get("next_retry_at")
            .map_err(|error| postgres_error("decode next retry time", &error))?,
    };
    validate_state(&state)?;
    Ok(state)
}

fn u32_from_bigint(value: i64, field: &str) -> Result<u32, NormalizerError> {
    u32::try_from(value)
        .map_err(|_| retry_state_error(format!("stored {field} is outside the u32 range")))
}

fn postgres_error(context: &str, error: &tokio_postgres::Error) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-RETRY-STATE-POSTGRES",
        message: format!("{context}: {error}"),
        retryable: true,
    }
}

fn postgres_state_error(message: impl Into<String>) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-RETRY-STATE-POSTGRES",
        message: message.into(),
        retryable: true,
    }
}
#[cfg(test)]
mod development_postgres_tests {
    use std::env;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tokio_postgres::NoTls;
    use uuid::Uuid;

    use super::PostgresRetryStateStore;
    use crate::retry_state::{RetryFailure, RetryStateStore};

    async fn connect_store() -> (
        PostgresRetryStateStore,
        JoinHandle<Result<(), tokio_postgres::Error>>,
    ) {
        let mut config = tokio_postgres::Config::new();
        config
            .host(env_required("POSTGRES_HOST"))
            .port(
                env_required("POSTGRES_PORT")
                    .parse()
                    .expect("POSTGRES_PORT must be a u16"),
            )
            .dbname(env_required("POSTGRES_DB"))
            .user(env_required("POSTGRES_NORMALIZER_USER"))
            .password(env_required("POSTGRES_NORMALIZER_PASSWORD"));

        let (client, connection) = config
            .connect(NoTls)
            .await
            .expect("connect development PostgreSQL retry-state store");
        let driver = tokio::spawn(connection);
        (PostgresRetryStateStore::new(Arc::new(client)), driver)
    }

    async fn close_store(
        store: PostgresRetryStateStore,
        driver: JoinHandle<Result<(), tokio_postgres::Error>>,
    ) {
        drop(store);
        let connection_result = timeout(Duration::from_secs(2), driver)
            .await
            .expect("PostgreSQL connection driver did not close")
            .expect("PostgreSQL connection driver task panicked");
        connection_result.expect("PostgreSQL connection driver failed");
    }

    fn env_required(name: &str) -> String {
        env::var(name)
            .unwrap_or_else(|_| panic!("{name} is required for development PostgreSQL test"))
    }

    fn failure(
        consumer_name: &str,
        message_key: &str,
        message_id: Option<Uuid>,
        occurred_at: SystemTime,
        retry_budget: u32,
        delivery_attempt: i64,
        retryable: bool,
    ) -> RetryFailure {
        RetryFailure {
            consumer_name: consumer_name.to_string(),
            message_key: message_key.to_string(),
            message_id,
            occurred_at,
            retry_budget,
            delivery_attempt,
            error_code: "CER-NORM-INTEGRATION-RETRY".to_string(),
            error_message: "development retry-state integration failure".to_string(),
            error_retryable: retryable,
            next_retry_at: occurred_at + Duration::from_secs(1),
        }
    }

    async fn establish_lifecycle(
        consumer: &str,
        primary_key: &str,
        message_id: Uuid,
    ) -> SystemTime {
        let (store, driver) = connect_store().await;
        let first = store
            .record_failure(failure(
                consumer,
                primary_key,
                Some(message_id),
                SystemTime::now(),
                3,
                1,
                true,
            ))
            .await
            .expect("record first retry failure");
        assert_eq!(first.failure_count, 1);
        assert_eq!(first.retry_budget, 3);
        assert!(!first.isolation_required());
        let persisted_first_at = first.first_failure_at;
        close_store(store, driver).await;
        persisted_first_at
    }

    async fn advance_to_exhaustion(
        consumer: &str,
        primary_key: &str,
        sibling_key: &str,
        message_id: Uuid,
        persisted_first_at: SystemTime,
    ) {
        let (store, driver) = connect_store().await;
        let after_restart = store
            .load(consumer, primary_key)
            .await
            .expect("load retry state after reconnect")
            .expect("retry state must survive reconnect");
        assert_eq!(after_restart.first_failure_at, persisted_first_at);
        assert_eq!(after_restart.retry_budget, 3);

        let second = store
            .record_failure(failure(
                consumer,
                primary_key,
                Some(message_id),
                SystemTime::now(),
                99,
                2,
                true,
            ))
            .await
            .expect("record second retry failure");
        assert_eq!(second.failure_count, 2);
        assert_eq!(second.retry_budget, 3);
        assert_eq!(second.first_failure_at, persisted_first_at);

        let exhausted = store
            .record_failure(failure(
                consumer,
                primary_key,
                Some(message_id),
                SystemTime::now(),
                99,
                3,
                true,
            ))
            .await
            .expect("record budget-exhausting retry failure");
        assert_eq!(exhausted.failure_count, 3);
        assert!(exhausted.exhausted());
        assert!(exhausted.isolation_required());
        assert_eq!(exhausted.next_retry_at, None);

        let sibling = store
            .record_failure(failure(
                consumer,
                sibling_key,
                Some(message_id),
                SystemTime::now(),
                2,
                1,
                true,
            ))
            .await
            .expect("record distinct delivery with reused message_id");
        assert_eq!(sibling.failure_count, 1);
        assert_eq!(sibling.message_id, Some(message_id));

        close_store(store, driver).await;
    }

    async fn verify_and_clear(
        consumer: &str,
        primary_key: &str,
        sibling_key: &str,
        persisted_first_at: SystemTime,
    ) {
        let (store, driver) = connect_store().await;
        let durable_exhausted = store
            .load(consumer, primary_key)
            .await
            .expect("load exhausted retry state")
            .expect("exhausted state must remain durable");
        assert_eq!(durable_exhausted.failure_count, 3);
        assert_eq!(durable_exhausted.retry_budget, 3);
        assert_eq!(durable_exhausted.first_failure_at, persisted_first_at);
        assert!(durable_exhausted.isolation_required());

        let durable_sibling = store
            .load(consumer, sibling_key)
            .await
            .expect("load sibling retry state")
            .expect("sibling delivery state must exist independently");
        assert_eq!(durable_sibling.failure_count, 1);
        assert!(!durable_sibling.isolation_required());

        store
            .clear(consumer, primary_key)
            .await
            .expect("clear exhausted retry lifecycle");
        store
            .clear(consumer, sibling_key)
            .await
            .expect("clear sibling retry lifecycle");
        assert_eq!(
            store
                .load(consumer, primary_key)
                .await
                .expect("verify primary clear"),
            None
        );
        assert_eq!(
            store
                .load(consumer, sibling_key)
                .await
                .expect("verify sibling clear"),
            None
        );

        close_store(store, driver).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires development PostgreSQL and migration 000003"]
    async fn retry_state_survives_reconnect_and_preserves_delivery_identity() {
        let consumer = format!("normalizer-retry-it-{}", Uuid::now_v7());
        let message_id = Uuid::now_v7();
        let primary_key = format!("stream:{}", Uuid::now_v7());
        let sibling_key = format!("stream:{}", Uuid::now_v7());

        let persisted_first_at = establish_lifecycle(&consumer, &primary_key, message_id).await;
        advance_to_exhaustion(
            &consumer,
            &primary_key,
            &sibling_key,
            message_id,
            persisted_first_at,
        )
        .await;
        verify_and_clear(&consumer, &primary_key, &sibling_key, persisted_first_at).await;
    }
}
