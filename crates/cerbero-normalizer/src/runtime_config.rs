use std::env;
use std::path::PathBuf;
use std::time::Duration;

use cerbero_common::contracts::v1::ExecutionMode;

use crate::NormalizerError;

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub nats_url: String,
    pub nats_user: String,
    pub nats_password: String,
    pub component_version: String,
    pub instance_id: String,
    pub pipeline_version: String,
    pub execution_mode: ExecutionMode,
    pub raw_store_path: PathBuf,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub retry_min_delay: Duration,
    pub retry_max_delay: Duration,
}

impl RuntimeConfig {
    pub fn from_env() -> Result<Self, NormalizerError> {
        if required("CERBERO_DEV_MODE")? != "1"
            || required("CERBERO_SECURITY_PROFILE")? != "DEVELOPMENT"
        {
            return Err(config_error(
                "cerbero-normalizer runtime is enabled only for the explicit DEVELOPMENT profile",
            ));
        }

        let execution_mode = match required("CERBERO_NORMALIZER_EXECUTION_MODE")?.as_str() {
            "LIVE" => ExecutionMode::Live,
            "REPLAY" => ExecutionMode::Replay,
            "TEST" => ExecutionMode::Test,
            other => {
                return Err(config_error(&format!(
                    "CERBERO_NORMALIZER_EXECUTION_MODE must be LIVE, REPLAY, or TEST; got {other}"
                )));
            }
        };
        let host = required("CLICKHOUSE_HOST")?;
        let port = required("CLICKHOUSE_HTTP_PORT")?;

        Ok(Self {
            nats_url: required("CERBERO_NATS_URL")?,
            nats_user: required("NATS_NORMALIZER_USER")?,
            nats_password: required("NATS_NORMALIZER_PASSWORD")?,
            component_version: required("CERBERO_NORMALIZER_COMPONENT_VERSION")?,
            instance_id: required("CERBERO_NORMALIZER_INSTANCE_ID")?,
            pipeline_version: required("CERBERO_NORMALIZER_PIPELINE_VERSION")?,
            execution_mode,
            raw_store_path: PathBuf::from(required("CERBERO_RAW_STORE_PATH")?),
            clickhouse_url: format!("http://{host}:{port}"),
            clickhouse_database: required("CLICKHOUSE_DB")?,
            clickhouse_user: required("CLICKHOUSE_NORMALIZER_USER")?,
            clickhouse_password: required("CLICKHOUSE_NORMALIZER_PASSWORD")?,
            retry_min_delay: parse_duration_seconds("CERBERO_NORMALIZER_RETRY_MIN_SECONDS")?,
            retry_max_delay: parse_duration_seconds("CERBERO_NORMALIZER_RETRY_MAX_SECONDS")?,
        })
        .and_then(Self::validate)
    }

    pub fn validate(self) -> Result<Self, NormalizerError> {
        for (name, value) in [
            ("CERBERO_NATS_URL", self.nats_url.as_str()),
            ("NATS_NORMALIZER_USER", self.nats_user.as_str()),
            ("NATS_NORMALIZER_PASSWORD", self.nats_password.as_str()),
            (
                "CERBERO_NORMALIZER_COMPONENT_VERSION",
                self.component_version.as_str(),
            ),
            ("CERBERO_NORMALIZER_INSTANCE_ID", self.instance_id.as_str()),
            (
                "CERBERO_NORMALIZER_PIPELINE_VERSION",
                self.pipeline_version.as_str(),
            ),
            ("CLICKHOUSE_URL", self.clickhouse_url.as_str()),
            ("CLICKHOUSE_DB", self.clickhouse_database.as_str()),
            ("CLICKHOUSE_NORMALIZER_USER", self.clickhouse_user.as_str()),
            (
                "CLICKHOUSE_NORMALIZER_PASSWORD",
                self.clickhouse_password.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(config_error(&format!("{name} is required")));
            }
        }
        if self.execution_mode == ExecutionMode::Unspecified {
            return Err(config_error(
                "normalizer execution mode must not be unspecified",
            ));
        }
        if !simple_identifier(&self.clickhouse_database) {
            return Err(config_error(
                "CLICKHOUSE_DB must be a simple ClickHouse identifier",
            ));
        }
        if self.retry_min_delay.is_zero() {
            return Err(config_error(
                "normalizer retry minimum must be greater than zero",
            ));
        }
        if self.retry_max_delay < self.retry_min_delay {
            return Err(config_error(
                "normalizer retry maximum must be greater than or equal to retry minimum",
            ));
        }
        if self.raw_store_path.as_os_str().is_empty() {
            return Err(config_error("CERBERO_RAW_STORE_PATH is required"));
        }
        Ok(self)
    }
}

fn simple_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn required(name: &str) -> Result<String, NormalizerError> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| config_error(&format!("{name} is required")))
}

fn parse_duration_seconds(name: &str) -> Result<Duration, NormalizerError> {
    let value = required(name)?;
    let seconds = value
        .parse::<u64>()
        .map_err(|_| config_error(&format!("{name} must contain an integer number of seconds")))?;
    Ok(Duration::from_secs(seconds))
}

fn config_error(message: &str) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-CONFIG",
        message: message.to_string(),
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_inverted_retry_range() {
        let config = RuntimeConfig {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            nats_user: "u".to_string(),
            nats_password: "p".to_string(),
            component_version: "dev".to_string(),
            instance_id: "normalizer-1".to_string(),
            pipeline_version: "normalizer-v1".to_string(),
            execution_mode: ExecutionMode::Live,
            raw_store_path: PathBuf::from("var/raw"),
            clickhouse_url: "http://127.0.0.1:8123".to_string(),
            clickhouse_database: "cerbero".to_string(),
            clickhouse_user: "normalizer".to_string(),
            clickhouse_password: "secret".to_string(),
            retry_min_delay: Duration::from_secs(5),
            retry_max_delay: Duration::from_secs(1),
        };
        assert_eq!(config.validate().unwrap_err().code, "CER-NORM-CONFIG");
    }
}
