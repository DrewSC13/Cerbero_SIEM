use std::env;
use std::path::PathBuf;
use std::time::Duration;

use cerbero_common::contracts::v1::ExecutionMode;

use crate::{NormalizerError, SourceTimePolicyRegistry};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayInput {
    Historical,
    Selective,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub nats_url: String,
    pub nats_user: String,
    pub nats_password: String,
    pub component_version: String,
    pub instance_id: String,
    pub pipeline_version: String,
    pub execution_mode: ExecutionMode,
    pub replay_input: ReplayInput,
    pub linux_sshd_parser_version: String,
    pub source_time_policies: SourceTimePolicyRegistry,
    pub raw_store_path: PathBuf,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_database: String,
    pub postgres_user: String,
    pub postgres_password: String,
    pub retry_min_delay: Duration,
    pub retry_max_delay: Duration,
    pub retry_budget: u32,
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
        let replay_input = match env::var("CERBERO_NORMALIZER_REPLAY_INPUT")
            .unwrap_or_else(|_| "HISTORICAL".to_string())
            .trim()
        {
            "HISTORICAL" => ReplayInput::Historical,
            "SELECTIVE" => ReplayInput::Selective,
            other => {
                return Err(config_error(&format!(
                    "CERBERO_NORMALIZER_REPLAY_INPUT must be HISTORICAL or SELECTIVE; got {other}"
                )));
            }
        };
        if execution_mode != ExecutionMode::Replay && replay_input == ReplayInput::Selective {
            return Err(config_error(
                "CERBERO_NORMALIZER_REPLAY_INPUT=SELECTIVE requires CERBERO_NORMALIZER_EXECUTION_MODE=REPLAY",
            ));
        }
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
            replay_input,
            linux_sshd_parser_version: required("CERBERO_NORMALIZER_LINUX_SSHD_PARSER_VERSION")?,
            source_time_policies: SourceTimePolicyRegistry::parse_spec(
                &env::var("CERBERO_NORMALIZER_SOURCE_TIME_OFFSETS").unwrap_or_default(),
            )
            .map_err(|error| config_error(&error.message))?,
            raw_store_path: PathBuf::from(required("CERBERO_RAW_STORE_PATH")?),
            clickhouse_url: format!("http://{host}:{port}"),
            clickhouse_database: required("CLICKHOUSE_DB")?,
            clickhouse_user: required("CLICKHOUSE_NORMALIZER_USER")?,
            clickhouse_password: required("CLICKHOUSE_NORMALIZER_PASSWORD")?,
            postgres_host: required("POSTGRES_HOST")?,
            postgres_port: parse_u16("POSTGRES_PORT")?,
            postgres_database: required("POSTGRES_DB")?,
            postgres_user: required("POSTGRES_NORMALIZER_USER")?,
            postgres_password: required("POSTGRES_NORMALIZER_PASSWORD")?,
            retry_min_delay: parse_duration_seconds("CERBERO_NORMALIZER_RETRY_MIN_SECONDS")?,
            retry_max_delay: parse_duration_seconds("CERBERO_NORMALIZER_RETRY_MAX_SECONDS")?,
            retry_budget: parse_u32("CERBERO_NORMALIZER_RETRY_BUDGET")?,
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
            ("POSTGRES_HOST", self.postgres_host.as_str()),
            ("POSTGRES_DB", self.postgres_database.as_str()),
            ("POSTGRES_NORMALIZER_USER", self.postgres_user.as_str()),
            (
                "POSTGRES_NORMALIZER_PASSWORD",
                self.postgres_password.as_str(),
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
        if self.execution_mode != ExecutionMode::Replay
            && self.replay_input == ReplayInput::Selective
        {
            return Err(config_error(
                "selective replay input requires REPLAY execution mode",
            ));
        }
        if !matches!(self.linux_sshd_parser_version.as_str(), "1" | "2") {
            return Err(config_error(
                "CERBERO_NORMALIZER_LINUX_SSHD_PARSER_VERSION must be 1 or 2",
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
        if self.postgres_port == 0 {
            return Err(config_error("POSTGRES_PORT must be greater than zero"));
        }
        if self.retry_budget == 0 {
            return Err(config_error(
                "CERBERO_NORMALIZER_RETRY_BUDGET must be greater than zero",
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

fn parse_u16(name: &str) -> Result<u16, NormalizerError> {
    required(name)?.parse::<u16>().map_err(|_| {
        config_error(&format!(
            "{name} must contain an integer between 0 and 65535"
        ))
    })
}

fn parse_u32(name: &str) -> Result<u32, NormalizerError> {
    required(name)?
        .parse::<u32>()
        .map_err(|_| config_error(&format!("{name} must contain a non-negative integer")))
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
            replay_input: ReplayInput::Historical,
            linux_sshd_parser_version: "1".to_string(),
            source_time_policies: SourceTimePolicyRegistry::default(),
            raw_store_path: PathBuf::from("var/raw"),
            clickhouse_url: "http://127.0.0.1:8123".to_string(),
            clickhouse_database: "cerbero".to_string(),
            clickhouse_user: "normalizer".to_string(),
            clickhouse_password: "secret".to_string(),
            postgres_host: "127.0.0.1".to_string(),
            postgres_port: 5432,
            postgres_database: "cerbero".to_string(),
            postgres_user: "normalizer".to_string(),
            postgres_password: "secret".to_string(),
            retry_min_delay: Duration::from_secs(5),
            retry_max_delay: Duration::from_secs(1),
            retry_budget: 5,
        };
        assert_eq!(config.validate().unwrap_err().code, "CER-NORM-CONFIG");
    }

    #[test]
    fn validate_rejects_unknown_governed_sshd_parser_version() {
        let config = RuntimeConfig {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            nats_user: "u".to_string(),
            nats_password: "p".to_string(),
            component_version: "dev".to_string(),
            instance_id: "normalizer-1".to_string(),
            pipeline_version: "normalizer-v1".to_string(),
            execution_mode: ExecutionMode::Replay,
            replay_input: ReplayInput::Historical,
            linux_sshd_parser_version: "3".to_string(),
            source_time_policies: SourceTimePolicyRegistry::default(),
            raw_store_path: PathBuf::from("var/raw"),
            clickhouse_url: "http://127.0.0.1:8123".to_string(),
            clickhouse_database: "cerbero".to_string(),
            clickhouse_user: "normalizer".to_string(),
            clickhouse_password: "secret".to_string(),
            postgres_host: "127.0.0.1".to_string(),
            postgres_port: 5432,
            postgres_database: "cerbero".to_string(),
            postgres_user: "normalizer".to_string(),
            postgres_password: "secret".to_string(),
            retry_min_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(5),
            retry_budget: 5,
        };
        assert_eq!(config.validate().unwrap_err().code, "CER-NORM-CONFIG");
    }

    #[test]
    fn validate_rejects_zero_retry_budget() {
        let config = RuntimeConfig {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            nats_user: "u".to_string(),
            nats_password: "p".to_string(),
            component_version: "dev".to_string(),
            instance_id: "normalizer-1".to_string(),
            pipeline_version: "normalizer-v1".to_string(),
            execution_mode: ExecutionMode::Live,
            replay_input: ReplayInput::Historical,
            linux_sshd_parser_version: "1".to_string(),
            source_time_policies: SourceTimePolicyRegistry::default(),
            raw_store_path: PathBuf::from("var/raw"),
            clickhouse_url: "http://127.0.0.1:8123".to_string(),
            clickhouse_database: "cerbero".to_string(),
            clickhouse_user: "normalizer".to_string(),
            clickhouse_password: "secret".to_string(),
            postgres_host: "127.0.0.1".to_string(),
            postgres_port: 5432,
            postgres_database: "cerbero".to_string(),
            postgres_user: "normalizer".to_string(),
            postgres_password: "secret".to_string(),
            retry_min_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(5),
            retry_budget: 0,
        };

        let error = config.validate().unwrap_err();
        assert_eq!(error.code, "CER-NORM-CONFIG");
        assert_eq!(
            error.message,
            "CERBERO_NORMALIZER_RETRY_BUDGET must be greater than zero"
        );
    }

    #[test]
    fn validate_rejects_selective_replay_input_outside_replay_mode() {
        let mut config = RuntimeConfig {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            nats_user: "u".to_string(),
            nats_password: "p".to_string(),
            component_version: "dev".to_string(),
            instance_id: "normalizer-1".to_string(),
            pipeline_version: "normalizer-v1".to_string(),
            execution_mode: ExecutionMode::Live,
            replay_input: ReplayInput::Historical,
            linux_sshd_parser_version: "1".to_string(),
            source_time_policies: SourceTimePolicyRegistry::default(),
            raw_store_path: PathBuf::from("var/raw"),
            clickhouse_url: "http://127.0.0.1:8123".to_string(),
            clickhouse_database: "cerbero".to_string(),
            clickhouse_user: "normalizer".to_string(),
            clickhouse_password: "secret".to_string(),
            postgres_host: "127.0.0.1".to_string(),
            postgres_port: 5432,
            postgres_database: "cerbero".to_string(),
            postgres_user: "normalizer".to_string(),
            postgres_password: "secret".to_string(),
            retry_min_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(5),
            retry_budget: 5,
        };
        config.replay_input = ReplayInput::Selective;
        let error = config.validate().unwrap_err();
        assert_eq!(error.code, "CER-NORM-CONFIG");
    }

    #[test]
    fn validate_accepts_governed_source_time_policy_registry() {
        let policies =
            SourceTimePolicyRegistry::parse_spec("source-a=-04:00,source-b=+05:30").unwrap();
        assert_eq!(
            policies.policy_canonical("source-a").as_deref(),
            Some("fixed_utc_offset=-04:00;rfc3164_year=nearest_ingest_year")
        );
    }
}
