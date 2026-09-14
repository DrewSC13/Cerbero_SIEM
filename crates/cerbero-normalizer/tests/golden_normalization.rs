use cerbero_common::contracts::sha256_lower_hex;
use cerbero_common::contracts::v1::{CerberoEnvelope, ExecutionMode, RawEventPersisted};
use cerbero_normalizer::{FixedClock, FixedIdGenerator, NormalizerCore, NormalizerCoreConfig};
use prost_types::Timestamp;

const RAW: &[u8] = b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2";

fn persisted() -> RawEventPersisted {
    RawEventPersisted {
        event_id: "01995000-0000-7000-8000-000000000102".to_string(),
        tenant_id: "tenant-a".to_string(),
        source_id: "source-a".to_string(),
        event_time: Some(Timestamp {
            seconds: 1_789_000_000,
            nanos: 0,
        }),
        ingest_time: Some(Timestamp {
            seconds: 1_789_000_001,
            nanos: 0,
        }),
        raw_size: u64::try_from(RAW.len()).unwrap(),
        raw_hash_algorithm: "sha256".to_string(),
        raw_hash: sha256_lower_hex(RAW),
        pipeline_version: "ingest-v1".to_string(),
        storage_uri: "raw:///tenant-a/2026/09/13/12/01995000-0000-7000-8000-000000000102/raw.bin"
            .to_string(),
        segment_id: "01995000-0000-7000-8000-000000000102".to_string(),
        length: u64::try_from(RAW.len()).unwrap(),
        persisted_at: Some(Timestamp {
            seconds: 1_789_000_001,
            nanos: 1,
        }),
        ..Default::default()
    }
}

fn envelope() -> CerberoEnvelope {
    CerberoEnvelope {
        contract_version: "1".to_string(),
        message_id: "01995000-0000-7000-8000-000000000103".to_string(),
        message_type: "RawEventPersisted".to_string(),
        tenant_id: "tenant-a".to_string(),
        payload_schema: "cerbero.raw_event_persisted.v1".to_string(),
        ..Default::default()
    }
}

fn plan(parser_version: &str, id_suffix: &str) -> cerbero_normalizer::NormalizationPlan {
    let mut core = NormalizerCore::new_with_linux_sshd_version(
        NormalizerCoreConfig {
            pipeline_version: "normalizer-v1".to_string(),
            execution_mode: ExecutionMode::Live,
        },
        parser_version,
        Box::new(FixedClock(Timestamp {
            seconds: 1_789_000_002,
            nanos: 0,
        })),
        Box::new(FixedIdGenerator::new([
            format!("01995000-0000-7000-8000-0000000001{id_suffix}"),
            format!("01995000-0000-7000-8000-0000000002{id_suffix}"),
        ])),
    )
    .unwrap();
    core.normalize(&envelope(), &persisted(), RAW).unwrap()
}

#[test]
fn sshd_v1_and_v2_match_versioned_ocsf_goldens() {
    for (version, fixture) in [
        ("1", include_str!("fixtures/golden/sshd-v1-ocsf.json")),
        ("2", include_str!("fixtures/golden/sshd-v2-ocsf.json")),
    ] {
        let id_suffix = if version == "1" { "10" } else { "20" };
        let plan = plan(version, id_suffix);
        let expected: serde_json::Value = serde_json::from_str(fixture).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&plan.canonical_ocsf_json).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            plan.normalized_event.normalized_hash,
            sha256_lower_hex(&plan.canonical_ocsf_json)
        );
        assert_eq!(plan.normalized_event.parser_version, version);
    }
}

#[test]
fn sshd_v2_golden_is_a_new_interpretation_not_a_rewrite() {
    let v1 = plan("1", "30");
    let v2 = plan("2", "40");
    assert_eq!(
        v1.normalized_event.raw_event_id,
        v2.normalized_event.raw_event_id
    );
    assert_ne!(v1.logical_key, v2.logical_key);
    assert_ne!(
        v1.normalized_event.normalized_hash,
        v2.normalized_event.normalized_hash
    );
}
