//! CERBERO v1 contract bindings and validation helpers.

/// Generated Protobuf bindings for `cerbero.contracts.v1`.
pub mod v1 {
    #![allow(clippy::all, clippy::pedantic)]
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/generated/cerbero/contracts/v1/cerbero.contracts.v1.rs"
    ));
}

mod signal_validation;
mod validation;

pub use signal_validation::validate_signal;
pub use validation::{
    ContractViolation, sha256_lower_hex, validate_envelope, validate_normalized_event,
    validate_raw_event, validate_raw_event_persisted, validate_timestamp, validate_transformation,
    validate_uuid_v7,
};
