use uuid::Uuid;

use crate::{IdGenerator, NormalizerError};

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemIdGenerator;

impl IdGenerator for SystemIdGenerator {
    fn new_uuid_v7(&mut self) -> Result<String, NormalizerError> {
        Ok(Uuid::now_v7().to_string())
    }
}

#[must_use]
pub fn new_uuid_v7() -> String {
    Uuid::now_v7().to_string()
}
