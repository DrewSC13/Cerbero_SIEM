use cerbero_common::contracts::v1::NormalizedEvent;
use prost_types::{Struct, Value, value::Kind};

use crate::{EventFieldState, EventFieldView};

/// Read-only EVENT-evaluation adapter over the canonical v1 `NormalizedEvent`.
///
/// Detection field references resolve only inside `NormalizedEvent.ocsf_event`.
/// Dotted field paths traverse nested protobuf `Struct` objects without copying
/// event values. The adapter preserves absent, explicit null, and malformed
/// states so leaf-evaluation policy remains explicit and deterministic.
#[derive(Clone, Copy)]
pub struct NormalizedEventFieldView<'event> {
    event: &'event NormalizedEvent,
}

impl<'event> NormalizedEventFieldView<'event> {
    /// Creates a view over one canonical normalized event.
    #[must_use]
    pub const fn new(event: &'event NormalizedEvent) -> Self {
        Self { event }
    }
}

impl<Field> EventFieldView<Field> for NormalizedEventFieldView<'_>
where
    Field: AsRef<str>,
{
    type Value = Value;

    fn field_state(&self, field: &Field) -> EventFieldState<'_, Self::Value> {
        let Some(root) = self.event.ocsf_event.as_ref() else {
            return EventFieldState::Malformed;
        };

        resolve_ocsf_field(root, field.as_ref())
    }
}

fn resolve_ocsf_field<'event>(root: &'event Struct, path: &str) -> EventFieldState<'event, Value> {
    if path.is_empty() {
        return EventFieldState::Malformed;
    }

    let mut segments = path.split('.').peekable();
    let mut current = root;

    while let Some(segment) = segments.next() {
        if segment.is_empty() {
            return EventFieldState::Malformed;
        }

        let Some(value) = current.fields.get(segment) else {
            return EventFieldState::Absent;
        };

        if segments.peek().is_none() {
            return classify_value(value);
        }

        current = match value.kind.as_ref() {
            Some(Kind::StructValue(next)) => next,
            Some(Kind::NullValue(0)) => return EventFieldState::Null,
            Some(Kind::NullValue(_) | _) | None => return EventFieldState::Malformed,
        };
    }

    EventFieldState::Malformed
}

fn classify_value(value: &Value) -> EventFieldState<'_, Value> {
    match value.kind.as_ref() {
        Some(Kind::NullValue(0)) => EventFieldState::Null,
        Some(Kind::NullValue(_)) | None => EventFieldState::Malformed,
        Some(_) => EventFieldState::Present(value),
    }
}
