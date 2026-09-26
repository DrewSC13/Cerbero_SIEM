use crate::{ComparisonOperator, ExecutionPlan};

/// Runtime state of a normalized field presented to EVENT evaluation.
///
/// PARSING & OCSF v1 requires rules to distinguish absent, null, and malformed
/// fields rather than treating every non-value as the same condition. Concrete
/// rule behavior for those states remains policy-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventFieldState<'a, Value> {
    Present(&'a Value),
    Absent,
    Null,
    Malformed,
}

/// Backend-neutral compiled predicate representation for EVENT execution plans.
///
/// This is intentionally distinct from the Detection AST: execution consumes
/// plan predicates, not the AST directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventPredicate<Field, RuleValue> {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Comparison {
        field: Field,
        operator: ComparisonOperator,
        value: RuleValue,
    },
    Exists {
        field: Field,
    },
}

/// Final boolean outcome of evaluating one EVENT plan against one event view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventEvaluationResult {
    Match,
    NoMatch,
}

/// Read-only normalized-event field boundary used by the EVENT evaluator.
pub trait EventFieldView<Field> {
    type Value;

    fn field_state(&self, field: &Field) -> EventFieldState<'_, Self::Value>;
}

/// Rule-owned leaf semantics for existence and comparison predicates.
///
/// The core deliberately does not decide how absent, null, or malformed fields
/// behave, nor does it freeze type-specific comparison or regex semantics. The
/// supplied policy must make those choices explicitly and deterministically.
pub trait EventLeafEvaluationPolicy<Field, RuleValue, EventValue> {
    type Error;

    /// Evaluates an `EXISTS` predicate for one resolved field state.
    ///
    /// # Errors
    ///
    /// Returns a policy-defined error when the rule cannot evaluate the state.
    fn evaluate_exists(
        &self,
        field: &Field,
        state: EventFieldState<'_, EventValue>,
    ) -> Result<bool, Self::Error>;

    /// Evaluates one comparison predicate for one resolved field state.
    ///
    /// # Errors
    ///
    /// Returns a policy-defined error when the comparison cannot be evaluated.
    fn evaluate_comparison(
        &self,
        field: &Field,
        state: EventFieldState<'_, EventValue>,
        operator: ComparisonOperator,
        rule_value: &RuleValue,
    ) -> Result<bool, Self::Error>;
}

/// Minimal view over the predicate portion of an EVENT execution plan.
pub trait EventExecutionPlanView<Field, RuleValue> {
    fn event_predicates(&self) -> &EventPredicate<Field, RuleValue>;
}

impl<
    PlanId,
    RuleVersion,
    RequiredFields,
    Field,
    RuleValue,
    Grouping,
    Window,
    ResourceLimits,
    PlanHash,
> EventExecutionPlanView<Field, RuleValue>
    for ExecutionPlan<
        PlanId,
        RuleVersion,
        RequiredFields,
        EventPredicate<Field, RuleValue>,
        Grouping,
        Window,
        ResourceLimits,
        PlanHash,
    >
{
    fn event_predicates(&self) -> &EventPredicate<Field, RuleValue> {
        &self.predicates
    }
}

/// Evaluates one compiled EVENT execution plan against one normalized-event view.
///
/// Boolean composition is deterministic and short-circuiting. Leaf behavior is
/// delegated to [`EventLeafEvaluationPolicy`] so rules can define explicit
/// semantics for present, absent, null, and malformed fields without the core
/// inventing a universal null/missing policy.
///
/// # Errors
///
/// Propagates errors returned by the leaf-evaluation policy.
pub fn evaluate_event_execution_plan<Field, RuleValue, Plan, View, Policy>(
    plan: &Plan,
    event: &View,
    policy: &Policy,
) -> Result<EventEvaluationResult, Policy::Error>
where
    Plan: EventExecutionPlanView<Field, RuleValue>,
    View: EventFieldView<Field>,
    Policy: EventLeafEvaluationPolicy<Field, RuleValue, View::Value>,
{
    if evaluate_predicate(plan.event_predicates(), event, policy)? {
        Ok(EventEvaluationResult::Match)
    } else {
        Ok(EventEvaluationResult::NoMatch)
    }
}

fn evaluate_predicate<Field, RuleValue, View, Policy>(
    predicate: &EventPredicate<Field, RuleValue>,
    event: &View,
    policy: &Policy,
) -> Result<bool, Policy::Error>
where
    View: EventFieldView<Field>,
    Policy: EventLeafEvaluationPolicy<Field, RuleValue, View::Value>,
{
    match predicate {
        EventPredicate::And(children) => {
            for child in children {
                if !evaluate_predicate(child, event, policy)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        EventPredicate::Or(children) => {
            for child in children {
                if evaluate_predicate(child, event, policy)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        EventPredicate::Not(child) => Ok(!evaluate_predicate(child, event, policy)?),
        EventPredicate::Comparison {
            field,
            operator,
            value,
        } => policy.evaluate_comparison(field, event.field_state(field), *operator, value),
        EventPredicate::Exists { field } => policy.evaluate_exists(field, event.field_state(field)),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::convert::Infallible;

    use crate::ComparisonOperator;

    use super::{
        EventEvaluationResult, EventExecutionPlanView, EventFieldState, EventFieldView,
        EventLeafEvaluationPolicy, EventPredicate, evaluate_event_execution_plan,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestValue {
        Text(&'static str),
        Null,
        Malformed,
    }

    struct TestEvent {
        fields: HashMap<&'static str, TestValue>,
    }

    impl EventFieldView<&'static str> for TestEvent {
        type Value = TestValue;

        fn field_state(&self, field: &&'static str) -> EventFieldState<'_, Self::Value> {
            match self.fields.get(field) {
                Some(TestValue::Null) => EventFieldState::Null,
                Some(TestValue::Malformed) => EventFieldState::Malformed,
                Some(value) => EventFieldState::Present(value),
                None => EventFieldState::Absent,
            }
        }
    }

    struct TestPolicy {
        leaf_calls: Cell<u32>,
    }

    impl EventLeafEvaluationPolicy<&'static str, &'static str, TestValue> for TestPolicy {
        type Error = Infallible;

        fn evaluate_exists(
            &self,
            _field: &&'static str,
            state: EventFieldState<'_, TestValue>,
        ) -> Result<bool, Self::Error> {
            self.leaf_calls.set(self.leaf_calls.get() + 1);
            Ok(matches!(state, EventFieldState::Present(_)))
        }

        fn evaluate_comparison(
            &self,
            _field: &&'static str,
            state: EventFieldState<'_, TestValue>,
            operator: ComparisonOperator,
            rule_value: &&'static str,
        ) -> Result<bool, Self::Error> {
            self.leaf_calls.set(self.leaf_calls.get() + 1);
            let EventFieldState::Present(TestValue::Text(actual)) = state else {
                return Ok(false);
            };

            Ok(match operator {
                ComparisonOperator::Eq => actual == rule_value,
                ComparisonOperator::Contains => actual.contains(*rule_value),
                _ => false,
            })
        }
    }

    struct TestPlan {
        predicates: EventPredicate<&'static str, &'static str>,
    }

    impl EventExecutionPlanView<&'static str, &'static str> for TestPlan {
        fn event_predicates(&self) -> &EventPredicate<&'static str, &'static str> {
            &self.predicates
        }
    }

    fn event() -> TestEvent {
        TestEvent {
            fields: HashMap::from([
                ("process.name", TestValue::Text("powershell.exe")),
                (
                    "process.command_line",
                    TestValue::Text("powershell.exe -EncodedCommand AAAA"),
                ),
                ("user.null", TestValue::Null),
                ("user.malformed", TestValue::Malformed),
            ]),
        }
    }

    #[test]
    fn event_plan_evaluates_boolean_match_and_no_match() {
        let positive_plan = TestPlan {
            predicates: EventPredicate::And(vec![
                EventPredicate::Comparison {
                    field: "process.name",
                    operator: ComparisonOperator::Eq,
                    value: "powershell.exe",
                },
                EventPredicate::Comparison {
                    field: "process.command_line",
                    operator: ComparisonOperator::Contains,
                    value: "-EncodedCommand",
                },
                EventPredicate::Not(Box::new(EventPredicate::Exists { field: "user.name" })),
            ]),
        };
        let negative_plan = TestPlan {
            predicates: EventPredicate::Comparison {
                field: "process.name",
                operator: ComparisonOperator::Eq,
                value: "cmd.exe",
            },
        };
        let policy = TestPolicy {
            leaf_calls: Cell::new(0),
        };

        assert_eq!(
            evaluate_event_execution_plan(&positive_plan, &event(), &policy),
            Ok(EventEvaluationResult::Match)
        );
        assert_eq!(
            evaluate_event_execution_plan(&negative_plan, &event(), &policy),
            Ok(EventEvaluationResult::NoMatch)
        );
    }

    #[test]
    fn evaluator_short_circuits_boolean_predicates() {
        let plan = TestPlan {
            predicates: EventPredicate::And(vec![
                EventPredicate::Comparison {
                    field: "process.name",
                    operator: ComparisonOperator::Eq,
                    value: "cmd.exe",
                },
                EventPredicate::Exists {
                    field: "process.command_line",
                },
            ]),
        };
        let policy = TestPolicy {
            leaf_calls: Cell::new(0),
        };

        assert_eq!(
            evaluate_event_execution_plan(&plan, &event(), &policy),
            Ok(EventEvaluationResult::NoMatch)
        );
        assert_eq!(policy.leaf_calls.get(), 1);
    }

    #[test]
    fn field_view_preserves_absent_null_and_malformed_states() {
        let event = event();

        assert_eq!(event.field_state(&"missing.field"), EventFieldState::Absent);
        assert_eq!(event.field_state(&"user.null"), EventFieldState::Null);
        assert_eq!(
            event.field_state(&"user.malformed"),
            EventFieldState::Malformed
        );
    }

    #[test]
    fn leaf_policy_owns_missing_and_null_behavior() {
        let policy = TestPolicy {
            leaf_calls: Cell::new(0),
        };
        let missing = TestPlan {
            predicates: EventPredicate::Exists {
                field: "missing.field",
            },
        };
        let null = TestPlan {
            predicates: EventPredicate::Exists { field: "user.null" },
        };

        assert_eq!(
            evaluate_event_execution_plan(&missing, &event(), &policy),
            Ok(EventEvaluationResult::NoMatch)
        );
        assert_eq!(
            evaluate_event_execution_plan(&null, &event(), &policy),
            Ok(EventEvaluationResult::NoMatch)
        );
    }
}
