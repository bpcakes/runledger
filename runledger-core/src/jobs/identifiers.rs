use std::fmt;

use super::identifier_macros::{define_identifier, define_owned_identifier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierValidationError {
    BlankJobType,
    BlankWorkflowType,
    BlankStepKey,
}

impl fmt::Display for IdentifierValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlankJobType => write!(f, "job_type must be non-empty"),
            Self::BlankWorkflowType => write!(f, "workflow_type must be non-empty"),
            Self::BlankStepKey => write!(f, "step_key must be non-empty"),
        }
    }
}

impl std::error::Error for IdentifierValidationError {}

fn validate_identifier(
    value: &str,
    blank_error: IdentifierValidationError,
) -> Result<(), IdentifierValidationError> {
    if value.trim().is_empty() {
        Err(blank_error)
    } else {
        Ok(())
    }
}

define_identifier!(JobType, BlankJobType);
define_identifier!(WorkflowType, BlankWorkflowType);
define_identifier!(StepKey, BlankStepKey);
define_owned_identifier!(JobTypeName, JobType, BlankJobType);
define_owned_identifier!(WorkflowTypeName, WorkflowType, BlankWorkflowType);
define_owned_identifier!(StepKeyName, StepKey, BlankStepKey);

#[cfg(feature = "sqlx-postgres")]
mod sqlx_postgres {
    use super::{
        IdentifierValidationError, JobType, JobTypeName, StepKey, StepKeyName, WorkflowType,
        WorkflowTypeName,
    };
    use sqlx::decode::Decode;
    use sqlx::encode::{Encode, IsNull};
    use sqlx::error::BoxDynError;
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
    use sqlx::{Postgres, Type};

    macro_rules! impl_postgres_text_identifier {
        ($identifier:ident) => {
            impl<'q> Type<Postgres> for $identifier<'q> {
                fn type_info() -> PgTypeInfo {
                    <&str as Type<Postgres>>::type_info()
                }

                fn compatible(ty: &PgTypeInfo) -> bool {
                    <&str as Type<Postgres>>::compatible(ty)
                }
            }

            impl<'q> Encode<'q, Postgres> for $identifier<'q> {
                fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
                    <&str as Encode<Postgres>>::encode(self.as_str(), buf)
                }

                fn size_hint(&self) -> usize {
                    self.as_str().len()
                }
            }
        };
    }

    macro_rules! impl_postgres_owned_text_identifier {
        ($owned_identifier:ident) => {
            impl Type<Postgres> for $owned_identifier {
                fn type_info() -> PgTypeInfo {
                    <String as Type<Postgres>>::type_info()
                }

                fn compatible(ty: &PgTypeInfo) -> bool {
                    <String as Type<Postgres>>::compatible(ty)
                }
            }

            impl<'q> Encode<'q, Postgres> for $owned_identifier {
                fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
                    <&str as Encode<Postgres>>::encode(self.as_str(), buf)
                }

                fn size_hint(&self) -> usize {
                    self.as_str().len()
                }
            }

            impl<'r> Decode<'r, Postgres> for $owned_identifier {
                fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
                    let value = <String as Decode<Postgres>>::decode(value)?;
                    Self::new(value).map_err(|error: IdentifierValidationError| error.into())
                }
            }
        };
    }

    impl_postgres_text_identifier!(JobType);
    impl_postgres_text_identifier!(WorkflowType);
    impl_postgres_text_identifier!(StepKey);
    impl_postgres_owned_text_identifier!(JobTypeName);
    impl_postgres_owned_text_identifier!(WorkflowTypeName);
    impl_postgres_owned_text_identifier!(StepKeyName);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        IdentifierValidationError, JobType, JobTypeName, StepKey, StepKeyName, WorkflowType,
        WorkflowTypeName,
    };

    #[test]
    fn job_type_compares_with_str_and_string() {
        let value = JobType::new("jobs.test");
        let owned = "jobs.test".to_string();

        assert_eq!(value, "jobs.test");
        assert_eq!("jobs.test", value);
        assert_eq!(value, owned);
        assert_eq!(owned, value);
    }

    #[test]
    fn job_type_try_new_rejects_blank_values() {
        assert_eq!(
            JobType::try_new(""),
            Err(IdentifierValidationError::BlankJobType)
        );
        assert_eq!(
            JobType::try_new("   "),
            Err(IdentifierValidationError::BlankJobType)
        );
    }

    #[test]
    fn job_type_try_from_str_rejects_blank_values() {
        assert_eq!(
            JobType::try_from(""),
            Err(IdentifierValidationError::BlankJobType)
        );
    }

    #[test]
    fn job_type_hash_map_supports_str_lookup() {
        let mut values = HashMap::new();
        values.insert(JobType::new("jobs.test"), 42);

        assert_eq!(values.get("jobs.test"), Some(&42));
    }

    #[test]
    fn owned_job_type_supports_str_lookup_and_borrowed_conversion() {
        let mut values = HashMap::new();
        values.insert(JobTypeName::new("jobs.test").expect("valid identifier"), 42);

        assert_eq!(values.get("jobs.test"), Some(&42));
        assert_eq!(
            JobTypeName::new("jobs.test")
                .expect("valid identifier")
                .as_borrowed(),
            JobType::new("jobs.test")
        );
        assert_eq!(
            JobTypeName::try_from(JobType::new("jobs.test")),
            Ok(JobTypeName::new("jobs.test").expect("valid identifier"))
        );
    }

    #[test]
    fn owned_job_type_try_from_borrowed_rejects_blank_values() {
        assert_eq!(
            JobTypeName::try_from(JobType::new("   ")),
            Err(IdentifierValidationError::BlankJobType)
        );
    }

    #[test]
    fn owned_job_type_deserialize_rejects_blank_values() {
        assert_eq!(
            serde_json::from_str::<JobTypeName>("\"\"")
                .expect_err("empty job type should fail")
                .to_string(),
            "job_type must be non-empty"
        );
        assert_eq!(
            serde_json::from_str::<JobTypeName>("\"   \"")
                .expect_err("blank job type should fail")
                .to_string(),
            "job_type must be non-empty"
        );
    }

    #[test]
    fn owned_job_type_roundtrips_through_serde() {
        let value = JobTypeName::new("jobs.test").expect("valid identifier");
        let serialized = serde_json::to_string(&value).expect("serialize job type");
        let deserialized =
            serde_json::from_str::<JobTypeName>(&serialized).expect("deserialize job type");

        assert_eq!(deserialized, value);
    }

    #[test]
    fn workflow_type_compares_with_str_and_string() {
        let value = WorkflowType::new("workflow.test");
        let owned = "workflow.test".to_string();

        assert_eq!(value, "workflow.test");
        assert_eq!("workflow.test", value);
        assert_eq!(value, owned);
        assert_eq!(owned, value);
    }

    #[test]
    fn workflow_type_try_new_rejects_blank_values() {
        assert_eq!(
            WorkflowType::try_new(""),
            Err(IdentifierValidationError::BlankWorkflowType)
        );
        assert_eq!(
            WorkflowType::try_new("   "),
            Err(IdentifierValidationError::BlankWorkflowType)
        );
    }

    #[test]
    fn workflow_type_try_from_str_rejects_blank_values() {
        assert_eq!(
            WorkflowType::try_from(""),
            Err(IdentifierValidationError::BlankWorkflowType)
        );
    }

    #[test]
    fn owned_workflow_type_rejects_blank_values() {
        assert_eq!(
            WorkflowTypeName::new(""),
            Err(IdentifierValidationError::BlankWorkflowType)
        );
    }

    #[test]
    fn owned_workflow_type_try_from_borrowed_rejects_blank_values() {
        assert_eq!(
            WorkflowTypeName::try_from(WorkflowType::new("   ")),
            Err(IdentifierValidationError::BlankWorkflowType)
        );
    }

    #[test]
    fn owned_workflow_type_deserialize_rejects_blank_values() {
        assert_eq!(
            serde_json::from_str::<WorkflowTypeName>("\"\"")
                .expect_err("empty workflow type should fail")
                .to_string(),
            "workflow_type must be non-empty"
        );
        assert_eq!(
            serde_json::from_str::<WorkflowTypeName>("\"   \"")
                .expect_err("blank workflow type should fail")
                .to_string(),
            "workflow_type must be non-empty"
        );
    }

    #[test]
    fn owned_workflow_type_roundtrips_through_serde() {
        let value = WorkflowTypeName::new("workflow.test").expect("valid identifier");
        let serialized = serde_json::to_string(&value).expect("serialize workflow type");
        let deserialized = serde_json::from_str::<WorkflowTypeName>(&serialized)
            .expect("deserialize workflow type");

        assert_eq!(deserialized, value);
    }

    #[test]
    fn step_key_compares_with_str_and_string() {
        let value = StepKey::new("step.test");
        let owned = "step.test".to_string();

        assert_eq!(value, "step.test");
        assert_eq!("step.test", value);
        assert_eq!(value, owned);
        assert_eq!(owned, value);
    }

    #[test]
    fn step_key_try_new_rejects_blank_values() {
        assert_eq!(
            StepKey::try_new(""),
            Err(IdentifierValidationError::BlankStepKey)
        );
        assert_eq!(
            StepKey::try_new("   "),
            Err(IdentifierValidationError::BlankStepKey)
        );
    }

    #[test]
    fn step_key_try_from_str_rejects_blank_values() {
        assert_eq!(
            StepKey::try_from(""),
            Err(IdentifierValidationError::BlankStepKey)
        );
    }

    #[test]
    fn owned_step_key_rejects_blank_values() {
        assert_eq!(
            StepKeyName::new(""),
            Err(IdentifierValidationError::BlankStepKey)
        );
    }

    #[test]
    fn owned_step_key_try_from_borrowed_rejects_blank_values() {
        assert_eq!(
            StepKeyName::try_from(StepKey::new("   ")),
            Err(IdentifierValidationError::BlankStepKey)
        );
    }

    #[test]
    fn owned_step_key_deserialize_rejects_blank_values() {
        assert_eq!(
            serde_json::from_str::<StepKeyName>("\"\"")
                .expect_err("empty step key should fail")
                .to_string(),
            "step_key must be non-empty"
        );
        assert_eq!(
            serde_json::from_str::<StepKeyName>("\"   \"")
                .expect_err("blank step key should fail")
                .to_string(),
            "step_key must be non-empty"
        );
    }

    #[test]
    fn owned_step_key_roundtrips_through_serde() {
        let value = StepKeyName::new("step.test").expect("valid identifier");
        let serialized = serde_json::to_string(&value).expect("serialize step key");
        let deserialized =
            serde_json::from_str::<StepKeyName>(&serialized).expect("deserialize step key");

        assert_eq!(deserialized, value);
    }

    #[test]
    fn unchecked_new_preserves_blank_values() {
        assert_eq!(JobType::new("").as_str(), "");
        assert_eq!(WorkflowType::new(" ").as_str(), " ");
        assert_eq!(StepKey::new("").as_str(), "");
    }
}
