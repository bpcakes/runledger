use runledger_core::jobs::{
    JobContract, JobDefinitionSettings, JobSpec, JobSpecError, JobSpecs, JobSubmissionError,
    JobType,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize)]
struct Payload {
    request_id: String,
}
struct Email;
impl JobContract for Email {
    type Payload = Payload;
    fn spec() -> JobSpec {
        JobSpec::new(JobType::new("email.send"))
            .expect("static identity")
            .with_settings(JobDefinitionSettings::new().version(4).max_attempts(7))
            .expect("static settings")
    }
}

#[test]
fn producer_contract_preserves_wire_payload_and_leaves_policy_out_of_request() {
    let request = Email::submit(&Payload {
        request_id: "old-row".into(),
    })
    .expect("serialize");
    assert_eq!(request.job_type.as_str(), "email.send");
    assert_eq!(request.payload, json!({"request_id":"old-row"}));
    assert_eq!(request.max_attempts, None);
    assert_eq!(request.timeout_seconds, None);
    assert_eq!(request.priority, None);
    let spec = Email::spec()
        .with_settings(JobDefinitionSettings::new().version(8).max_attempts(9))
        .expect("updated settings");
    let updated = spec.submit(request.payload.clone()).expect("submit");
    assert_eq!(updated.payload, request.payload);
    assert_eq!(updated.max_attempts, request.max_attempts);
}

#[test]
fn rejects_invalid_specs_duplicate_identities_and_disabled_submissions() {
    assert_eq!(
        JobSpec::new(JobType::new("  ")).expect_err("blank"),
        JobSpecError::InvalidJobType
    );
    for settings in [
        JobDefinitionSettings::new().version(0),
        JobDefinitionSettings::new().max_attempts(0),
        JobDefinitionSettings::new().timeout_seconds(-1),
    ] {
        assert!(Email::spec().with_settings(settings).is_err());
    }
    assert!(matches!(
        JobSpecs::new([Email::spec(), Email::spec()]),
        Err(JobSpecError::DuplicateJobType(_))
    ));
    let spec = Email::spec()
        .with_settings(JobDefinitionSettings::new().enabled(false))
        .expect("disabled");
    assert!(matches!(
        spec.submit(json!({})),
        Err(JobSpecError::DisabledJobType(_))
    ));
}

#[test]
fn serialization_failure_is_recoverable() {
    #[derive(Deserialize)]
    struct Unserializable;
    impl Serialize for Unserializable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("cannot encode"))
        }
    }
    struct Broken;
    impl JobContract for Broken {
        type Payload = Unserializable;
        fn spec() -> JobSpec {
            Email::spec()
        }
    }
    assert!(matches!(
        Broken::submit(&Unserializable),
        Err(JobSubmissionError::Serialize(_))
    ));
}
