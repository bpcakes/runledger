use super::QueryErrorSpec;

mod jobs;
mod workflows;

pub(super) fn classify_constraint(constraint: &str) -> Option<QueryErrorSpec> {
    jobs::classify_constraint(constraint).or_else(|| workflows::classify_constraint(constraint))
}
