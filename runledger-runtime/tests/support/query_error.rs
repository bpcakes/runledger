pub fn query_error_code(error: &runledger_postgres::Error) -> Option<&str> {
    match error {
        runledger_postgres::Error::QueryError(query_error) => Some(query_error.code()),
        _ => None,
    }
}
