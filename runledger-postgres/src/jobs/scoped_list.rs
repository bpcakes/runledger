// SQLx requires literal SQL for compile-time checking. Keep the projection and
// filters at each call site, but select the scope predicate here. Separate
// statements keep organization predicates indexable even with generic prepared
// plans; a nullable equality or an admin OR flag cannot guarantee that.
//
// $1 is reserved for organization identity in every branch. Global/Admin bind
// NULL and test that parameter so the remaining filter positions stay stable.
macro_rules! scoped_list {
    ($row:path, $pool:expr, $scope:expr, $prefix:literal, $suffix:literal, $($arg:expr),+ $(,)?) => {
        match $scope {
            $crate::jobs::JobReadScope::Organization(id) => {
                sqlx::query_as!(
                    $row, $prefix + " organization_id = $1 " + $suffix,
                    Some(id), $($arg),+
                ).fetch_all($pool).await
            }
            $crate::jobs::JobReadScope::Global => {
                sqlx::query_as!(
                    $row, $prefix + " ($1::uuid IS NULL AND organization_id IS NULL) " + $suffix,
                    None::<sqlx::types::Uuid>, $($arg),+
                ).fetch_all($pool).await
            }
            $crate::jobs::JobReadScope::Admin => {
                sqlx::query_as!(
                    $row, $prefix + " $1::uuid IS NULL " + $suffix,
                    None::<sqlx::types::Uuid>, $($arg),+
                ).fetch_all($pool).await
            }
        }
    };
}

pub(super) use scoped_list;
