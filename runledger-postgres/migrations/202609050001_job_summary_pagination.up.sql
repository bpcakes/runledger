-- Support compact pages with and without an exact organization scope.
-- Include the UUID tie-breaker so deep cursors constrain the full ordering key.
CREATE INDEX idx_job_queue_scope_created_id
    ON job_queue (organization_id, created_at DESC, id DESC);
CREATE INDEX idx_job_queue_created_id
    ON job_queue (created_at DESC, id DESC);

-- Additive indexes do not change the persisted contract. Omit this migration
-- from runledger_migration_history so older filtered startup helpers coexist.
