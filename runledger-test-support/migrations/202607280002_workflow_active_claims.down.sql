DROP TRIGGER IF EXISTS trg_workflow_runs_mark_active_claim_release_pending
    ON workflow_runs;

DROP FUNCTION IF EXISTS mark_terminal_workflow_active_claim_release_pending();

DROP TABLE IF EXISTS workflow_active_claims;
