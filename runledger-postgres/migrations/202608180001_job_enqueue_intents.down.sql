DROP TRIGGER IF EXISTS trg_job_enqueue_intents_set_updated_at
    ON job_enqueue_intents;

DROP TABLE IF EXISTS job_enqueue_intents;
