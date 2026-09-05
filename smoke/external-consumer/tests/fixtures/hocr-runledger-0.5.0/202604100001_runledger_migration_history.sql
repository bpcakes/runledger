CREATE TABLE IF NOT EXISTS runledger_migration_history (
    version BIGINT PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO runledger_migration_history (version)
VALUES (202603280001), (202604100001)
ON CONFLICT (version) DO NOTHING;
