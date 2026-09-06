# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]==3.3.5"]
# ///
"""Run `uv run docs/research/probes/capacity_protocol.py` from any directory.

Creates and removes its OWN postgres:18 container, bound only to loopback.
Applies current migration SQL, then exercises a proposed protocol on real queue,
workflow, resource, attempt and event tables. It does not call the Rust APIs,
exercise SQLx migration-history validation, or benchmark the production runtime.
"""

import concurrent.futures
import json
from pathlib import Path
import subprocess
import threading
import time
import uuid

import psycopg


ROOT = Path(__file__).resolve().parents[3]
PROBE = Path(__file__).with_suffix(".sql")


def docker(*args):
    return subprocess.run(["docker", *args], text=True, capture_output=True, check=True).stdout.strip()


def scalar(conn, statement, args=()):
    return conn.execute(statement, args).fetchone()[0]


def job(conn, policies=(1, 2), resource=None, workflow=False, priority=100):
    jid = scalar(conn, """
        INSERT INTO job_queue(job_type, max_attempts, execution_resource_key, priority)
        VALUES ('capacity.probe', 8, %s, %s) RETURNING id
    """, (resource, priority))
    for policy in policies:
        conn.execute("INSERT INTO capacity_probe_requirements VALUES (%s, %s)", (jid, policy))
    if workflow:
        wid = scalar(conn, "INSERT INTO workflow_runs(workflow_type) VALUES ('capacity.probe') RETURNING id")
        conn.execute("""
            INSERT INTO workflow_steps(workflow_run_id, step_key, job_type, priority,
                max_attempts, timeout_seconds, stage, status, job_id, execution_resource_key)
            VALUES (%s, 'work', 'capacity.probe', 100, 8, 60, 'queued', 'ENQUEUED', %s, %s)
        """, (wid, jid, resource))
    return jid


def candidate(conn, jid, owner="worker"):
    conn.execute("SAVEPOINT candidate")
    try:
        admission = scalar(conn, "SELECT capacity_probe_claim(%s, %s)", (jid, owner))
    except psycopg.errors.LockNotAvailable:
        admission = None
    if admission is None:
        conn.execute("ROLLBACK TO SAVEPOINT candidate")
    conn.execute("RELEASE SAVEPOINT candidate")
    return admission


def begin(conn):
    conn.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
    conn.execute("SET LOCAL lock_timeout = '5ms'")
    conn.execute("SET LOCAL statement_timeout = '3s'")


def run(dsn):
    def connect():
        return psycopg.connect(dsn, autocommit=True)

    results = {}
    with connect() as conn:
        version = scalar(conn, "SHOW server_version")
        assert int(scalar(conn, "SHOW server_version_num")) // 10000 == 18
        paths = sorted((ROOT / "migrations").glob("*.up.sql"))
        for path in paths:
            with conn.transaction():
                conn.execute(path.read_text())
        conn.execute(PROBE.read_text())
        conn.execute("INSERT INTO job_definitions(job_type) VALUES ('capacity.probe')")
        conn.execute("INSERT INTO capacity_probe_policies VALUES (1, 3), (2, 3), (3, 1000)")

        # Independent sessions, reversed input order, real resource/workflow rows.
        jobs = [job(conn, (1, 2) if i % 2 else (2, 1), f"resource:{i}", True) for i in range(24)]
        barrier = threading.Barrier(24)

        def contender(jid):
            with connect() as other:
                barrier.wait(timeout=10)
                for _ in range(40):
                    begin(other)
                    admission = candidate(other, jid, str(jid))
                    other.execute("COMMIT")
                    if admission:
                        return True
                    time.sleep(0.003)
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
            successes = sum(executor.map(contender, jobs))
        assert successes == 3, successes
        assert scalar(conn, "SELECT count(*) FROM capacity_probe_permits") == 6
        assert scalar(conn, "SELECT count(*) FROM job_execution_resource_claims") == 3
        assert scalar(conn, "SELECT count(*) FROM workflow_steps WHERE status = 'RUNNING'") == 3
        assert scalar(conn, "SELECT count(*) FROM job_attempts") == 3
        results["multi_policy_resource_workflow_claims"] = successes
        conn.execute("UPDATE job_queue SET status = 'SUCCEEDED' WHERE status = 'LEASED'")

        # Earlier batch success is not committed if an unexpected later write fails.
        first, second = job(conn), job(conn)
        begin(conn)
        assert candidate(conn, first)
        assert candidate(conn, second)
        try:
            conn.execute("SELECT 1 / 0")
        except psycopg.errors.DivisionByZero:
            conn.execute("ROLLBACK")
        assert scalar(conn, "SELECT count(*) FROM job_attempts WHERE job_id = ANY(%s)", ([first, second],)) == 0
        assert scalar(conn, "SELECT count(*) FROM capacity_probe_permits") == 0
        results["outer_batch_failure_rolls_back_claims_and_audit"] = True

        # Retained earlier locks must not force a wait on another policy/job.
        with connect() as blocker:
            begin(blocker)
            blocker.execute("SELECT id FROM capacity_probe_policies WHERE id = 2 FOR NO KEY UPDATE")
            one, two = job(conn, (1,)), job(conn, (2,))
            begin(conn)
            assert candidate(conn, one)
            assert candidate(conn, two) is None
            conn.execute("COMMIT")
            blocker.execute("ROLLBACK")
        assert scalar(conn, "SELECT status::text FROM job_queue WHERE id = %s", (two,)) == "PENDING"
        results["denied_savepoint_preserves_earlier_success"] = True
        conn.execute("UPDATE job_queue SET status = 'SUCCEEDED' WHERE status = 'LEASED'")

        # FK writers can reference a policy while admission holds NO KEY UPDATE.
        with connect() as locker:
            begin(locker)
            locker.execute("SELECT id FROM capacity_probe_policies WHERE id = 1 FOR NO KEY UPDATE")
            begin(conn)
            job(conn, (1,))
            conn.execute("COMMIT")
            locker.execute("ROLLBACK")
        results["policy_lock_allows_requirement_foreign_key"] = True

        # Bound implicit unique-index wait from the legacy resource INSERT.
        a, b = job(conn, (1,), resource="shared"), job(conn, (2,), resource="shared")
        with connect() as locker:
            begin(locker)
            assert candidate(locker, a)
            begin(conn)
            assert candidate(conn, b) is None
            conn.execute("COMMIT")
            locker.execute("ROLLBACK")
        assert scalar(conn, "SELECT count(*) FROM capacity_probe_permits") == 0
        results["uncommitted_resource_conflict_rolls_back_candidate"] = True

        # Explicit step prelock protects claim from an inverted workflow writer.
        w = job(conn, workflow=True)
        with connect() as locker:
            begin(locker)
            locker.execute("SELECT id FROM workflow_steps WHERE job_id = %s FOR UPDATE", (w,))
            begin(conn)
            assert candidate(conn, w) is None
            conn.execute("COMMIT")
            locker.execute("ROLLBACK")
        results["workflow_step_contention_skipped"] = True

        # Reproduce the reverse step-order cycle identified in source review.
        older, newer = job(conn, (1,), workflow=True), job(conn, (1,), workflow=True)
        old_run = scalar(conn, "SELECT workflow_run_id FROM workflow_steps WHERE job_id = %s", (older,))
        conn.execute("UPDATE workflow_steps SET workflow_run_id = %s, step_key = 'second' WHERE job_id = %s", (old_run, newer))
        first_step_locked = threading.Event()

        def external_step_locker():
            with connect() as external:
                external.execute("SET application_name = 'capacity_reverse_step_probe'")
                begin(external)
                external.execute("SET LOCAL lock_timeout = '2s'")
                external.execute("SELECT id FROM workflow_steps WHERE job_id = %s FOR UPDATE", (older,))
                first_step_locked.set()
                external.execute("SELECT id FROM workflow_steps WHERE job_id = %s FOR UPDATE", (newer,))
                external.execute("COMMIT")

        begin(conn)
        assert candidate(conn, newer)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(external_step_locker)
            assert first_step_locked.wait(2)
            with connect() as observer:
                deadline = time.monotonic() + 1
                while not scalar(observer, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = 'capacity_reverse_step_probe' AND wait_event_type = 'Lock')"):
                    assert time.monotonic() < deadline
                    time.sleep(0.005)
            assert candidate(conn, older) is None
            conn.execute("COMMIT")
            pending.result(timeout=3)
        conn.execute("UPDATE job_queue SET status = 'SUCCEEDED' WHERE id = %s", (newer,))
        results["reverse_workflow_step_cycle_broken_by_nowait"] = True

        # Cancellation retains permits and RESTRICT prevents retention cascades.
        retained = job(conn, resource="canceled")
        begin(conn)
        admission = candidate(conn, retained)
        conn.execute("COMMIT")
        assert admission
        conn.execute("UPDATE job_queue SET status = 'CANCELED' WHERE id = %s", (retained,))
        assert scalar(conn, "SELECT count(*) FROM capacity_probe_permits WHERE release_after IS NOT NULL") == 2
        try:
            conn.execute("DELETE FROM job_queue WHERE id = %s", (retained,))
            raise AssertionError("retention deleted live retained permits")
        except (psycopg.errors.ForeignKeyViolation, psycopg.errors.RestrictViolation):
            pass
        assert scalar(conn, "SELECT count(*) FROM job_execution_resource_claims WHERE job_id = %s", (retained,)) == 1
        conn.execute("UPDATE capacity_probe_permits SET release_after = clock_timestamp() - interval '1s' WHERE job_id = %s", (retained,))
        conn.execute("DELETE FROM capacity_probe_permits WHERE release_after <= clock_timestamp()")
        conn.execute("DELETE FROM job_queue WHERE id = %s", (retained,))
        results["cancellation_retention_restricts_job_deletion"] = True

        # Same tuple reused after prestart release still needs a fresh token.
        reused = job(conn)
        begin(conn)
        old = candidate(conn, reused)
        conn.execute("COMMIT")
        conn.execute("UPDATE job_queue SET status = 'PENDING', attempt = attempt - 1, worker_id = NULL, lease_expires_at = NULL WHERE id = %s", (reused,))
        conn.execute("DELETE FROM job_attempts WHERE job_id = %s", (reused,))
        begin(conn)
        new = candidate(conn, reused)
        conn.execute("COMMIT")
        assert old and new and old != new
        assert conn.execute("UPDATE job_queue SET lease_expires_at = clock_timestamp() + interval '120s' WHERE id = %s AND capacity_probe_admission = %s", (reused, old)).rowcount == 0
        assert scalar(conn, "SELECT count(*) FROM capacity_probe_permits WHERE admission_id = %s", (new,)) == 2
        results["reused_attempt_rejects_stale_admission_token"] = True

        # Expiry alone is insufficient; heartbeat and reaper serialize on owner.
        conn.execute("UPDATE job_queue SET lease_expires_at = clock_timestamp() - interval '1s' WHERE id = %s", (reused,))
        conn.execute("DELETE FROM capacity_probe_permits p WHERE release_after IS NULL AND lease_expires_at <= clock_timestamp() AND NOT EXISTS (SELECT 1 FROM job_queue j WHERE j.id = p.job_id AND j.status = 'LEASED' AND j.capacity_probe_admission = p.admission_id)")
        assert scalar(conn, "SELECT count(*) FROM capacity_probe_permits WHERE job_id = %s", (reused,)) == 2
        with connect() as reaper:
            begin(reaper)
            reaper.execute("SELECT id FROM job_queue WHERE id = %s FOR UPDATE", (reused,))
            begin(conn)
            try:
                conn.execute("WITH locked AS MATERIALIZED (SELECT id FROM job_queue WHERE id = %s AND status = 'LEASED' FOR UPDATE) UPDATE job_queue j SET lease_expires_at = clock_timestamp() + interval '60s' FROM locked WHERE j.id = locked.id AND j.lease_expires_at > clock_timestamp()", (reused,))
            except psycopg.errors.LockNotAvailable:
                pass
            conn.execute("ROLLBACK")
            reaper.execute("UPDATE job_queue SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL WHERE id = %s", (reused,))
            reaper.execute("COMMIT")
        assert scalar(conn, "SELECT count(*) FROM capacity_probe_permits WHERE job_id = %s", (reused,)) == 0
        results["expiry_waits_for_owner_reap"] = True

        renewed = job(conn)
        begin(conn)
        assert candidate(conn, renewed)
        conn.execute("COMMIT")
        with connect() as heartbeat:
            begin(heartbeat)
            heartbeat.execute("UPDATE job_queue SET lease_expires_at = clock_timestamp() + interval '120s' WHERE id = %s", (renewed,))
            begin(conn)
            assert conn.execute("SELECT id FROM job_queue WHERE id = %s FOR UPDATE SKIP LOCKED", (renewed,)).fetchone() is None
            conn.execute("COMMIT")
            heartbeat.execute("COMMIT")
        assert scalar(conn, "SELECT bool_and(p.lease_expires_at = j.lease_expires_at) FROM capacity_probe_permits p JOIN job_queue j ON j.id = p.job_id WHERE j.id = %s", (renewed,))
        conn.execute("UPDATE job_queue SET status = 'SUCCEEDED' WHERE id = %s", (renewed,))
        results["heartbeat_first_keeps_permits_and_reaper_skips_owner"] = True

        # Old-style lease update without a capacity admission is rejected.
        old_writer = job(conn)
        try:
            conn.execute("UPDATE job_queue SET status = 'LEASED', attempt = 1, worker_id = 'old', lease_expires_at = now() + interval '60s' WHERE id = %s", (old_writer,))
            raise AssertionError("old writer bypassed guard")
        except psycopg.errors.CheckViolation:
            pass
        results["old_lease_writer_rejected"] = True

        # Traverse a saturated prefix with real admission attempts and 24-savepoint batches.
        conn.execute("UPDATE job_queue SET status = 'CANCELED' WHERE status = 'PENDING'")
        conn.execute("UPDATE capacity_probe_policies SET capacity = 1 WHERE id = 1")
        held = job(conn, (1,))
        begin(conn)
        assert candidate(conn, held)
        conn.execute("COMMIT")
        blocked = [job(conn, (1,), priority=200) for _ in range(300)]
        free = job(conn, (3,), priority=1)
        cursor = None
        seen = []
        for page in range(1, 17):
            rows = conn.execute("""
                SELECT id, priority, next_run_at, created_at FROM job_queue
                WHERE status = 'PENDING' AND next_run_at <= clock_timestamp()
                  AND (%s::uuid IS NULL OR (-priority, next_run_at, created_at, id) >
                       (SELECT -priority, next_run_at, created_at, id FROM job_queue WHERE id = %s))
                ORDER BY priority DESC, next_run_at, created_at, id LIMIT 128
            """, (cursor, cursor)).fetchall()
            begin(conn)
            for row in rows[:24]:
                seen.append(row[0])
                acquired = candidate(conn, row[0])
                assert bool(acquired) == (row[0] == free)
            conn.execute("COMMIT")
            if free in seen:
                break
            cursor = rows[:24][-1][0]
        assert page == 13 and len(seen) == 301
        urgent = job(conn, (3,), priority=1000)
        assert scalar(conn, "SELECT id FROM job_queue WHERE status = 'PENDING' ORDER BY priority DESC, next_run_at, created_at, id LIMIT 1") == urgent
        results["dense_prefix_admission_batches"] = page
        results["head_probe_observes_new_priority"] = True
        return {"server_version": version, "migration_sql_files_applied": len(paths),
                "source_baseline": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=True).stdout.strip(),
                "results": results,
                "limits": ["SQL protocol model, not Rust API integration", "scanner uses stable rows; production cursor deletion/rollback cases remain", "no rate implementation or throughput claim", "workflow lock order is modeled; full cancellation/recovery Rust paths remain"]}


def main():
    container = "runledger-capacity-plan-" + uuid.uuid4().hex[:10]
    docker("run", "--detach", "--rm", "--name", container,
           "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", "postgres:18")
    try:
        port = docker("port", container, "5432/tcp").rsplit(":", 1)[1]
        dsn = f"postgresql://postgres@127.0.0.1:{port}/postgres"
        deadline = time.monotonic() + 30
        while True:
            try:
                with psycopg.connect(dsn, connect_timeout=1):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        print(json.dumps(run(dsn), indent=2))
    finally:
        docker("rm", "--force", container)


if __name__ == "__main__":
    main()
