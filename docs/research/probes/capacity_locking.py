"""Admission primitive experiment, NOT Runledger integration or a benchmark.

Start a disposable postgres:18 container, then pass its name to this script.
Only use a disposable database: this replaces the capacity_probe schema.
Requires Python 3 and docker; PostgreSQL client runs inside the container.
"""

import concurrent.futures
import json
import subprocess
import sys
import time


container = sys.argv[1]
base = ["docker", "exec", "-i", container, "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres"]


def sql(statement):
    return subprocess.run(base, input=statement, text=True, capture_output=True, check=True).stdout.strip()


version = sql("SHOW server_version;")
assert int(sql("SHOW server_version_num;")) // 10000 == 18
sql("""
DROP SCHEMA IF EXISTS capacity_probe CASCADE;
CREATE SCHEMA capacity_probe;
CREATE TABLE capacity_probe.policy (id int PRIMARY KEY, capacity int NOT NULL);
INSERT INTO capacity_probe.policy VALUES (1, 20);
CREATE TABLE capacity_probe.permit (id int PRIMARY KEY, policy_id int NOT NULL);
""")

# Hold a barrier until every unsafe claimant has read the same empty state.
barrier = subprocess.Popen(base, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
barrier.stdin.write("BEGIN; SELECT pg_advisory_xact_lock(982341); SELECT 'ready';\n")
barrier.stdin.flush()
while barrier.stdout.readline().strip() != "ready":
    if barrier.poll() is not None:
        raise RuntimeError("barrier connection exited")


def unsafe_claim(i):
    return sql(fr"""
SET application_name = 'capacity_probe_unsafe';
BEGIN;
SELECT count(*) AS occupied FROM capacity_probe.permit WHERE policy_id = 1 \gset
SELECT pg_advisory_xact_lock_shared(982341);
INSERT INTO capacity_probe.permit SELECT {i}, 1 WHERE :occupied < 20;
COMMIT;
""")


try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(unsafe_claim, i) for i in range(32)]
        try:
            deadline = time.monotonic() + 30
            while int(sql("""
                SELECT count(*) FROM pg_stat_activity
                WHERE application_name = 'capacity_probe_unsafe'
                  AND wait_event = 'advisory';
            """)) != 32:
                if time.monotonic() > deadline:
                    raise TimeoutError("not all unsafe claimants reached the barrier")
                time.sleep(0.05)
        finally:
            barrier.communicate("COMMIT;\n", timeout=10)
        for future in futures:
            future.result()
finally:
    if barrier.poll() is None:
        barrier.kill()
        barrier.wait()

unsafe_count = int(sql("SELECT count(*) FROM capacity_probe.permit;"))
assert unsafe_count == 32, unsafe_count
sql("TRUNCATE capacity_probe.permit;")


def safe_claim(i):
    # The count is a SEPARATE statement after locking the stable policy row.
    return sql(f"""
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT id FROM capacity_probe.policy WHERE id = 1 FOR UPDATE;
INSERT INTO capacity_probe.permit
SELECT {i}, 1
WHERE (SELECT count(*) FROM capacity_probe.permit WHERE policy_id = 1)
    < (SELECT capacity FROM capacity_probe.policy WHERE id = 1);
COMMIT;
""")


with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
    list(executor.map(safe_claim, range(32)))
safe_count = int(sql("SELECT count(*) FROM capacity_probe.permit;"))
assert safe_count == 20, safe_count

# A failed admission must leave no permit, even after writing it.
sql("""
BEGIN;
SELECT id FROM capacity_probe.policy WHERE id = 1 FOR UPDATE;
INSERT INTO capacity_probe.permit VALUES (1000, 1);
ROLLBACK;
""")
assert int(sql("SELECT count(*) FROM capacity_probe.permit;")) == 20
print(json.dumps({
    "server_version": version,
    "claimants": 32,
    "capacity": 20,
    "unserialized_permits": unsafe_count,
    "serialized_permits": safe_count,
    "rollback_preserved_count": True,
}, indent=2))
