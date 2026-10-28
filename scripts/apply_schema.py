"""Wait for Cassandra, then apply the configured CREATE KEYSPACE followed by app/db/schema.cql via cqlsh.

Usage (from `make schema`):  uv run python scripts/apply_schema.py [container-name]
"""
import subprocess
import sys
import time
from pathlib import Path

from app.db.keyspace import effective_replication_factor, keyspace_cql
from app.core.config import settings

CONTAINER = sys.argv[1] if len(sys.argv) > 1 else "graphgpt-cassandra"
SCHEMA = Path(__file__).resolve().parent.parent / "app" / "db" / "schema.cql"


def main() -> int:
    ks = keyspace_cql(
        settings.CASSANDRA_KEYSPACE,
        settings.CASSANDRA_REPLICATION_STRATEGY,
        settings.CASSANDRA_LOCAL_DC,
        effective_replication_factor(settings.CASSANDRA_REPLICATION_FACTOR, settings.ENVIRONMENT),
    )
    print("Waiting for Cassandra CQL port 9042 to accept connections...")
    for _ in range(30):
        probe = subprocess.run(["docker", "exec", "-i", CONTAINER, "cqlsh", "-e", "DESCRIBE KEYSPACES"], capture_output=True)
        if probe.returncode == 0:
            cql = ks + "\n\n" + SCHEMA.read_text(encoding="utf-8")
            return subprocess.run(["docker", "exec", "-i", CONTAINER, "cqlsh"], input=cql.encode("utf-8")).returncode
        time.sleep(2)
    print("Error: Cassandra timed out after 60s.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
