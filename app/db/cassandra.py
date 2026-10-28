"""
Cassandra Client Connection Manager.
Handles session pools creation, keyspace declarations, and connection lifecycle checks.
"""

from typing import Optional
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement, Statement
from cassandra.io.asyncioreactor import AsyncioConnection
Cluster.connection_class = AsyncioConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from app.core.config import settings
from app.core.logging import logger
from app.db.keyspace import is_read_query


class ConsistencySession:
    """Thin proxy over a driver Session that applies CASSANDRA_CONSISTENCY_READ to SELECTs.

    Everything else (writes, batches, LWTs) uses the cluster's default execution profile, which carries
    CASSANDRA_CONSISTENCY_WRITE and CASSANDRA_SERIAL_CONSISTENCY. All other attributes are delegated unchanged."""

    def __init__(self, session: Session, read_cl: int):
        self._session = session
        self._read_cl = read_cl

    def __getattr__(self, name):
        return getattr(self._session, name)

    def _query_text(self, query) -> str:
        if isinstance(query, str):
            return query
        text = getattr(query, "query_string", None)
        if text is None and getattr(query, "prepared_statement", None) is not None:
            text = query.prepared_statement.query_string
        return text or ""

    def _apply(self, query):
        if isinstance(query, str):
            return SimpleStatement(query, consistency_level=self._read_cl) if is_read_query(query) else query
        if isinstance(query, Statement) and query.consistency_level is None and is_read_query(self._query_text(query)):
            query.consistency_level = self._read_cl
        return query

    def prepare(self, query, *args, **kwargs):
        prepared = self._session.prepare(query, *args, **kwargs)
        if is_read_query(self._query_text(prepared)):
            prepared.consistency_level = self._read_cl
        return prepared

    def execute(self, query, *args, **kwargs):
        return self._session.execute(self._apply(query), *args, **kwargs)

    def execute_async(self, query, *args, **kwargs):
        return self._session.execute_async(self._apply(query), *args, **kwargs)

class CassandraClientManager:
    """
    Manages the cluster connectivity and session query interfaces for Cassandra.
    """
    def __init__(self):
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None

    def initialize(self):
        """
        Attempts connection setup and creates prepared sessions pool.
        """
        try:
            auth_provider = None
            if settings.CASSANDRA_USERNAME and settings.CASSANDRA_PASSWORD:
                auth_provider = PlainTextAuthProvider(
                    username=settings.CASSANDRA_USERNAME,
                    password=settings.CASSANDRA_PASSWORD
                )

            lb_policy = DCAwareRoundRobinPolicy(local_dc=settings.CASSANDRA_LOCAL_DC)
            contact_points = [x.strip() for x in settings.CASSANDRA_CONTACT_POINTS.split(",")]

            logger.info("Initializing Cassandra connection pool...",
                        hosts=contact_points, port=settings.CASSANDRA_PORT)

            profile = ExecutionProfile(
                load_balancing_policy=lb_policy,
                consistency_level=ConsistencyLevel.name_to_value[settings.CASSANDRA_CONSISTENCY_WRITE],
                serial_consistency_level=ConsistencyLevel.name_to_value[settings.CASSANDRA_SERIAL_CONSISTENCY],
            )
            self.cluster = Cluster(
                contact_points=contact_points,
                port=settings.CASSANDRA_PORT,
                auth_provider=auth_provider,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                connect_timeout=settings.CASSANDRA_TIMEOUT_SECONDS
            )
            # Establish session bound to keyspace
            self.session = ConsistencySession(
                self.cluster.connect(settings.CASSANDRA_KEYSPACE),
                ConsistencyLevel.name_to_value[settings.CASSANDRA_CONSISTENCY_READ],
            )
            logger.info("Cassandra database session established successfully.")
        except Exception as e:
            logger.error("Failed to connect to Cassandra cluster", error=str(e))
            self.session = None

    def check_health(self) -> bool:
        """
        Runs simple CQL select to check database online status.
        """
        if not self.session:
            logger.info("Cassandra session inactive, attempting lazy reconnection check...")
            self.initialize()
        if not self.session:
            return False
        try:
            self.session.execute("SELECT release_version FROM system.local")
            return True
        except Exception as e:
            logger.warning("Cassandra health probe check failed", error=str(e))
            return False

    def close(self):
        """
        Clears the session and cluster connections cleanly.
        """
        if self.cluster:
            try:
                self.cluster.shutdown()
                logger.info("Cassandra client cluster shutdown completed.")
            except Exception as e:
                logger.error("Error shutting down Cassandra cluster", error=str(e))

cassandra_manager = CassandraClientManager()
