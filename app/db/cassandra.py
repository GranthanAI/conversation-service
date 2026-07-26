"""
Cassandra Client Connection Manager.
Handles session pools creation, keyspace declarations, and connection lifecycle checks.
"""

from typing import Optional
from cassandra.cluster import Cluster, Session
from cassandra.io.asyncioreactor import AsyncioConnection
Cluster.connection_class = AsyncioConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from app.core.config import settings
from app.core.logging import logger

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

            self.cluster = Cluster(
                contact_points=contact_points,
                port=settings.CASSANDRA_PORT,
                auth_provider=auth_provider,
                load_balancing_policy=lb_policy,
                connect_timeout=settings.CASSANDRA_TIMEOUT_SECONDS
            )
            # Establish session bound to keyspace
            self.session = self.cluster.connect(settings.CASSANDRA_KEYSPACE)
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
