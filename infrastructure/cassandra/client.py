"""
Infrastructure Cassandra Client.
Manages connections to Apache Cassandra cluster setups, constructing token-aware,
DC-aware policies, opening keyspace sessions, and handling shutdown sweeps.
"""

import logging
from typing import Optional, List
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy

logger = logging.getLogger(__name__)

class CassandraClientManager:
    """
    Manages connections and session mappings to an Apache Cassandra keyspace.
    """
    def __init__(
        self,
        contact_points: List[str],
        port: int = 9042,
        keyspace: str = "graphgpt_conversations",
        local_dc: Optional[str] = "datacenter1",
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 5.0
    ):
        self.contact_points = contact_points
        self.port = port
        self.keyspace = keyspace
        self.local_dc = local_dc
        self.username = username
        self.password = password
        self.timeout = timeout
        
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None

    def initialize(self) -> None:
        """
        Builds the Cluster configuration and initializes a Session to the keyspace.
        """
        try:
            auth_provider = None
            if self.username and self.password:
                auth_provider = PlainTextAuthProvider(username=self.username, password=self.password)

            # Load balancing policy setup
            lb_policy = None
            if self.local_dc:
                lb_policy = TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=self.local_dc))

            self.cluster = Cluster(
                contact_points=self.contact_points,
                port=self.port,
                auth_provider=auth_provider,
                load_balancing_policy=lb_policy,
                connect_timeout=self.timeout
            )
            
            # Connect and select keyspace
            self.session = self.cluster.connect(self.keyspace)
            self.session.default_timeout = self.timeout
            logger.info(f"Connected to Cassandra cluster at {self.contact_points}, keyspace: {self.keyspace}")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra cluster: {e}")
            self.cluster = None
            self.session = None

    def close(self) -> None:
        """
        Shuts down the cluster connections cleanly.
        """
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Closed Cassandra cluster connections.")
