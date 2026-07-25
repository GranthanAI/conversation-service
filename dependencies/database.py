"""
Database Dependency Provider.
Instantiates the Cassandra cluster client manager using configurations
imported from the central Settings model.
"""

from core.config import settings
from infrastructure.cassandra.client import CassandraClientManager

# Singleton client manager using actual environment configuration settings
cassandra_manager = CassandraClientManager(
    contact_points=settings.CASSANDRA_CONTACT_POINTS.split(","),
    port=settings.CASSANDRA_PORT,
    keyspace=settings.CASSANDRA_KEYSPACE,
    local_dc=settings.CASSANDRA_LOCAL_DC,
    username=settings.CASSANDRA_USERNAME,
    password=settings.CASSANDRA_PASSWORD,
    timeout=settings.CASSANDRA_TIMEOUT_SECONDS
)

def get_cassandra_manager() -> CassandraClientManager:
    """
    Returns the initialized Cassandra client manager singleton.
    """
    return cassandra_manager
