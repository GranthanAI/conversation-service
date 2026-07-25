from fastapi import Depends
from dependencies.database import get_cassandra_manager, CassandraClientManager
from infrastructure.cassandra.conversation_repository import CassandraConversationRepository

def get_conversation_repository(
    manager: CassandraClientManager = Depends(get_cassandra_manager)
) -> CassandraConversationRepository:
    return CassandraConversationRepository(client_manager=manager)
