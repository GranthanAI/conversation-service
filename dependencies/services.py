from fastapi import Depends
from dependencies.repositories import get_conversation_repository
from dependencies.cache import get_conversation_cache
from infrastructure.cassandra.conversation_repository import CassandraConversationRepository
from infrastructure.redis.conversation_cache import ConversationCache
from services.conversation_service import ConversationService

def get_conversation_service(
    repo: CassandraConversationRepository = Depends(get_conversation_repository),
    cache: ConversationCache = Depends(get_conversation_cache)
) -> ConversationService:
    return ConversationService(repo=repo, cache=cache)
