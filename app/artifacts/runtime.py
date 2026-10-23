"""Process-wide wiring for the artifact registry and export services (lazy singletons; tests override the FastAPI
dependencies instead of touching these)."""

from typing import Optional

from app.artifacts.render_backend import LlmRenderBackend
from app.artifacts.service import ArtifactService
from app.artifacts.storage import MinioObjectStore, ObjectStore
from app.core.config import settings
from app.db.session import get_session_factory

_store: Optional[ObjectStore] = None
_service: Optional[ArtifactService] = None


def get_object_store() -> ObjectStore:
    global _store
    if _store is None:
        _store = MinioObjectStore(settings.ARTIFACT_STORE_ENDPOINT, settings.ARTIFACT_STORE_ACCESS_KEY,
                                  settings.ARTIFACT_STORE_SECRET_KEY, settings.ARTIFACT_STORE_BUCKET,
                                  secure=settings.ARTIFACT_STORE_SECURE, region=settings.ARTIFACT_STORE_REGION)
    return _store


def get_artifact_service() -> ArtifactService:
    global _service
    if _service is None:
        _service = ArtifactService(get_session_factory(), get_object_store(), LlmRenderBackend())
    return _service


def build_export_service():
    """A long-lived ExportService for the background worker (requests build their own through FastAPI dependencies)."""
    from app.clients.kafka_producer import kafka_producer_client
    from app.db.redis import redis_manager
    from app.exports.service import ExportService
    from app.repositories.conversation_repository import CassandraConversationRepository
    from app.repositories.message_repository import CassandraMessageRepository
    from app.services.cache_service import CacheService
    from app.services.conversation_service import ConversationService

    conversations = ConversationService(repo=CassandraConversationRepository(),
                                        cache_service=CacheService(redis_client=redis_manager.client),
                                        producer_client=kafka_producer_client)
    return ExportService(get_session_factory(), get_object_store(), conversations, CassandraMessageRepository(),
                         get_artifact_service())
