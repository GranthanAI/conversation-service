"""
FastAPI Main Application Entrypoint.
Initializes the FastAPI application instance, registers lifespan event handlers,
defines exception handlers, and mounts routing modules.
"""

# --- Python 3.12 Compatibility Patch for Cassandra Driver ---
import sys
import types
asyncore_mock = types.ModuleType("asyncore")
class DummyDispatcher:
    pass
asyncore_mock.dispatcher = DummyDispatcher
sys.modules['asyncore'] = asyncore_mock
# -------------------------------------------------------------

from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, status
from app.db.errors import register_postgres_handlers
from app.core.config import settings
import asyncio
from app.core.logging import logger
from app.db.cassandra import cassandra_manager
from app.db.postgres import check_database, dispose_engine, pool_stats
from app.db.redis import redis_manager
from app.db.kafka import kafka_manager
from app.api.router import api_router
from app.events.consumers import start_kafka_consumer
from app.workers.outbox_worker import start_outbox_worker
from app.workers.cleanup_worker import start_cleanup_worker
from app.workers.retry_worker import start_retry_worker
from app.workers.summary_worker import start_summary_worker
from app.workers.pending_timeout_worker import start_pending_timeout_worker
from app.middleware.correlation import CorrelationIDMiddleware
from app.artifacts.runtime import build_export_service, get_artifact_service, get_object_store
from app.artifacts.service import ArtifactWorker
from app.exports.service import ExportWorker

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager managing start and shutdown lifecycles of database clients and workers.
    """
    # 1. Startup phase
    logger.info("Starting up FastAPI application lifespan context...")
    cassandra_manager.initialize()
    redis_manager.initialize()
    await kafka_manager.initialize()

    # Spawn background consumer and outbox tasks
    app.state.kafka_consumer_task = asyncio.create_task(start_kafka_consumer())
    app.state.outbox_worker_task = asyncio.create_task(start_outbox_worker())
    app.state.cleanup_worker_task = asyncio.create_task(start_cleanup_worker())
    app.state.retry_worker_task = asyncio.create_task(start_retry_worker())
    app.state.summary_worker_task = asyncio.create_task(start_summary_worker())
    app.state.pending_timeout_worker_task = asyncio.create_task(start_pending_timeout_worker())

    # Artifact registry + export workers (PostgreSQL state, private object storage). A storage outage at startup is
    # logged, not fatal: the workers retry, and every other endpoint keeps working.
    app.state.artifact_tasks = []
    try:
        await get_object_store().ensure_ready()
    except Exception as exc:
        logger.error("artifact object store unavailable at startup", error_type=type(exc).__name__)
    if settings.ARTIFACT_WORKER_ENABLED:
        app.state.artifact_tasks.append(asyncio.create_task(
            ArtifactWorker(get_artifact_service(), settings.ARTIFACT_WORKER_INTERVAL_SECONDS).run()))
    if settings.EXPORT_WORKER_ENABLED:
        app.state.artifact_tasks.append(asyncio.create_task(
            ExportWorker(build_export_service(), settings.EXPORT_WORKER_INTERVAL_SECONDS).run()))

    yield
    # 2. Shutdown phase
    logger.info("Shutting down FastAPI application lifespan context...")

    # Cancel tasks
    app.state.kafka_consumer_task.cancel()
    app.state.outbox_worker_task.cancel()
    app.state.cleanup_worker_task.cancel()
    app.state.retry_worker_task.cancel()
    app.state.summary_worker_task.cancel()
    app.state.pending_timeout_worker_task.cancel()
    for task in app.state.artifact_tasks:
        task.cancel()
    try:
        await asyncio.gather(
            *app.state.artifact_tasks,
            app.state.kafka_consumer_task,
            app.state.outbox_worker_task,
            app.state.cleanup_worker_task,
            app.state.retry_worker_task,
            app.state.summary_worker_task,
            app.state.pending_timeout_worker_task,
            return_exceptions=True
        )
    except asyncio.CancelledError:
        pass

    cassandra_manager.close()
    await dispose_engine()
    await redis_manager.close()
    await kafka_manager.close()

app = FastAPI(
    title="GraphGPT Conversation Service",
    description="Backend microservice managing conversations, message states, and token streaming streams.",
    version="1.0.0",
    lifespan=lifespan
)

# Middleware: Correlation ID must be first so all downstream code can read it
app.add_middleware(CorrelationIDMiddleware)

register_postgres_handlers(app)

# Mount central API routing prefixes
app.include_router(api_router, prefix="/v1")


# Global health check redirects for standard paths
@app.get("/live", status_code=status.HTTP_200_OK, tags=["Health Checks"])
async def root_liveness():
    """
    Liveness probe check at root.
    """
    return {"status": "UP", "message": "Service process is alive."}

@app.get("/ready", status_code=status.HTTP_200_OK, tags=["Health Checks"])
async def root_readiness(response: Response):
    """
    Readiness probe check at root.
    """
    cassandra_ok = cassandra_manager.check_health()
    redis_ok = await redis_manager.check_health()
    kafka_ok = await kafka_manager.check_health()
    postgres_ok = await check_database()

    status_info = {
        "postgres": "UP" if postgres_ok else "DOWN",
        "cassandra": "UP" if cassandra_ok else "DOWN",
        "redis": "UP" if redis_ok else "DOWN",
        "kafka": "UP" if kafka_ok else "DOWN",
    }

    if not (cassandra_ok and redis_ok and kafka_ok and postgres_ok):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "DOWN", "details": status_info}
        
    return {"status": "UP", "details": status_info}

@app.get("/", tags=["General"])
async def root():
    """
    Redirects root index request.
    """
    return {"message": "Welcome to GraphGPT Conversation Service. Visit /docs for Swagger UI documentation."}
