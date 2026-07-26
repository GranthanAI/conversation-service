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
from app.core.config import settings
from app.core.logging import logger
from app.db.cassandra import cassandra_manager
from app.db.redis import redis_manager
from app.db.kafka import kafka_manager
from app.db.grpc import grpc_manager
from app.api.router import api_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager managing start and shutdown lifecycles of database clients.
    """
    # 1. Startup phase
    logger.info("Starting up FastAPI application lifespan context...")
    cassandra_manager.initialize()
    redis_manager.initialize()
    await kafka_manager.initialize()
    await grpc_manager.initialize()
    yield
    # 2. Shutdown phase
    logger.info("Shutting down FastAPI application lifespan context...")
    cassandra_manager.close()
    await redis_manager.close()
    await kafka_manager.close()
    await grpc_manager.close()

app = FastAPI(
    title="GraphGPT Conversation Service",
    description="Backend microservice managing conversations, message states, and token streaming streams.",
    version="1.0.0",
    lifespan=lifespan
)

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
    grpc_ok = await grpc_manager.check_health()
    
    status_info = {
        "cassandra": "UP" if cassandra_ok else "DOWN",
        "redis": "UP" if redis_ok else "DOWN",
        "kafka": "UP" if kafka_ok else "DOWN",
        "grpc": "UP" if grpc_ok else "DOWN"
    }
    
    if not (cassandra_ok and redis_ok and kafka_ok and grpc_ok):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "DOWN", "details": status_info}
        
    return {"status": "UP", "details": status_info}

@app.get("/", tags=["General"])
async def root():
    """
    Redirects root index request.
    """
    return {"message": "Welcome to GraphGPT Conversation Service. Visit /docs for Swagger UI documentation."}
