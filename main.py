"""
FastAPI Entrypoint File.
Initializes the FastAPI application, mounts router paths, configures exception handlers
for base domain logic errors, and links the asynchronous application lifespan events.
"""

import sys
import types

# -------------------------------------------------------------
# Python 3.12 Compatibility Hack for DataStax Cassandra Driver
# -------------------------------------------------------------
# DataStax driver attempts to load default 'asyncore' module, 
# which was removed in Python 3.12. We construct and register
# a mock module here to allow safe imports.
# -------------------------------------------------------------
asyncore_mock = types.ModuleType("asyncore")
class DummyDispatcher:
    pass
asyncore_mock.dispatcher = DummyDispatcher
sys.modules['asyncore'] = asyncore_mock

# Configure driver connection reactor to use asyncio
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
Cluster.connection_class = AsyncioConnection
# -------------------------------------------------------------

import logging
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from lifespan import lifespan
from api.routers.conversations import router as conversations_router
from domain.exceptions import NotFoundError, OwnershipError, ValidationError


# Set up logging format
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Initialize FastAPI App with lifespan context
app = FastAPI(
    title="Conversation Service",
    description="Production-Scale GraphGPT Conversation Orchestration Service",
    version="1.0.0",
    lifespan=lifespan
)

# Exception handlers mapping domain exceptions to HTTP responses
@app.exception_handler(NotFoundError)
async def not_found_exception_handler(request: Request, exc: NotFoundError):
    """Handles domain NotFoundError exceptions and outputs 404 response."""
    logger.warning(f"Resource not found error occurred: {exc.message}")
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": {"code": "NOT_FOUND", "message": exc.message}}
    )

@app.exception_handler(OwnershipError)
async def ownership_exception_handler(request: Request, exc: OwnershipError):
    """Handles domain OwnershipError exceptions and outputs 403 response."""
    logger.warning(f"Ownership/Access error occurred: {exc.message}")
    return JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content={"error": {"code": "FORBIDDEN", "message": exc.message}}
    )

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    """Handles domain ValidationError exceptions and outputs 422 response."""
    logger.warning(f"Validation error occurred: {exc.message}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"error": {"code": "VALIDATION_FAILED", "message": exc.message}}
    )

# Include conversations router endpoints
app.include_router(conversations_router)

@app.get("/health", tags=["Health"])
async def health_check():
    """Simple API health probe."""
    return {"status": "healthy"}
