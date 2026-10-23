"""
Main API Presentation Router.
Aggregates versioned endpoints under single routing namespaces.
"""

from fastapi import APIRouter
from app.api.v1.health import router as health_router
from app.api.v1.conversations import router as conversations_router
from app.api.v1.messages import router as messages_router
from app.api.v1.stream import router as stream_router
from app.api.v1.preferences import router as preferences_router
from app.api.v1.custom_modes import router as custom_modes_router
from app.api.v1.artifacts import router as artifacts_router
from app.api.v1.notes import router as notes_router
from app.api.v1.exports import router as exports_router

api_router = APIRouter()

# Register v1 routes
api_router.include_router(health_router, prefix="/health", tags=["Health Checks"])
api_router.include_router(conversations_router, prefix="/conversations", tags=["Conversations"])
api_router.include_router(messages_router, prefix="/conversations", tags=["Messages"])
api_router.include_router(stream_router, prefix="/stream", tags=["SSE Stream"])
api_router.include_router(preferences_router, prefix="/preferences", tags=["Preferences"])
api_router.include_router(custom_modes_router, prefix="/custom-modes", tags=["Custom modes"])
api_router.include_router(artifacts_router, prefix="/artifacts", tags=["Artifacts"])
api_router.include_router(exports_router, prefix="/exports", tags=["Exports"])
api_router.include_router(notes_router, prefix="/conversations", tags=["Notes"])
