"""
Main API Presentation Router.
Aggregates versioned endpoints under single routing namespaces.
"""

from fastapi import APIRouter
from app.api.v1.health import router as health_router
from app.api.v1.conversations import router as conversations_router
from app.api.v1.messages import router as messages_router

api_router = APIRouter()

# Register v1 routes
api_router.include_router(health_router, prefix="/health", tags=["Health Checks"])
api_router.include_router(conversations_router, prefix="/conversations", tags=["Conversations"])
api_router.include_router(messages_router, prefix="/conversations", tags=["Messages"])
