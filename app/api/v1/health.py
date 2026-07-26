"""
Health Probe Routers.
Exposes endpoints to assess process status and dependencies connectivity.
"""

from fastapi import APIRouter, Response, status
from app.db.cassandra import cassandra_manager
from app.db.redis import redis_manager
from app.db.kafka import kafka_manager
from app.db.grpc import grpc_manager

router = APIRouter()

@router.get("/live", status_code=status.HTTP_200_OK, summary="Liveness check")
async def liveness_check():
    """
    Indicates if the FastAPI application process is up and running.
    """
    return {"status": "UP", "message": "Service process is alive."}

@router.get("/ready", status_code=status.HTTP_200_OK, summary="Readiness check")
async def readiness_check(response: Response):
    """
    Verifies functional socket connections to all database and message brokers.
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
