"""
Lifespan Management File.
Handles startup initialization and shutdown cleanup of system connections,
including the Cassandra database session and the Redis cluster client.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from dependencies.database import get_cassandra_manager
from dependencies.cache import get_redis_manager

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager for FastAPI lifespan events.
    Initializes external resource connection managers on startup,
    and cleanly closes connections on shutdown.
    """
    logger.info("Starting up FastAPI application lifespan context...")
    
    # 1. Initialize Cassandra connection pool
    cassandra_manager = get_cassandra_manager()
    try:
        cassandra_manager.initialize()
        logger.info("Cassandra client manager initialized successfully.")
    except Exception as e:
        logger.error(f"Critical error initializing Cassandra client: {e}")

    # 2. Initialize Redis connection pool
    redis_manager = get_redis_manager()
    try:
        await redis_manager.initialize()
        logger.info("Redis client manager initialized successfully.")
    except Exception as e:
        logger.error(f"Critical error initializing Redis client: {e}")

    yield

    logger.info("Shutting down FastAPI application lifespan context...")
    
    # 3. Clean up Redis connections
    try:
        await redis_manager.close()
        logger.info("Redis client closed.")
    except Exception as e:
        logger.error(f"Error closing Redis client: {e}")

    # 4. Clean up Cassandra connections
    try:
        cassandra_manager.close()
        logger.info("Cassandra client closed.")
    except Exception as e:
        logger.error(f"Error closing Cassandra client: {e}")
