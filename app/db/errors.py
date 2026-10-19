"""Mapping of PostgreSQL failures to a controlled 503 (never a false success, never SQL or driver text)."""

import asyncpg
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from app.core.logging import logger

# A refused/reset connection surfaces from asyncpg as a plain OSError subclass, so SQLAlchemyError alone is not enough.
POSTGRES_UNAVAILABLE = (SQLAlchemyError, ConnectionError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError)


def register_postgres_handlers(app: FastAPI) -> None:
    async def handler(request, exc):
        logger.error("postgres_error", error_type=type(exc).__name__, path=request.url.path)
        return JSONResponse(status_code=503, content={"detail": {
            "code": "storage_unavailable",
            "message": "Settings storage is temporarily unavailable. Please retry."}})

    for exc_type in POSTGRES_UNAVAILABLE:
        app.add_exception_handler(exc_type, handler)
