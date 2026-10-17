"""
Correlation ID Middleware.
Propagates a request-scoped X-Correlation-ID header through every inbound HTTP request.
If the client does not supply one, a fresh UUID is generated.
The ID is stored in a ContextVar so any code in the call stack (services, repos, Kafka publish)
can access it without passing it as a parameter.
"""

import uuid
from contextvars import ContextVar
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Module-level ContextVar — holds the correlation ID for the current async task
_correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="")

CORRELATION_ID_HEADER = "X-Correlation-ID"


def get_correlation_id() -> str:
    """
    Returns the current request's correlation ID.
    Returns an empty string if called outside a request context.
    """
    return _correlation_id_ctx.get("")


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that reads or generates an X-Correlation-ID on every request
    and stores it in a ContextVar for downstream use.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        # Read from incoming header or mint a new one
        correlation_id = request.headers.get(CORRELATION_ID_HEADER) or str(uuid.uuid4())

        # Bind to the current async context
        token = _correlation_id_ctx.set(correlation_id)

        try:
            response: Response = await call_next(request)
        finally:
            # Always reset the ContextVar after the response, even on error
            _correlation_id_ctx.reset(token)

        # Echo the correlation ID back so clients can trace their request
        response.headers[CORRELATION_ID_HEADER] = correlation_id
        return response
