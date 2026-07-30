"""
Structured Logging Configuration.
Sets up standard python logging integrated with structlog to output formatted JSON traces.
Automatically injects correlation_id from the request context into every log record.
"""

import logging
import sys
import structlog
from app.core.config import settings


def _inject_correlation_id(logger, method_name, event_dict):
    """
    Structlog processor: reads the current X-Correlation-ID from the ContextVar
    and injects it as `correlation_id` into every log record.
    Import is deferred to avoid circular import at module load time.
    """
    try:
        from app.middleware.correlation import get_correlation_id
        cid = get_correlation_id()
        if cid:
            event_dict["correlation_id"] = cid
    except Exception:
        pass  # Never let logging break the request
    return event_dict


def setup_logging():
    """
    Configures structural formatters and hooks for application-wide log traces.
    """
    # Structlog processors pipeline
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        _inject_correlation_id,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.ENVIRONMENT == "production":
        # Production JSON logs format
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Development clean color format
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Clean default handlers
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        processor=processors[-1]
    ))

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(logging.INFO if not settings.DEBUG else logging.DEBUG)

    # Disable spammy third-party library log outputs
    logging.getLogger("cassandra").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

setup_logging()
logger = structlog.get_logger("app")
