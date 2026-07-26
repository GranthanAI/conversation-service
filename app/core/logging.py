"""
Structured Logging Configuration.
Sets up standard python logging integrated with structlog to output formatted JSON traces.
"""

import logging
import sys
import structlog
from app.core.config import settings

def setup_logging():
    """
    Configures structural formatters and hooks for application-wide log traces.
    """
    # Structlog processors pipeline
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
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
