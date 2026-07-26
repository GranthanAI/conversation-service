"""
gRPC Client Initialization.
Sets up async channels with keepalive pings to interact with the LLM Service and probes connectivity.
"""

from typing import Optional
import grpc
from app.core.config import settings
from app.core.logging import logger

class GRPCClientManager:
    """
    Manages channel connection lifecycle to downstream gRPC services.
    """
    def __init__(self):
        self.channel: Optional[grpc.aio.Channel] = None

    async def initialize(self):
        """
        Creates the asynchronous insecure channel with keepalive configurations.
        """
        try:
            logger.info("Initializing gRPC client manager with keepalives...",
                        endpoint=settings.LLM_SERVICE_GRPC_ENDPOINT)
            
            # Configure keepalive parameters as per LLD §12
            options = [
                ('grpc.keepalive_time_ms', 20000),          # Ping every 20s
                ('grpc.keepalive_timeout_ms', 10000),       # 10s ping timeout
                ('grpc.keepalive_permit_without_calls', 1), # Allow keepalive without active streams
                ('grpc.http2.max_pings_without_data', 0)    # Unbounded pings
            ]
            
            self.channel = grpc.aio.insecure_channel(
                settings.LLM_SERVICE_GRPC_ENDPOINT,
                options=options
            )
            logger.info("gRPC channel client instantiated successfully.")
        except Exception as e:
            logger.error("Failed to initialize gRPC channel", error=str(e))
            self.channel = None

    async def check_health(self) -> bool:
        """
        Verifies if gRPC channels are ready to receive stream connections.
        """
        if not self.channel:
            logger.info("gRPC channel inactive, attempting lazy initialization...")
            await self.initialize()
        if not self.channel:
            return False
        try:
            # Wait for channel readiness with a brief timeout
            await grpc.aio.channel_ready(self.channel)
            return True
        except Exception as e:
            logger.warning("gRPC server health check failed", error=str(e))
            return False

    async def close(self):
        """
        Closes connection channels cleanly.
        """
        if self.channel:
            try:
                await self.channel.close()
                logger.info("gRPC channel closed successfully.")
            except Exception as e:
                logger.error("Error closing gRPC channel", error=str(e))
            self.channel = None

grpc_manager = GRPCClientManager()
