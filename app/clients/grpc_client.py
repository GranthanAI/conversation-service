"""
gRPC Downstream Service Client.
Exposes stream generation hooks mapping to the LLM Service channel.
"""

from typing import Any
from app.db.grpc import grpc_manager
from app.core.logging import logger

class GRPCGenerationClient:
    """
    Wrapper around the downstream gRPC channel to execute token stream generations.
    """
    def __init__(self):
        self.manager = grpc_manager

    def get_stub(self) -> Any:
        """
        Returns compiled generation stub linked to the active channel.
        """
        if not self.manager.channel:
            logger.error("gRPC client channel is inactive.")
            raise RuntimeError("gRPC channel not running")
        # Linked compilation stub placeholder (resolved once protobuf files are generated)
        return None

grpc_generation_client = GRPCGenerationClient()
