"""
gRPC Downstream Service Client.
Exposes stream generation hooks mapping to the LLM Service channel.
"""

import asyncio
from typing import AsyncGenerator
from uuid import UUID
import grpc

from app.db.grpc import grpc_manager
from app.core.config import settings
from app.core.logging import logger
from app.events.generation_pb2 import GenerationRequest, TokenChunk
from app.events.generation_pb2_grpc import GenerationServiceStub

class GRPCGenerationClient:
    """
    Wrapper around the downstream gRPC channel to execute token stream generations.
    Supports exponential backoff retry pre-first-byte and chunk-level deadline resets.
    """
    def __init__(self):
        self.manager = grpc_manager

    def get_stub(self) -> GenerationServiceStub:
        """
        Returns compiled generation stub linked to the active channel.
        """
        if not self.manager.channel:
            logger.error("gRPC client channel is inactive.")
            raise RuntimeError("gRPC channel not running")
        return GenerationServiceStub(self.manager.channel)

    async def generate(
        self,
        conversation_id: UUID,
        message_id: UUID,
        prompt_context: str
    ) -> AsyncGenerator[dict, None]:
        """
        Calls the downstream LLM gRPC service.
        Implements 3 retries with exponential backoff (1s, 2s, 4s) only safe pre-first-byte.
        Implements deadline of 60s between chunks, reset on each chunk.
        """
        req = GenerationRequest(
            conversation_id=str(conversation_id),
            message_id=str(message_id),
            prompt_context=prompt_context
        )

        attempts = settings.GRPC_RETRY_ATTEMPTS
        backoff = settings.GRPC_RETRY_BACKOFF_BASE
        first_byte_received = False
        
        for attempt in range(attempts):
            try:
                stub = self.get_stub()
                # Run with large overall timeout, but enforce chunk-level timeout
                stream = stub.Generate(req, timeout=settings.GRPC_STREAM_TIMEOUT_SECONDS)
                iterator = stream.__aiter__()
                
                while True:
                    try:
                        chunk = await asyncio.wait_for(iterator.__anext__(), timeout=settings.GRPC_CHUNK_TIMEOUT_SECONDS)
                        first_byte_received = True
                        yield {
                            "conversation_id": str(conversation_id),
                            "message_id": chunk.message_id,
                            "sender": "assistant",
                            "content": chunk.chunk,
                            "is_final": chunk.is_final
                        }
                    except StopAsyncIteration:
                        break
                    except asyncio.TimeoutError:
                        logger.error(f"gRPC stream chunk deadline exceeded ({settings.GRPC_CHUNK_TIMEOUT_SECONDS}s).")
                        raise grpc.RpcError("Stream chunk deadline exceeded")
                # Successfully completed
                return
            except (grpc.RpcError, Exception) as e:
                # If we've already received some tokens, do NOT retry
                if first_byte_received:
                    logger.error("gRPC stream failed mid-generation; aborting.", error=str(e))
                    raise
                
                logger.warning(
                    f"gRPC stream connect attempt {attempt + 1} failed. Retrying in {backoff}s...",
                    error=str(e)
                )
                if attempt + 1 < attempts:
                    await asyncio.sleep(backoff)
                    backoff *= 2.0
                else:
                    logger.error("gRPC stream retries exhausted. Aborting.")
                    raise

grpc_generation_client = GRPCGenerationClient()
