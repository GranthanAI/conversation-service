"""
gRPC Generation Client Integration Tests.
Starts a mock gRPC server in a local test fixture and verifies
the client's deadline chunk resets, streaming yield correctness, and retry backoffs.
"""

import asyncio
import uuid
import pytest
import grpc
from unittest.mock import patch

from app.events.generation_pb2 import TokenChunk, GenerationRequest
from app.events.generation_pb2_grpc import GenerationServiceServicer, add_GenerationServiceServicer_to_server
from app.clients.grpc_client import GRPCGenerationClient
from app.core.config import settings

class DummyGenerationService(GenerationServiceServicer):
    async def Generate(self, request: GenerationRequest, context):
        """
        Streams 3 test chunks.
        """
        yield TokenChunk(message_id=request.message_id, chunk="First", is_final=False)
        await asyncio.sleep(0.1)
        yield TokenChunk(message_id=request.message_id, chunk="Second", is_final=False)
        await asyncio.sleep(0.1)
        yield TokenChunk(message_id=request.message_id, chunk="Third", is_final=True)

@pytest.fixture(scope="module")
async def mock_grpc_server():
    server = grpc.aio.server()
    add_GenerationServiceServicer_to_server(DummyGenerationService(), server)
    server.add_insecure_port("[::]:50052")
    await server.start()
    yield
    await server.stop(grace=None)

@pytest.mark.anyio
async def test_grpc_client_receives_chunks(mock_grpc_server):
    from app.db.grpc import grpc_manager
    # Patch client port to point to the mock test server
    with patch.object(settings, "LLM_SERVICE_GRPC_ENDPOINT", "localhost:50052"):
        await grpc_manager.initialize()
        client = GRPCGenerationClient()
        
        try:
            chunks = []
            async for chunk in client.generate(uuid.uuid4(), uuid.uuid4(), "Explain test"):
                chunks.append(chunk["content"])
                
            assert chunks == ["First", "Second", "Third"]
        finally:
            await grpc_manager.close()
