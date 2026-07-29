"""
Mock Downstream LLM Service (gRPC).
Implements the GenerationService protobuf contract to stream token chunks
with simulated latency for local E2E verification.
"""

import asyncio
from concurrent import futures
import grpc

from app.events.generation_pb2 import TokenChunk, GenerationRequest
from app.events.generation_pb2_grpc import GenerationServiceServicer, add_GenerationServiceServicer_to_server
from app.core.logging import logger

class MockGenerationService(GenerationServiceServicer):
    async def Generate(self, request: GenerationRequest, context):
        """
        Streams simulated AI response chunks back to the client.
        """
        logger.info(
            "Mock LLM Service received generation request",
            conversation_id=request.conversation_id,
            message_id=request.message_id,
            prompt_context=request.prompt_context
        )
        
        reply_text = f"Mock AI reply to context: '{request.prompt_context}'."
        words = reply_text.split(" ")
        
        for i, word in enumerate(words):
            # Send space prefix except for the first word
            chunk_val = f" {word}" if i > 0 else word
            is_final = (i == len(words) - 1)
            
            logger.info("Mock LLM sending chunk", message_id=request.message_id, chunk=chunk_val, is_final=is_final)
            
            yield TokenChunk(
                message_id=request.message_id,
                chunk=chunk_val,
                is_final=is_final
            )
            await asyncio.sleep(0.1) # 100ms typewriter latency

async def serve():
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=5))
    add_GenerationServiceServicer_to_server(MockGenerationService(), server)
    
    server.add_insecure_port("[::]:50051")
    logger.info("Starting mock LLM gRPC Service on port 50051...")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())
