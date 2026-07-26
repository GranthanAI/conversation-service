"""
Pydantic Schemas & Domain Models Verification Test.
Asserts structural limits, whitespace cleanups, and UUIDv7 chronological ordering.
"""

import sys
import os
sys.path.append(os.getcwd())

import time
from pydantic import ValidationError
from app.utils.helpers import uuidv7
from app.schemas.conversation import CreateConversationRequest, RenameConversationRequest
from app.schemas.message import CreateMessageRequest
from app.core.logging import logger

def test_schemas_and_uuidv7():
    logger.info("Initializing Validation Layer Tests...")

    # 1. Verify UUIDv7 Chronological Generation
    logger.info("Testing UUIDv7 ordering and compliance...")
    id1 = uuidv7()
    time.sleep(0.005) # small sleep to advance timestamp millisecond
    id2 = uuidv7()
    
    # Assert version is 7
    assert id1.version == 7, "UUIDv7 version mismatch"
    assert id2.version == 7, "UUIDv7 version mismatch"
    # Assert chronological sort order
    assert id1 < id2, "Chronological sort order failed"
    logger.info("UUIDv7 Generator: chronological sorting [SUCCESS]")

    # 2. Verify CreateConversationRequest Validations
    logger.info("Testing CreateConversationRequest validation limits...")
    # Empty title check
    try:
        CreateConversationRequest(title="  ")
        assert False, "Failed to reject empty title"
    except ValidationError:
         logger.info("Rejected empty title [SUCCESS]")

    # Max length check
    try:
        CreateConversationRequest(title="a" * 256)
        assert False, "Failed to reject title exceeding 255 chars"
    except ValidationError:
         logger.info("Rejected title > 255 chars [SUCCESS]")

    # Strip check
    req = CreateConversationRequest(title="  Clean Title  ")
    assert req.title == "Clean Title", "Title whitespace strip failed"
    logger.info("Successfully stripped whitespace from title [SUCCESS]")

    # 3. Verify CreateMessageRequest Validations
    logger.info("Testing CreateMessageRequest validation limits...")
    # Empty message check
    try:
        CreateMessageRequest(content=" ")
        assert False, "Failed to reject empty message content"
    except ValidationError:
         logger.info("Rejected empty message content [SUCCESS]")

    # Max length check
    try:
        CreateMessageRequest(content="a" * 8001)
        assert False, "Failed to reject message content exceeding 8000 chars"
    except ValidationError:
         logger.info("Rejected message content > 8000 chars [SUCCESS]")

    req_msg = CreateMessageRequest(content="A valid message content.")
    assert req_msg.content == "A valid message content.", "Valid message content parse failed"
    logger.info("Message parsing [SUCCESS]")

    print("\n==================================================")
    print("SCHEMAS & DOMAIN VALIDATION VERIFICATION: ALL PASSED")
    print("==================================================\n")

if __name__ == "__main__":
    test_schemas_and_uuidv7()
