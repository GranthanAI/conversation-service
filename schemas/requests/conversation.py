"""
Schemas Requests File - Conversation.
Defines Pydantic request models with input validation rules for creating
and renaming conversations via API controllers.
"""

from pydantic import BaseModel, Field, field_validator

class CreateConversationRequest(BaseModel):
    """
    Pydantic schema representing request parameters for initiating a new conversation.
    """
    title: str = Field(..., min_length=1, max_length=255, description="Initial title of the conversation")

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: str) -> str:
        """
        Trims whitespace and ensures the title is not empty.
        """
        stripped = v.strip()
        if not stripped:
            raise ValueError("Title must not be empty or whitespace only.")
        return stripped

class RenameConversationRequest(BaseModel):
    """
    Pydantic schema representing request parameters for renaming a conversation.
    """
    title: str = Field(..., min_length=1, max_length=255, description="New title of the conversation")

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: str) -> str:
        """
        Trims whitespace and ensures the title is not empty.
        """
        stripped = v.strip()
        if not stripped:
            raise ValueError("Title must not be empty or whitespace only.")
        return stripped
