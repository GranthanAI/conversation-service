"""
System Enums File.
Defines system-wide constants, statuses, and type flags used
across presentation, domain logic, and data access layers.
"""

from enum import Enum

class SenderType(str, Enum):
    """
    Indicates the author of a message in a conversation.
    """
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"

class MessageStatus(str, Enum):
    """
    Represents the operational status lifecycle of a message.
    """
    PENDING = "pending"
    ACTIVE = "active"
    SOFT_DELETED = "soft_deleted"
    FAILED = "failed"

class ConversationStatus(str, Enum):
    """
    Represents the state lifecycle of a conversation.
    """
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"

class OutboxStatus(str, Enum):
    """
    Represents the publication lifecycle status of transactional events in the outbox.
    """
    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"

class EventType(str, Enum):
    """
    Represents unique names/types of Kafka business events.
    """
    CHAT_MESSAGE_CREATED = "chat.message.created"
    CHAT_RESPONSE_COMPLETED = "chat.response.completed"
    CONVERSATION_CREATED = "conversation.created"
    CONVERSATION_UPDATED = "conversation.updated"
    CONVERSATION_DELETED = "conversation.deleted"
    MESSAGE_DELETED = "message.deleted"
