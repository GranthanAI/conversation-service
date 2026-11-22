"""
Kafka Topics Constants.
Centralizes all Kafka message brokers topics naming configurations.
"""

class KafkaTopics:
    """
    Registry constants representing system Kafka topics.
    """
    CHAT_MESSAGE_CREATED = "chat.message.created"
    CHAT_RESPONSE_CHUNK = "chat.response.chunk"
    CHAT_RESPONSE_GENERATED = "chat.response.generated"
    CHAT_RESPONSE_CANCELLED = "chat.response.cancelled"
    CHAT_RESPONSE_SUGGESTIONS = "chat.response.suggestions"

    # Consumed from other services (billing integration)
    ENTITLEMENTS_CHANGED = "entitlements.changed"
    USER_DELETED = "user.deleted"
    USAGE_THRESHOLD_REACHED = "usage.threshold_reached"

    CONVERSATION_CREATED = "conversation.created"
    CONVERSATION_UPDATED = "conversation.updated"
    CONVERSATION_DELETED = "conversation.deleted"

    SUMMARY_GENERATED = "conversation.summary.generated"
    TITLE_GENERATED = "conversation.title.generated"

    CHAT_DLQ = "chat.message.dlq"
