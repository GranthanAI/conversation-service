"""
Kafka Topics Constants.
Centralizes all Kafka message brokers topics naming configurations.
"""

class KafkaTopics:
    """
    Registry constants representing system Kafka topics.
    """
    CHAT_MESSAGE_CREATED = "chat.message.created"
    CHAT_RESPONSE_COMPLETED = "chat.response.completed"

    CONVERSATION_CREATED = "conversation.created"
    CONVERSATION_UPDATED = "conversation.updated"
    CONVERSATION_DELETED = "conversation.deleted"

    SUMMARY_GENERATED = "conversation.summary.generated"
    TITLE_GENERATED = "conversation.title.generated"

    CHAT_DLQ = "chat.message.dlq"
