import os
import threading
from dataclasses import dataclass
from typing import Optional

# --- Конфигурация ---
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TOPIC_REGISTERED_USERS = os.getenv("TOPIC_REGISTERED_USERS", "registered_users")
TOPIC_MESSAGES = os.getenv("TOPIC_MESSAGES", "messages")
TOPIC_BLOCKED_USERS = os.getenv("TOPIC_BLOCKED_USERS", "blocked_users")
TOPIC_BLOCKED_WORDS = os.getenv("TOPIC_BLOCKED_WORDS", "blocked_words")
TOPIC_FILTERED_MESSAGES = os.getenv("TOPIC_FILTERED_MESSAGES", "filtered_messages")


@dataclass
class AppState:
    current_user: Optional[str] = None

state = AppState()
state_lock = threading.Lock()  # защита от гонки потоков