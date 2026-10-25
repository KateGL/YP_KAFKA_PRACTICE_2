import os
import re
import json
import signal
import threading
import time
from dataclasses import dataclass
from typing import Optional

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

from messages import MessagePayload, UserRegisteredMessage, UserBlockMessage, WordBlockMessage

# --- Конфигурация из переменных окружения (Vault/Consul/K8s) ---
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

# --- Producer ---
producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",                 # гарантия доставки
    "retries": 3,
    "retry.backoff.ms": 100,
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    payload = None
    try:
        payload = json.loads(msg.value().decode("utf-8"))
    except Exception:
        pass

    event_type = payload.get("event") if payload else "UNKNOWN"
    topic = msg.topic()
    print(f"✅ Подтверждено: event={event_type} | топик={topic} | partition={msg.partition()} | offset={msg.offset()}")



def send_event(topic: str, key: str, message: MessagePayload):
    """Идемпотентная отправка события: ключ — user_id или слово, значение — событие."""
    try:
        producer.produce(
            topic=topic,
            key=key,
            value=message.to_json(),
            callback=delivery_report
        )
    except KafkaException as e:
        print(f"⚠️ Ошибка продюсера: {e}")

def flush_producer():
    producer.flush(timeout=10)

# --- Consumer для filtered_messages ---
def consumer_loop():
    consumer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "cli-app-consumer-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_FILTERED_MESSAGES])

    while not shutdown_flag.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
            # Выводим сообщение в консоль
            print(f"[filtered_messages] offset={msg.offset()} | {data}")
        except Exception as e:
            print(f"Ошибка парсинга сообщения: {e}, raw={msg.value()}")

        # Коммитим офсет вручную
        consumer.commit(asynchronous=False)

    consumer.close()

# --- Команды CLI ---
def cmd_login(args):
    if len(args) != 1:
        print("Usage: login(user_name)")
        return

    user_name = args[0]

    if state.current_user is not None and state.current_user != user_name:
        print(f"❌ Нельзя залогиниться как {user_name}, пока активен {state.current_user}. Сначала выполните logout().")
        return

    state.current_user = user_name
    print(f"Logged in as: {user_name}")

    send_event(
        TOPIC_REGISTERED_USERS,
        key=user_name,
        message=UserRegisteredMessage(
            event="LOGIN",
            user_id=user_name,
            timestamp=int(time.time()),
        )
    )

def cmd_logout(args):
    if state.current_user is None:
        print("No active user session.")
        return

    user_name = state.current_user
    state.current_user = None
    print("Logged out.")

    send_event(
        TOPIC_REGISTERED_USERS,
        key=user_name,
        message=UserRegisteredMessage(
            event="LOGOUT",
            user_id=user_name,
            timestamp=int(time.time()),
        )
    )

class MessagePayloadSimple(MessagePayload):
    # Если у тебя нет отдельного типа для обычных сообщений, можно сделать простой обёрткой.
    # Или лучше — привести все сообщения к одному формату через базовый класс.
    def __init__(self, from_user, to_user, message, timestamp):
        self.from_user = from_user
        self.to_user = to_user
        self.message = message
        self.timestamp = timestamp

    def to_json(self):
        return json.dumps({
            "from": self.from_user,
            "to": self.to_user,
            "message": self.message,
            "timestamp": self.timestamp,
        })

def cmd_send_message(args):
    if state.current_user is None:
        print("You must be logged in to send a message.")
        return
    if len(args) != 2:
        print("Usage: send_message(user_to, message)")
        return

    user_to, message = args
    payload = MessagePayloadSimple(
        from_user=state.current_user,
        to_user=user_to,
        message=message,
        timestamp=int(time.time())
    )
    send_event(TOPIC_MESSAGES, key=state.current_user, message=payload)
    print(f"Message sent to {user_to}")

def _block_unblock_user(args: list, block: bool):
    if state.current_user is None:
        print("You must be logged in.")
        return
    if len(args) != 1:
        print("Usage: block_user_for_me(user) / unblock_user_for_me(user)")
        return

    target = args[0]
    event = "BLOCK" if block else "UNBLOCK"

    send_event(
        TOPIC_BLOCKED_USERS,
        key=state.current_user,
        message=UserBlockMessage(
            event=event,
            user_id=state.current_user,
            target_user_id=target,
            timestamp=int(time.time()),
        )
    )

def _block_unblock_word(args: list, block: bool):
    if state.current_user is None:
        print("You must be logged in.")
        return
    if len(args) != 1:
        print("Usage: block_user_for_me(user) / unblock_user_for_me(user)")
        return

    target = args[0]
    event = "BLOCK" if block else "UNBLOCK"

    send_event(
        TOPIC_BLOCKED_WORDS,
        key=state.current_user,
        message=WordBlockMessage(
            event=event,
            user_id=state.current_user,
            word=target,
            timestamp=int(time.time()),
        )
    )


def cmd_block_user_for_me(args):
    _block_unblock_user(args, block=True)

def cmd_unblock_user_for_me(args):
    _block_unblock_user(args, block=False)

def cmd_block_word(args):
    _block_unblock_word(args, block=True)

def cmd_unblock_word(args):
    _block_unblock_word(args, block=False)

# --- Парсер команд (синтаксис login(name), send_message(to,msg)) ---
CMD_PATTERN = re.compile(r"^([a-zA-Z_]+)\((.*)\)$")

def parse_command(raw: str):
    raw = raw.strip()
    m = CMD_PATTERN.match(raw)
    if not m:
        return None, []
    name = m.group(1)
    args_raw = m.group(2)
    args = [a.strip() for a in args_raw.split(",") if a.strip()]
    return name, args

COMMANDS = {
    "login": cmd_login,
    "logout": cmd_logout,
    "send_message": cmd_send_message,
    "block_user_for_me": cmd_block_user_for_me,
    "unblock_user_for_me": cmd_unblock_user_for_me,
    "block_word": cmd_block_word,
    "unblock_word": cmd_unblock_word,
}

# --- Обработка SIGTERM ---
shutdown_flag = threading.Event()

def handle_sigterm(signum, frame):
    shutdown_flag.set()
    print("Shutdown requested.")

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

def main():
    # Запускаем consumer в отдельном потоке
    consumer_thread = threading.Thread(target=consumer_loop, daemon=True)
    consumer_thread.start()

    print("CLI app ready. Enter commands like: login(alice), logout(), send_message(bob,Hi)")

    while not shutdown_flag.is_set():
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            continue

        name, args = parse_command(line)
        if name is None or name not in COMMANDS:
            print(f"Unknown command: {line}. Use: {', '.join(COMMANDS.keys())}")
            continue

        try:
            COMMANDS[name](args)
        except Exception as e:
            print(f"Command error: {e}")

        # producer.poll нужен, чтобы callback-ы срабатывали
        producer.poll(0)

    flush_producer()
    print("App shutting down.")

if __name__ == "__main__":
    main()
