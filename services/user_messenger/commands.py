import json
import re
import time

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from consts import BOOTSTRAP_SERVERS, TOPIC_REGISTERED_USERS, TOPIC_BLOCKED_USERS, TOPIC_BLOCKED_WORDS, TOPIC_MESSAGES, state, state_lock
from messages import MessageForChatting, UserRegisteredMessage, UserBlockMessage, WordBlockMessage

# --- Producer ---
producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
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
    # print(f"✅ Подтверждено: event={event_type} | топик={topic} | partition={msg.partition()} | offset={msg.offset()}")


def send_event(topic: str, key: str, message: MessageForChatting):
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



# --- Команды CLI ---
def cmd_login(args):
    if len(args) != 1:
        print("Usage: login(user_name)")
        return
    user_name = args[0]

    with state_lock:
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
    with state_lock:
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

def cmd_send_message(args):
    with state_lock:
        if state.current_user is None:
            print("You must be logged in to send a message.")
            return
        current = state.current_user

    if len(args) != 2:
        print("Usage: send_message(user_to, message)")
        return

    user_to, message = args
    payload = MessageForChatting(
        user_id=current,
        target_user_id=user_to,
        text=message,
        timestamp=int(time.time())
    )
    send_event(TOPIC_MESSAGES, key=current, message=payload)
    print(f"Message sent to {user_to}")

# Вспомогательные команды (block/unblock) — аналогично с lock при чтении current_user
def _block_unblock_user(args: list, block: bool):
    with state_lock:
        if state.current_user is None:
            print("You must be logged in.")
            return
        user = state.current_user
    if len(args) != 1:
        print("Usage: block_user_for_me(user) / unblock_user_for_me(user)")
        return
    target = args[0]
    event = "BLOCK" if block else "UNBLOCK"
    send_event(
        TOPIC_BLOCKED_USERS,
        key=f"{user}|{target}",
        message=UserBlockMessage(
            event=event,
            user_id=user,
            target_user_id=target,
            timestamp=int(time.time()),
        )
    )

def _block_unblock_word(args: list, block: bool):
    with state_lock:
        if state.current_user is None:
            print("You must be logged in.")
            return
        user = state.current_user
    if len(args) != 1:
        print("Usage: block_word(word) / unblock_word(word)")
        return
    word = args[0]
    event = "BLOCK" if block else "UNBLOCK"
    send_event(
        TOPIC_BLOCKED_WORDS,
        key=user,
        message=WordBlockMessage(
            event=event,
            user_id=user,
            word=word,
            timestamp=int(time.time()),
        )
    )

def cmd_block_user_for_me(args): _block_unblock_user(args, block=True)
def cmd_unblock_user_for_me(args): _block_unblock_user(args, block=False)
def cmd_block_word(args): _block_unblock_word(args, block=True)
def cmd_unblock_word(args): _block_unblock_word(args, block=False)

# --- Парсер команд ---
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