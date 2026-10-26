import faust
import re
import os
from messages import MessageForChatting, UserBlockMessage, WordBlockMessage

# --- Конфигурация из переменных окружения (Vault/Consul/K8s) ---
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TOPIC_REGISTERED_USERS = os.getenv("TOPIC_REGISTERED_USERS", "registered_users")
TOPIC_MESSAGES = os.getenv("TOPIC_MESSAGES", "messages")
TOPIC_BLOCKED_USERS = os.getenv("TOPIC_BLOCKED_USERS", "blocked_users")
TOPIC_BLOCKED_WORDS = os.getenv("TOPIC_BLOCKED_WORDS", "blocked_words")
TOPIC_FILTERED_MESSAGES = os.getenv("TOPIC_FILTERED_MESSAGES", "filtered_messages")
TOPIC_PARTITIONS_COUNT = int(os.getenv("TOPIC_PARTITIONS_COUNT", "3"))

app = faust.App(
    id='message-processor-service',
    broker=BOOTSTRAP_SERVERS,
    autodiscover=False,
    processing_guarantee='exactly_once',
)

# Топики
messages_topic = app.topic(TOPIC_MESSAGES, value_type=MessageForChatting)
blocked_words_topic = app.topic(TOPIC_BLOCKED_WORDS, value_type=WordBlockMessage)
user_blocks_topic = app.topic(TOPIC_BLOCKED_USERS, value_type=UserBlockMessage)
filtered_messages_topic = app.topic(TOPIC_FILTERED_MESSAGES, value_type=MessageForChatting)

# Таблицы состояния (streaming state)
# blocked_pairs[user_id][target_user_id] = True
blocked_pairs = app.Table('blocked_pairs', default=dict, partitions=TOPIC_PARTITIONS_COUNT)

# blocked_words_set[word] = True (для быстрого поиска)
blocked_words_set = app.Table('blocked_words_set', default=set, partitions=TOPIC_PARTITIONS_COUNT)

@app.agent(blocked_words_topic)
async def process_blocked_words(events):
    """Потоковое обновление стоп‑слов: добавление и удаление"""
    async for block in events:
        word = block.word
        print(block)
        if block.event == "BLOCK":
            blocked_words_set[word] = True
            print(f"[BLOCKED WORD] Добавлен: {word}")

        elif block.event == "UNBLOCK":
            # Удаляем ключ из таблицы
            if word in blocked_words_set:
                del blocked_words_set[word]
                print(f"[UNBLOCK WORD] Удален: {word}")
            else:
                print(f"[UNBLOCK WORD] Слово не было в списке: {word}")

@app.agent(user_blocks_topic)
async def process_user_blocks(events):
    """Потоковая обработка блокировок пользователей"""
    async for block in events:
        user_from = block.user_id
        user_target = block.target_user_id

        if block.event == "BLOCK":
            if user_from not in blocked_pairs:
                blocked_pairs[user_from] = {}
            blocked_pairs[user_from][user_target] = True
            print(f"[BLOCK] {user_from} заблокировал {user_target}")

        elif block.event == "UNBLOCK":
            if user_from in blocked_pairs and user_target in blocked_pairs[user_from]:
                del blocked_pairs[user_from][user_target]
                print(f"[UNBLOCK] {user_from} разблокировал {user_target}")


@app.agent(messages_topic)
async def process_messages(events):
    """Основной streaming-агент: фильтрация и замена слов"""
    async for msg in events:
        # 1. Проверка блокировки: если user_id заблокирован ДЛЯ target_user_id — пропускаем
        is_blocked = False
        # print(msg)
        if msg.target_user_id in blocked_pairs:
            is_blocked = blocked_pairs[msg.target_user_id].get(msg.user_id, False)
            # print(f"{is_blocked=}")

        if is_blocked:
            print(f"[SKIP] Сообщение пропущено: {msg.target_user_id} -> {msg.user_id} (заблокирован)")
            continue

        # 2. Замена стоп‑слов в тексте
        text = msg.text
        for word in blocked_words_set.keys():
            # \b — границы слова, IGNORECASE — регистронезависимо
            pattern = r'\b' + re.escape(word) + r'\b'
            text = re.sub(pattern, '***', text, flags=re.IGNORECASE)

        # 3. Отправка отфильтрованного сообщения
        filtered_msg = MessageForChatting(
            user_id= msg.user_id,
            target_user_id= msg.target_user_id,
            text= text,
            timestamp=msg.timestamp
        )

        await filtered_messages_topic.send(value=filtered_msg)
        print(f"[OK] Обработано: {msg.user_id} -> {msg.target_user_id}, текст: {text}")


if __name__ == '__main__':
    app.main()