import sys
import signal
import threading
import time

from confluent_kafka import Consumer

from consts import BOOTSTRAP_SERVERS, TOPIC_FILTERED_MESSAGES
from commands import COMMANDS, flush_producer, parse_command, producer
from messages import MessageForChatting

# --- Consumer для filtered_messages
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

    try:
        while not shutdown_flag.is_set():
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Ошибка сообщения: {msg.error()}")
                continue


            try:
                payload = MessageForChatting.from_json(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"❌ Ошибка сериализации/обработки: {e}")
                # продолжаем работать, не прерываем цикл
                continue

            ts = payload.timestamp
            time_str = time.strftime("%H:%M:%S", time.localtime(ts))

            # Важно: печатаем с \n в начале, чтобы не сливалось с приглашением >
            print(f"\n📩 [{time_str}] {payload.user_id} → {payload.target_user_id}: {payload.text}")

            consumer.commit(asynchronous=False)

    except Exception as e:
        print(f"💥 Непредвиденная ошибка: {e}", file=sys.stderr)
    finally:
        print("🛑 Закрываем Consumer...")
        consumer.close()
        print("✅ Consumer закрыт.")



# --- Управление жизненным циклом ---
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

        # Обязательно: poll продюсера, чтобы callback-ы успели отработать
        producer.poll(0)

    flush_producer()
    print("App shutting down.")

if __name__ == "__main__":
    main()
