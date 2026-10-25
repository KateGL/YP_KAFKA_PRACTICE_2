import argparse
import time
import os
from pathlib import Path

from confluent_kafka import Producer
from services.user_messenger.messages import WordBlockMessage

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Ошибка доставки: {err}")
    else:
        print(
            f"✅ Слово доставлено: топик={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Путь к файлу со списком слов")
    parser.add_argument("--topic", required=True, help="Топик Kafka")
    args = parser.parse_args()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092")

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    path = Path(args.file)
    if not path.is_file():
        raise FileNotFoundError(f"Файл не найден: {path}")

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            word = line.strip()
            if not word:
                continue

            # Используем ТОЧНО ту же схему, что и в команде block_word
            payload = WordBlockMessage(
                user_id="script",
                event="BLOCK",
                word=word,
                timestamp=int(time.time()),
                # если в схеме есть дополнительные поля (source, version и т.п.) — добавь их здесь
            )

            producer.produce(
                topic=args.topic,
                value=payload.to_json(),  # сериализация как JSON (как в CLI)
                on_delivery=delivery_report,
            )
            producer.poll(0)

    producer.flush()

if __name__ == "__main__":
    main()
