.PHONY: up up-infra down reset status create-topic describe-topic list-topics delete-topic restart-apps restart-ui

# Явно указываем имя файла — так точно не будет ошибок с пустым именем
COMPOSE_FILE := docker-compose.yaml

CONTAINER_PREFIX := kafka-
BOOTSTRAP_SERVERS := kafka-1:9092,kafka-2:9092,kafka-3:9092

PARTITIONS ?= 3
REPLICATION_FACTOR ?= 2

# Получаем ID контейнера kafka-1
KAFKA_CONTAINER := $(shell docker compose -f $(COMPOSE_FILE) ps -q kafka-1 2>/dev/null)

check-cluster:
	@if [ -z "$(KAFKA_CONTAINER)" ]; then \
		echo "❌ Ошибка: Кластер Kafka не найден или не запущен."; \
		echo "   Запустите: make up-infra или make up"; \
		exit 1; \
	fi

up:
	@echo "🚀 Запуск всего (Kafka + UI + продюсер + консьюмеры)..."
	docker compose -f $(COMPOSE_FILE) up --build -d

up-infra:
	@echo "🚀 Запуск инфраструктуры (Kafka + Zookeeper + kafka-ui) без приложений..."
	# Теперь kafka-ui стартует вместе с брокерами
	docker compose -f $(COMPOSE_FILE) up --build -d kafka-1 kafka-2 kafka-3 zookeeper-1 kafka-ui

down:
	@echo "🛑 Остановка всего кластера и приложений..."
	docker compose -f $(COMPOSE_FILE) down

reset:
	@echo "💥 Полный сброс (контейнеры, volumes, orphan-сервисы)..."
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans || true
	@echo "🧹 Попытка удалить сеть проекта, если осталась..."
	docker network rm yp_kafka_practice_1_kafka-net 2>/dev/null || true
	@echo "✅ Сброс завершён."



restart-ui:
	@echo "🔄 Перезапуск Kafka UI..."
	docker compose -f $(COMPOSE_FILE) restart kafka-ui

status-ui:
	@echo "👁 Статус Kafka UI:"
	docker ps --filter "name=kafka-ui" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

status:
	@echo "📊 Статус контейнеров Kafka:"
	docker ps --filter "name=$(CONTAINER_PREFIX)" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "🌐 Bootstrap Servers: $(BOOTSTRAP_SERVERS)"
	@echo "   Default Partitions: $(PARTITIONS)"
	@echo "   Default Replication Factor: $(REPLICATION_FACTOR)"
	@if [ -n "$(KAFKA_CONTAINER)" ]; then echo "✅ Кластер активен."; else echo "⚠️ Кластер не активен."; fi

create-topic: check-cluster
	@if [ -z "$(TOPIC)" ]; then \
		echo "❌ Ошибка: не задано имя топика. Используйте: make create-topic TOPIC=my-topic"; \
		exit 1; \
	fi
	@echo "✨ Создание топика '$(TOPIC)'..."
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--create \
		--topic "$(TOPIC)" \
		--partitions "$(PARTITIONS)" \
		--replication-factor "$(REPLICATION_FACTOR)" \
		--config min.insync.replicas="$(REPLICATION_FACTOR)" \
		--if-not-exists

describe-topic: check-cluster
	@if [ -z "$(TOPIC)" ]; then \
		echo "❌ Ошибка: не задано имя топика."; \
		exit 1; \
	fi
	@echo "👁️ Описание топика '$(TOPIC)'..."
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--describe \
		--topic "$(TOPIC)"

list-topics: check-cluster
	@echo "📋 Список топиков:"
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--list

delete-topic: check-cluster
	@if [ -z "$(TOPIC)" ]; then \
		echo "❌ Ошибка: не задано имя топика."; \
		exit 1; \
	fi
	@echo "⚠️ Внимание: удаление топика может не сработать, если delete.topic.enable=false в конфиге Kafka."
	@echo "   Удаление топика '$(TOPIC)'..."
	@docker exec "$(KAFKA_CONTAINER)" kafka-topics \
		--bootstrap-server "$(BOOTSTRAP_SERVERS)" \
		--delete \
		--topic "$(TOPIC)"
