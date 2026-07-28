.PHONY: help setup infra schema dev test clean

help:
	@echo "========================================================================"
	@echo "GraphGPT Conversation Service - Setup & Execution Commands"
	@echo "========================================================================"
	@echo "  make setup      - Copy .env and install dependencies via uv"
	@echo "  make infra      - Start local Cassandra, Redis, and Kafka in Docker"
	@echo "  make schema     - Wait for Cassandra readiness & apply CQL schema"
	@echo "  make dev        - Launch the FastAPI development server"
	@echo "  make test       - Run pytest unit tests suite"
	@echo "  make clean      - Remove __pycache__ and build files"
	@echo "  Tailing Kafka Topics:"
	@echo "    make kafka-log-conv-created - Stream conversation.created topic"
	@echo "    make kafka-log-conv-updated - Stream conversation.updated topic"
	@echo "    make kafka-log-conv-deleted - Stream conversation.deleted topic"
	@echo "    make kafka-log-msg-created  - Stream chat.message.created topic"
	@echo "========================================================================"

setup:
	@python -c "import os, shutil; os.path.exists('.env') or shutil.copy('.env.example', '.env')"
	uv pip install -r requirements.txt

infra:
	docker compose up -d

schema:
	@python -c "import subprocess, time, sys; print('Waiting for Cassandra CQL port 9042 to accept connections...'); [sys.exit(subprocess.run(['docker', 'exec', '-i', 'graphgpt-cassandra', 'cqlsh'], stdin=open('app/db/schema.cql')).returncode) if subprocess.run(['docker', 'exec', '-i', 'graphgpt-cassandra', 'cqlsh', '-e', 'DESCRIBE KEYSPACES'], capture_output=True).returncode == 0 else time.sleep(2) for _ in range(30)]; print('Error: Cassandra timed out after 60s.'); sys.exit(1)"

dev:
	uv run uvicorn app.main:app --reload

test:
	uv run python -m pytest tests/unit/

clean:
	@python -c "import shutil, glob; [shutil.rmtree(p, ignore_errors=True) for p in glob.glob('**/__pycache__', recursive=True)]"

kafka-log-conv-created:
	docker exec -it graphgpt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic conversation.created --from-beginning

kafka-log-conv-updated:
	docker exec -it graphgpt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic conversation.updated --from-beginning

kafka-log-conv-deleted:
	docker exec -it graphgpt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic conversation.deleted --from-beginning

kafka-log-msg-created:
	docker exec -it graphgpt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic chat.message.created --from-beginning

