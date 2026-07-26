.PHONY: help setup infra schema dev clean

help:
	@echo "========================================================================"
	@echo "GraphGPT Conversation Service - Setup & Execution Commands"
	@echo "========================================================================"
	@echo "  make setup      - Copy .env and install dependencies via uv"
	@echo "  make infra      - Start local Cassandra, Redis, and Kafka in Docker"
	@echo "  make schema     - Apply Cassandra CQL schemas and tables"
	@echo "  make dev        - Launch the FastAPI development server"
	@echo "  make clean      - Remove __pycache__ and build files"
	@echo "========================================================================"

setup:
	@python -c "import os, shutil; os.path.exists('.env') or shutil.copy('.env.example', '.env')"
	uv pip install -r requirements.txt

infra:
	docker compose up -d

schema:
	@python -c "import subprocess; subprocess.run(['docker', 'exec', '-i', 'graphgpt-cassandra', 'cqlsh'], stdin=open('app/db/schema.cql'))"

dev:
	uv run uvicorn app.main:app --reload

clean:
	@python -c "import shutil, glob; [shutil.rmtree(p, ignore_errors=True) for p in glob.glob('**/__pycache__', recursive=True)]"
