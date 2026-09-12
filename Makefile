.PHONY: up down restart ps logs health topics reset-topics connector-status ingest clean

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

health:
	docker exec redpanda rpk cluster health
	docker exec postgres pg_isready -U fraud -d fraud_platform
	curl -s http://localhost:8083/connectors/ops-postgres-source/status

topics:
	docker exec redpanda rpk topic list

connector-status:
	curl -s http://localhost:8083/connectors/ops-postgres-source/status

ingest:
	uv run python -m src.ingestion.run_ingestion

# Destroys all data. Use when you want a clean slate.
clean:
	docker compose down -v