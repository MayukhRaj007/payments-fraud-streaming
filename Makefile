# Thin wrapper over docker compose. Windows users without `make` can use the
# equivalent ./make.ps1 <target>, which dispatches to the same commands.
.PHONY: up down logs produce test lint evaluate ps clean

DEV_IMAGE := pfs-dev

## up: start the whole stack (Kafka, Flink, Postgres, Grafana, producer)
up:
	docker compose up -d
	@echo ""
	@echo "Kafka UI   http://localhost:8082"
	@echo "Flink UI   http://localhost:8081"
	@echo "Grafana    http://localhost:3000  (admin/admin)"

## down: stop the stack, keeping volumes
down:
	docker compose down

## logs: follow logs for all services
logs:
	docker compose logs -f --tail=100

## ps: show container status
ps:
	docker compose ps -a

## produce: restart the producer (re-runs the simulation from the fixed seed)
produce:
	docker compose up -d --force-recreate producer
	docker compose logs -f --tail=50 producer

## test: run unit tests in a container (no local Python needed)
test:
	docker build -q -f Dockerfile.dev -t $(DEV_IMAGE) .
	docker run --rm -v "$(CURDIR)":/w -w /w $(DEV_IMAGE) python -m pytest

## lint: run ruff in a container
lint:
	docker build -q -f Dockerfile.dev -t $(DEV_IMAGE) .
	docker run --rm -v "$(CURDIR)":/w -w /w $(DEV_IMAGE) ruff check .

## evaluate: compare injected labels against detected alerts
evaluate:
	docker build -q -f Dockerfile.dev -t $(DEV_IMAGE) .
	docker run --rm --network payments-fraud-streaming_default \
		-v "$(CURDIR)":/w -w /w $(DEV_IMAGE) \
		python scripts/evaluate.py

## clean: stop everything and delete volumes (full reset)
clean:
	docker compose down -v
