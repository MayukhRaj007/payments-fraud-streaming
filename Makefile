# Thin wrapper over docker compose. Windows users without `make` can use the
# equivalent ./make.ps1 <target>, which dispatches to the same commands.
.PHONY: up stop start down logs produce test lint evaluate ps clean

DEV_IMAGE := pfs-dev

## up: start the whole stack (Kafka, Flink, Postgres, Grafana, producer)
up:
	docker compose up -d
	@echo ""
	@echo "Kafka UI   http://localhost:8082"
	@echo "Flink UI   http://localhost:8081"
	@echo "Grafana    http://localhost:3000  (admin/admin)"

## stop: pause the stack, keeping the containers so `start` resumes them
stop:
	docker compose stop

## start: resume containers previously paused with `stop`
start:
	docker compose start
	@echo ""
	@echo "Kafka UI   http://localhost:8082"
	@echo "Flink UI   http://localhost:8081"
	@echo "Grafana    http://localhost:3000  (admin/admin)"

## down: remove the containers and network. Data survives -- it lives in named
## volumes, so `up` brings everything back with the alert history intact. Use
## `stop` instead if you only want to pause.
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

## clean: full reset -- removes containers AND deletes the volumes, so the
## alert history, Kafka log and Grafana state are all destroyed. This is the
## only target that loses data. Also required after editing sql/init.sql, since
## the Postgres init hook only runs on an empty data directory.
clean:
	docker compose down -v
