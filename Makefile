PROJECT_NAME := flink-kraft-realtime-lab
COMPOSE := docker compose
KAFKA := docker compose exec kafka /opt/kafka/bin

.PHONY: build up down restart logs topics lag produce produce-high-load replay-dlq consume-alerts consume-aggregates consume-dlq consume-replay consume-merchant-profiles schema-up schema-register cdc-up cdc-register cdc-update-merchant observe-up chaos-kill-taskmanager chaos-restart-kafka savepoint smoke test k8s-render-dev k8s-render-prod-like clean

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d kafka topic-init flink-jobmanager flink-taskmanager flink-submit kafka-ui api

down:
	$(COMPOSE) down -v

restart: down build up

logs:
	$(COMPOSE) logs -f --tail=200

topics:
	$(KAFKA)/kafka-topics.sh --bootstrap-server kafka:9092 --list

lag:
	$(KAFKA)/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group flink-realtime-lab || true

produce:
	$(COMPOSE) run --rm generator

produce-high-load:
	$(COMPOSE) run --rm -e RUN_SECONDS=180 -e EVENTS_PER_SECOND=150 generator

replay-dlq:
	$(COMPOSE) run --rm replayer

consume-alerts:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic alerts.fraud --from-beginning

consume-aggregates:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic transactions.aggregates --from-beginning

consume-dlq:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic transactions.dlq --from-beginning

consume-replay:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic transactions.replay --from-beginning

consume-merchant-profiles:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic merchant_risk_profiles --from-beginning

schema-up:
	$(COMPOSE) --profile schema up -d schema-registry

schema-register:
	$(COMPOSE) --profile schema run --rm schema-init

cdc-up:
	$(COMPOSE) --profile cdc up -d postgres kafka-connect schema-registry

cdc-register:
	$(COMPOSE) --profile cdc run --rm cdc-init

cdc-update-merchant:
	$(COMPOSE) exec postgres psql -U lab -d realtime_lab -c "update merchant_risk_profiles set risk_tier='HIGH', risk_multiplier=1.8, updated_at=now() where merchant_id='merchant-hot';"

observe-up:
	$(COMPOSE) --profile observability up -d prometheus grafana

chaos-kill-taskmanager:
	./scripts/chaos-kill-taskmanager.sh

chaos-restart-kafka:
	$(COMPOSE) restart kafka

savepoint:
	./scripts/flink-savepoint.sh

smoke:
	./scripts/smoke-test.sh

test:
	docker build --target test -t $(PROJECT_NAME)-flink-test ./flink-job

k8s-render-dev:
	kubectl kustomize k8s/overlays/dev

k8s-render-prod-like:
	kubectl kustomize k8s/overlays/prod-like

clean:
	$(COMPOSE) down -v --remove-orphans
	docker image rm $(PROJECT_NAME)-flink-test 2>/dev/null || true
