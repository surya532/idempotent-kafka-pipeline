![ci](https://github.com/surya532/idempotent-kafka-pipeline/actions/workflows/ci.yml/badge.svg)

# idempotent-kafka-pipeline

An event-driven payment-processing pipeline on **Java 17, Spring Boot, Apache Kafka, and PostgreSQL** that delivers **exactly-once effects on top of at-least-once delivery**, with retries, a dead-letter topic, and Prometheus metrics.

> Personal project, built independently of my employer. It demonstrates patterns, not any production system.

## The problem
Kafka guarantees at-least-once delivery. Consumers crash before committing offsets, rebalances replay messages, and producers retry. If processing a payment event isn't idempotent, a replay double-applies money.

## Design
```
HTTP /demo/burst ──► Kafka topic payments.events (3 partitions, key = accountId)
                          │
                          ▼
              Consumer group "payments-processor" (3 threads)
                          │ parse ──► InvalidEventException ──► dead-letter topic (no retry)
                          ▼
              IdempotentProcessor  (one DB transaction)
                ├─ INSERT processed_events ... ON CONFLICT DO NOTHING   ← idempotency guard
                └─ UPSERT account_balances (balance += amount)          ← business effect
                          │ failure ──► retry 500ms/1s/2s ──► payments.events.DLT
                          ▼
              Micrometer ──► /actuator/prometheus ──► Prometheus
```

### Key decisions
- **Idempotency via a primary key, in the same transaction as the effect.** Dedup row and balance update commit or roll back together, so a crash between them can't leave a half-applied event.
- **`INSERT ... ON CONFLICT DO NOTHING`** instead of catching a unique-violation exception, which would mark the Spring transaction rollback-only.
- **Message key = `accountId`** so each account's events stay ordered within a partition.
- **Producer:** `acks=all` and `enable.idempotence=true` to avoid duplicate writes from producer retries.
- **Consumer:** manual-commit semantics through Spring's listener container; offsets advance only after the handler succeeds.
- **Error handling:** transient failures retry with exponential backoff; malformed messages skip retries; exhausted records go to `payments.events.DLT` so one bad message can't block a partition.
- **Persistence behind an interface** (`EventStore`) so the idempotency logic is unit-tested without a database.

### Known limitations (honest list)
- Single broker, replication factor 1. Not a durability demo.
- The DLT is written but nothing consumes it yet.
- Idempotency table grows forever; production needs TTL/partitioned cleanup.
- Exactly-once here means *effects*, not Kafka transactions end to end.

## Run it
Requires Docker, Java 17, Maven.
```bash
docker compose up -d                 # Kafka (KRaft), Postgres, Prometheus
mvn spring-boot:run                  # app on :8080
```

### Try the behaviors
```bash
# 1. Duplicate delivery -> applied once
curl -X POST localhost:8080/events -H 'content-type: application/json' \
  -d '{"eventId":"e1","accountId":"a1","amountCents":500,"type":"PAYMENT"}'
curl -X POST localhost:8080/events -H 'content-type: application/json' \
  -d '{"eventId":"e1","accountId":"a1","amountCents":500,"type":"PAYMENT"}'
curl localhost:8080/accounts/a1/balance          # {"balanceCents":500}, not 1000

# 2. Malformed message -> straight to the dead-letter topic
curl -X POST localhost:8080/events/raw -d 'not json'

# 3. Failing message -> 3 retries, then dead-letter
curl -X POST localhost:8080/events -H 'content-type: application/json' \
  -d '{"eventId":"e2","accountId":"a1","amountCents":1,"type":"POISON"}'

# 4. Load + duplicate replay, prints MEASURED throughput
scripts/load_test.sh 10000 1000
```
Inspect the DLT:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payments.events.DLT --from-beginning
```

## Metrics
`/actuator/prometheus` exposes `pipeline_events_processed_total`, `pipeline_events_duplicates_total`, `pipeline_events_dead_lettered_total`, plus Kafka client metrics from Micrometer (check for consumer lag metrics there). Prometheus UI: http://localhost:9090.

## Test
```bash
mvn verify      # unit tests for idempotency and parsing
```

## Benchmarks
Run `scripts/load_test.sh` on your machine and record the result here with your hardware, e.g.
`10,000 events in X s (~Y events/sec) on <machine>, single broker, local`. Local numbers are for comparison between changes, not production capacity claims.

## Next steps
- Testcontainers integration test (real Kafka + Postgres) proving no duplicate effects under consumer restarts
- DLT replay tool
- Grafana dashboard
- Outbox pattern for the producer side
