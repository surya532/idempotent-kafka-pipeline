# idempotent-kafka-pipeline
![ci](https://github.com/surya532/idempotent-kafka-pipeline/actions/workflows/ci.yml/badge.svg)

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

## Testing

```bash
mvn verify      # unit tests + Testcontainers integration tests (requires Docker)
```

CI runs both layers on every push.

### Unit tests (no infrastructure)
The processing logic sits behind an `EventStore` interface, so it is tested with an in-memory fake:
- A duplicate delivery has no second effect
- Different events accumulate correctly
- A failing event records nothing
- Event parsing rejects malformed or incomplete messages

### Integration tests (real Kafka + PostgreSQL via Testcontainers)
Five scenarios run against real containers:

| Scenario | What it asserts |
|---|---|
| Duplicate delivery | The same event sent 5 times changes the balance once. |
| Bulk with replays | 200 unique events plus 100 replayed duplicates leave exactly 200 processed rows and the correct total balance. |
| Consumer outage and restart | Events sent while the consumer is stopped, each sent twice, are applied once after restart. |
| Malformed message | Unparseable JSON goes straight to `payments.events.DLT` without retries. |
| Failing event | The event is retried with backoff, dead-lettered, and leaves no dedup row behind. |

The last scenario checks the transactional design: the dedup insert and the balance update commit or roll back together, so a failed event never marks itself as processed.

### What the tests do not cover
- The restart test stops and restarts the listener containers. It simulates a consumer outage, not a process crash mid-message. The transaction boundary is what covers the crash case, and that is not tested by killing a process.
- Single broker, replication factor 1. Broker failure is not tested.
- No load or soak testing in CI (see Benchmarks).

## Benchmarks
Not yet benchmarked. `scripts/load_test.sh` measures end-to-end throughput on your own hardware:

    scripts/load_test.sh 10000 1000
    
## Next steps
- Crash test: kill the app process mid-batch and assert no duplicate effects on recovery
- DLT replay tool
- Grafana dashboard for the Prometheus metrics
- Outbox pattern for the producer side
- Cleanup job for the `processed_events` table
