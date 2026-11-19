package com.suryadevathi.pipeline;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Exactly-once EFFECT on top of at-least-once delivery.
 *
 * Kafka may redeliver a message (consumer crash before offset commit, rebalance, producer retry).
 * The dedup insert and the balance update run in ONE database transaction, so either both happen
 * or neither does, and a redelivered event is detected by its primary key and skipped.
 */
@Service
public class IdempotentProcessor {

    private final EventStore store;
    private final Counter processed;
    private final Counter duplicates;

    public IdempotentProcessor(EventStore store, MeterRegistry registry) {
        this.store = store;
        this.processed = registry.counter("pipeline.events.processed");
        this.duplicates = registry.counter("pipeline.events.duplicates");
    }

    /** @return true if the event was applied, false if it was a duplicate. */
    @Transactional
    public boolean process(PaymentEvent event) {
        if ("POISON".equals(event.type())) {
            // Demo hook: simulates a downstream failure. The transaction rolls back, so nothing is recorded,
            // and the listener's error handler retries with backoff, then dead-letters the message.
            throw new IllegalStateException("Simulated processing failure for " + event.eventId());
        }
        if (!store.markProcessed(event)) {
            duplicates.increment();
            return false;
        }
        store.applyBalance(event.accountId(), event.amountCents());
        processed.increment();
        return true;
    }
}
