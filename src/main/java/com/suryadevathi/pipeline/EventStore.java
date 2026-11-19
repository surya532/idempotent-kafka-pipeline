package com.suryadevathi.pipeline;

/** Persistence port. Kept as an interface so the idempotency logic is unit-testable without a database. */
public interface EventStore {

    /** Atomically records the event id. Returns false if it was already recorded. */
    boolean markProcessed(PaymentEvent event);

    /** Applies the business effect (balance change). */
    void applyBalance(String accountId, long deltaCents);

    long processedCount();

    long balanceOf(String accountId);
}
