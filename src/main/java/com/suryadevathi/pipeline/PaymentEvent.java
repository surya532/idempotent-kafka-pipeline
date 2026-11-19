package com.suryadevathi.pipeline;

/**
 * @param eventId     globally unique id; the idempotency key
 * @param accountId   also used as the Kafka message key, so one account's events stay ordered
 * @param amountCents signed amount applied to the account balance
 * @param type        "PAYMENT" normally; "POISON" simulates a processing failure (demo only)
 */
public record PaymentEvent(String eventId, String accountId, long amountCents, String type) {
}
