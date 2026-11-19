package com.suryadevathi.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IdempotentProcessorTest {

    /** In-memory fake of the persistence port. */
    static class FakeStore implements EventStore {
        final Set<String> seen = new HashSet<>();
        final Map<String, Long> balances = new HashMap<>();

        public boolean markProcessed(PaymentEvent e) { return seen.add(e.eventId()); }
        public void applyBalance(String a, long d) { balances.merge(a, d, Long::sum); }
        public long processedCount() { return seen.size(); }
        public long balanceOf(String a) { return balances.getOrDefault(a, 0L); }
    }

    FakeStore store;
    IdempotentProcessor processor;

    @BeforeEach
    void setUp() {
        store = new FakeStore();
        processor = new IdempotentProcessor(store, new SimpleMeterRegistry());
    }

    @Test
    void appliesEventOnce() {
        assertTrue(processor.process(new PaymentEvent("e1", "a1", 500, "PAYMENT")));
        assertEquals(500, store.balanceOf("a1"));
    }

    @Test
    void duplicateDeliveryHasNoSecondEffect() {
        PaymentEvent e = new PaymentEvent("e1", "a1", 500, "PAYMENT");
        assertTrue(processor.process(e));
        assertFalse(processor.process(e));
        assertFalse(processor.process(e));
        assertEquals(500, store.balanceOf("a1"));
        assertEquals(1, store.processedCount());
    }

    @Test
    void differentEventsAccumulate() {
        processor.process(new PaymentEvent("e1", "a1", 500, "PAYMENT"));
        processor.process(new PaymentEvent("e2", "a1", -200, "PAYMENT"));
        assertEquals(300, store.balanceOf("a1"));
    }

    @Test
    void poisonEventFailsAndRecordsNothing() {
        assertThrows(IllegalStateException.class,
                () -> processor.process(new PaymentEvent("e9", "a1", 1, "POISON")));
        assertEquals(0, store.processedCount());
    }
}
