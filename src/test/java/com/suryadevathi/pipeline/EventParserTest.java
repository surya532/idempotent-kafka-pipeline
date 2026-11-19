package com.suryadevathi.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class EventParserTest {

    private final EventParser parser = new EventParser(new ObjectMapper());

    @Test
    void parsesValidEvent() {
        PaymentEvent e = parser.parse("{\"eventId\":\"e1\",\"accountId\":\"a1\",\"amountCents\":250,\"type\":\"PAYMENT\"}");
        assertEquals("e1", e.eventId());
        assertEquals(250, e.amountCents());
    }

    @Test
    void roundTrips() {
        PaymentEvent original = new PaymentEvent("e1", "a1", 7, "PAYMENT");
        assertEquals(original, parser.parse(parser.toJson(original)));
    }

    @Test
    void rejectsGarbage() {
        assertThrows(InvalidEventException.class, () -> parser.parse("not json"));
    }

    @Test
    void rejectsMissingIds() {
        assertThrows(InvalidEventException.class,
                () -> parser.parse("{\"accountId\":\"a1\",\"amountCents\":1,\"type\":\"PAYMENT\"}"));
    }
}
