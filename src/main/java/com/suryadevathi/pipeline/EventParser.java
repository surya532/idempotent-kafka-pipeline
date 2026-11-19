package com.suryadevathi.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class EventParser {

    private final ObjectMapper mapper;

    public EventParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public PaymentEvent parse(String json) {
        PaymentEvent event;
        try {
            event = mapper.readValue(json, PaymentEvent.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new InvalidEventException("Unparseable event", e);
        }
        if (event == null || isBlank(event.eventId()) || isBlank(event.accountId())) {
            throw new InvalidEventException("eventId and accountId are required");
        }
        return event;
    }

    public String toJson(PaymentEvent event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
