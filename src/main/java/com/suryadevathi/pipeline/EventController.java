package com.suryadevathi.pipeline;

import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EventController {

    private final KafkaTemplate<String, String> kafka;
    private final EventParser parser;
    private final EventStore store;
    private final String topic;

    public EventController(KafkaTemplate<String, String> kafka, EventParser parser, EventStore store,
                           @Value("${app.topic}") String topic) {
        this.kafka = kafka;
        this.parser = parser;
        this.store = store;
        this.topic = topic;
    }

    /** Publish one event (use the same eventId twice to see deduplication). */
    @PostMapping("/events")
    public Map<String, String> publish(@RequestBody PaymentEvent event) {
        kafka.send(topic, event.accountId(), parser.toJson(event));
        return Map.of("status", "sent", "eventId", event.eventId());
    }

    /** Publish a raw string as-is, e.g. malformed JSON, to demo the dead-letter path. */
    @PostMapping("/events/raw")
    public Map<String, String> publishRaw(@RequestBody String raw, @RequestParam(defaultValue = "bad") String key) {
        kafka.send(topic, key, raw);
        return Map.of("status", "sent");
    }

    /**
     * Load/duplicate demo: sends `count` unique events across `accounts` accounts (+1 cent each),
     * then resends `duplicates` of them. Expected processed count == count.
     */
    @PostMapping("/demo/burst")
    public Map<String, Object> burst(@RequestParam int count,
                                     @RequestParam(defaultValue = "100") int accounts,
                                     @RequestParam(defaultValue = "0") int duplicates) {
        String run = UUID.randomUUID().toString().substring(0, 8);
        int dups = Math.min(duplicates, count);
        for (int i = 0; i < count; i++) {
            send(run, i, accounts);
        }
        for (int i = 0; i < dups; i++) {
            send(run, i, accounts);
        }
        kafka.flush();
        return Map.of("run", run, "uniqueEvents", count, "duplicatesSent", dups);
    }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return Map.of("processedEvents", store.processedCount());
    }

    @GetMapping("/accounts/{id}/balance")
    public Map<String, Long> balance(@PathVariable String id) {
        return Map.of("balanceCents", store.balanceOf(id));
    }

    private void send(String run, int i, int accounts) {
        String account = "acct-" + (i % accounts);
        PaymentEvent e = new PaymentEvent(run + "-" + i, account, 1, "PAYMENT");
        kafka.send(topic, account, parser.toJson(e));
    }
}
