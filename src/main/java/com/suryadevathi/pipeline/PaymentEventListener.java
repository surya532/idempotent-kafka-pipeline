package com.suryadevathi.pipeline;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);

    private final EventParser parser;
    private final IdempotentProcessor processor;

    public PaymentEventListener(EventParser parser, IdempotentProcessor processor) {
        this.parser = parser;
        this.processor = processor;
    }

    @KafkaListener(topics = "${app.topic}")
    public void onMessage(ConsumerRecord<String, String> record) {
        PaymentEvent event = parser.parse(record.value());
        boolean applied = processor.process(event);
        log.debug("partition={} offset={} eventId={} applied={}",
                record.partition(), record.offset(), event.eventId(), applied);
    }
}
