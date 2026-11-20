package com.suryadevathi.pipeline;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end tests against REAL Kafka and PostgreSQL (Testcontainers).
 * Skipped automatically when Docker is not available.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class PipelineIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired KafkaTemplate<String, String> template;
    @Autowired EventParser parser;
    @Autowired EventStore store;
    @Autowired JdbcTemplate jdbc;
    @Autowired MeterRegistry meters;
    @Autowired KafkaListenerEndpointRegistry listeners;

    @Value("${app.topic}") String topic;
    @Value("${app.dlt-topic}") String dltTopic;

    // ---------------------------------------------------------------- tests

    @Test
    void duplicateDeliveryIsAppliedOnce() throws Exception {
        String account = "it-dup-" + shortId();
        PaymentEvent event = new PaymentEvent("evt-" + shortId(), account, 500, "PAYMENT");
        double duplicatesBefore = duplicates();

        for (int i = 0; i < 5; i++) {
            send(event);
        }

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .until(() -> duplicates() >= duplicatesBefore + 4 && store.balanceOf(account) == 500);

        assertEquals(500, store.balanceOf(account), "5 deliveries must have exactly one effect");
    }

    @Test
    void manyEventsWithReplayedDuplicatesHaveNoExtraEffect() throws Exception {
        String run = "it-bulk-" + shortId();
        int unique = 200;
        int replays = 100;
        double duplicatesBefore = duplicates();

        for (int i = 0; i < unique; i++) {
            send(new PaymentEvent(run + "-" + i, run + "-acct-" + (i % 10), 1, "PAYMENT"));
        }
        for (int i = 0; i < replays; i++) {
            send(new PaymentEvent(run + "-" + i, run + "-acct-" + (i % 10), 1, "PAYMENT"));
        }

        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(250))
                .until(() -> processedWithPrefix(run) == unique && duplicates() >= duplicatesBefore + replays);

        assertEquals(unique, processedWithPrefix(run));
        assertEquals(unique, totalBalanceWithPrefix(run), "money must be applied once per unique event");
    }

    @Test
    void eventsSentWhileConsumerIsDownAreProcessedOnceAfterRestart() throws Exception {
        String run = "it-restart-" + shortId();
        int count = 50;
        double duplicatesBefore = duplicates();

        listeners.getListenerContainers().forEach(MessageListenerContainer::stop);
        try {
            for (int i = 0; i < count; i++) {
                send(new PaymentEvent(run + "-" + i, run + "-acct", 1, "PAYMENT"));
            }
            for (int i = 0; i < count; i++) { // redelivery of everything
                send(new PaymentEvent(run + "-" + i, run + "-acct", 1, "PAYMENT"));
            }
        } finally {
            listeners.getListenerContainers().forEach(MessageListenerContainer::start);
        }

        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(250))
                .until(() -> processedWithPrefix(run) == count && duplicates() >= duplicatesBefore + count);

        assertEquals(count, totalBalanceWithPrefix(run));
    }

    @Test
    void malformedMessageGoesStraightToDeadLetterTopic() throws Exception {
        String marker = "garbage-" + shortId();
        template.send(topic, "bad-key", marker).get();

        assertDeadLettered(marker);
    }

    @Test
    void failingEventIsRetriedThenDeadLetteredAndNothingIsRecorded() throws Exception {
        String eventId = "poison-" + shortId();
        send(new PaymentEvent(eventId, "it-poison-" + shortId(), 1, "POISON"));

        assertDeadLettered(eventId);

        assertEquals(0, processedWithPrefix(eventId), "a failed event must not leave a dedup row behind");
    }

    // -------------------------------------------------------------- helpers

    private void send(PaymentEvent event) throws Exception {
        template.send(topic, event.accountId(), parser.toJson(event)).get();
    }

    private double duplicates() {
        return meters.counter("pipeline.events.duplicates").count();
    }

    private long processedWithPrefix(String prefix) {
        Long n = jdbc.queryForObject(
                "SELECT count(*) FROM processed_events WHERE event_id LIKE ?", Long.class, prefix + "%");
        return n == null ? 0 : n;
    }

    private long totalBalanceWithPrefix(String prefix) {
        Long n = jdbc.queryForObject(
                "SELECT COALESCE(SUM(balance_cents), 0) FROM account_balances WHERE account_id LIKE ?",
                Long.class, prefix + "%");
        return n == null ? 0 : n;
    }

    private void assertDeadLettered(String marker) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-checker-" + shortId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(dltTopic));
            await().atMost(Duration.ofSeconds(60)).pollInSameThread().until(() -> {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value() != null && record.value().contains(marker)) {
                        return true;
                    }
                }
                return false;
            });
        }
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
