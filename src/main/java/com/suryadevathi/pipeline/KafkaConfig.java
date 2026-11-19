package com.suryadevathi.pipeline;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic eventsTopic(@Value("${app.topic}") String topic, @Value("${app.partitions}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic(@Value("${app.dlt-topic}") String topic, @Value("${app.partitions}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    /**
     * Retry transient failures with exponential backoff (3 retries: 500ms, 1s, 2s),
     * then publish the record to the dead-letter topic and move on so one bad message cannot block a partition.
     * Malformed messages are not retried at all.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, MeterRegistry registry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template, (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        Counter deadLettered = registry.counter("pipeline.events.dead_lettered");
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, ex) -> {
                    deadLettered.increment();
                    recoverer.accept(record, ex);
                }, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }
}
