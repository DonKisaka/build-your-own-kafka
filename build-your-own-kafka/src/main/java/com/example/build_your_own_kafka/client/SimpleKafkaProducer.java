package com.example.build_your_own_kafka.client;


import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Producer for Build Your Own Kafka.
 * Wraps SimpleKafkaClient to provide a simple
 * message-sending API for the application layer.
 */
@Slf4j
@Service
public class SimpleKafkaProducer {



    private final SimpleKafkaClient client;

    /**
     * Spring automatically injects the SimpleKafkaClient
     * bean via constructor — no @Autowired needed on
     * single-constructor classes
     */
    public SimpleKafkaProducer(SimpleKafkaClient client) {
        this.client = client;
    }



    @Value("${kafka.producer.topic}")
    private String topic;

    @Value("${kafka.producer.default-partitions:3}")
    private int defaultPartitions;

    @Value("${kafka.producer.default-replication:2}")
    private short defaultReplication;

    @Value("${kafka.producer.create-topic-if-not-exists:true}")
    private boolean createTopicIfNotExists;

    private final Random random = new Random();

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Initialize the producer — called automatically by Spring
     * after all dependencies and @Value fields are injected.

     */
    @PostConstruct
    public void initialize() throws IOException {
        if (client.getTopicMetadata(topic) == null && createTopicIfNotExists) {
            log.info("Topic does not exist, creating: {}", topic);
            boolean created = client.createTopic(
                    topic, defaultPartitions, defaultReplication);
            if (!created) {
                throw new IOException("Failed to create topic: " + topic);
            }
            log.info("Topic created successfully: {}", topic);
        } else {
            log.info("Topic already exists: {}", topic);
        }
    }


    /**
     * Send a message to a random partition.
     * @return the offset where the message was stored
     */
    public long send(String message) throws IOException {
        SimpleKafkaClient.TopicMetadata metadata = client.getTopicMetadata(topic);
        if (metadata == null) {
            throw new IOException("Topic does not exist: " + topic);
        }

        int partition = random.nextInt(metadata.getPartitions().size());
        return send(message, partition);
    }

    /**
     * Send a message to a specific partition.
     * @return the offset where the message was stored
     */
    public long send(String message, int partition) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        long offset = client.send(topic, partition, data);
        log.info("Sent message to topic: {}, partition: {}, offset: {}",
                topic, partition, offset);
        return offset;
    }

    /**
     * Send raw bytes to a specific partition.
     * @return the offset where the message was stored
     */
    public long sendBytes(byte[] data, int partition) throws IOException {
        long offset = client.send(topic, partition, data);
        log.info("Sent {} bytes to topic: {}, partition: {}, offset: {}",
                data.length, topic, partition, offset);
        return offset;
    }
}