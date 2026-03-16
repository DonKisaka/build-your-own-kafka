package com.example.build_your_own_kafka.client;


import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer for Build Your Own Kafka.
 * Runs a background polling loop managed entirely
 * by Spring's lifecycle via @PostConstruct and @PreDestroy.
 */
@Slf4j
@Service
public class SimpleKafkaConsumer {



    private final SimpleKafkaClient client;

    public SimpleKafkaConsumer(SimpleKafkaClient client) {
        this.client = client;
    }



    @Value("${kafka.consumer.topic}")
    private String topic;

    @Value("${kafka.consumer.partition:0}")
    private int partition;

    @Value("${kafka.consumer.start-offset:0}")
    private long startOffset;

    @Value("${kafka.consumer.max-bytes:1048576}")
    private int maxBytes;

    @Value("${kafka.consumer.poll-interval-ms:100}")
    private int pollIntervalMs;



    private final AtomicLong currentOffset = new AtomicLong(0);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ExecutorService consumerExecutor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "kafka-consumer-thread");
                t.setDaemon(true); // dies automatically if app shuts down
                return t;
            });

    private MessageHandler messageHandler;


    /**
     * Validate topic exists after Spring injects all dependencies.
     * Does NOT start consuming yet — call startConsuming() to begin.
     */
    @PostConstruct
    public void initialize() throws IOException {
        currentOffset.set(startOffset);

        if (client.getTopicMetadata(topic) == null) {
            throw new IOException("Topic does not exist: " + topic);
        }

        log.info("Consumer initialized for topic: {}, partition: {}, startOffset: {}",
                topic, partition, startOffset);
    }

    /**
     * Gracefully stop the consumer loop and shut down the thread.
     * Called automatically by Spring on application shutdown.
     */
    @PreDestroy
    public void close() {
        stopConsuming();

        // Shut down executor — wait up to 5 seconds for current poll to finish
        consumerExecutor.shutdown();
        try {
            if (!consumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                consumerExecutor.shutdownNow();
                log.warn("Consumer executor did not terminate cleanly");
            }
        } catch (InterruptedException e) {
            consumerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Consumer closed for topic: {}, partition: {}", topic, partition);
    }



    /**
     * Single poll — fetch one batch of messages at the current offset.
     * @return list of raw message bytes
     */
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(
                topic, partition, currentOffset.get(), maxBytes);

        if (!messages.isEmpty()) {
            currentOffset.addAndGet(messages.size());
            log.debug("Polled {} messages from topic: {}, partition: {}, new offset: {}",
                    messages.size(), topic, partition, currentOffset.get());
        }

        return messages;
    }

    /**
     * Start consuming messages continuously in a background thread.
     * Each message is passed to the provided MessageHandler.
     */
    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            this.messageHandler = handler;

            consumerExecutor.submit(this::consumeLoop);

            log.info("Started consuming from topic: {}, partition: {}",
                    topic, partition);
        } else {
            log.warn("Consumer is already running");
        }
    }

    /**
     * Stop the background consuming loop.
     */
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopped consuming from topic: {}, partition: {}",
                    topic, partition);
        }
    }

    /**
     * Seek to a specific offset — next poll will start from here.
     */
    public void seek(long offset) {
        currentOffset.set(offset);
        log.info("Seeked to offset {} on topic: {}, partition: {}",
                offset, topic, partition);
    }

    /**
     * Seek back to the very beginning of the partition.
     */
    public void seekToBeginning() {
        seek(0);
    }



    /**
     * The core polling loop — runs on the consumer background thread.
     * Polls for messages and hands each one to the MessageHandler.
     */
    private void consumeLoop() {
        log.info("Consumer loop started for topic: {}, partition: {}", topic, partition);

        while (running.get()) {
            try {
                List<byte[]> messages = poll();

                if (!messages.isEmpty()) {
                    long batchStartOffset = currentOffset.get() - messages.size();

                    for (int i = 0; i < messages.size(); i++) {
                        long messageOffset = batchStartOffset + i;
                        try {
                            messageHandler.handle(messages.get(i), messageOffset);
                        } catch (Exception e) {
                            // Handler errors don't stop the loop
                            log.error("Error in message handler at offset {}",
                                    messageOffset, e);
                        }
                    }
                } else {
                    Thread.sleep(pollIntervalMs);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Consumer loop interrupted");
                break;
            } catch (Exception e) {
                if (running.get()) {
                    log.error("Error in consumer loop", e);
                    try {
                        Thread.sleep(pollIntervalMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        log.info("Consumer loop ended for topic: {}, partition: {}", topic, partition);
    }



    public long getCurrentOffset() { return currentOffset.get(); }
    public boolean isRunning() { return running.get(); }
    public String getTopic() { return topic; }
    public int getPartition() { return partition; }



    /**
     * Implement this to receive messages from the consumer.
     * Example usage:
     *   consumer.startConsuming((message, offset) -> {
     *       String text = new String(message, StandardCharsets.UTF_8);
     *       log.info("Received at offset {}: {}", offset, text);
     *   });
     */
    @FunctionalInterface
    public interface MessageHandler {
        void handle(byte[] message, long offset);
    }
}