package com.example.build_your_own_kafka.controller;

import com.example.build_your_own_kafka.client.SimpleKafkaClient;
import com.example.build_your_own_kafka.client.SimpleKafkaConsumer;
import com.example.build_your_own_kafka.client.SimpleKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST Controller for testing the Kafka system.
 * Exposes all core operations over HTTP.
 */
@Slf4j
@RestController
@RequestMapping("/kafka")
public class KafkaTestController {

    private final SimpleKafkaProducer producer;
    private final SimpleKafkaConsumer consumer;
    private final SimpleKafkaClient client;

    public KafkaTestController(SimpleKafkaProducer producer,
                               SimpleKafkaConsumer consumer,
                               SimpleKafkaClient client) {
        this.producer = producer;
        this.consumer = consumer;
        this.client = client;
    }

    // -------------------------------------------------------------------------
    // 1. CLUSTER STATUS
    // -------------------------------------------------------------------------

    /**
     * GET /kafka/status
     * Shows the full cluster state — brokers, topics, partitions
     */
    @GetMapping("/status")
    public Map<String, Object> clusterStatus() {
        Map<String, Object> status = new HashMap<>();
        try {
            client.refreshMetadata();
            status.put("brokers", client.getBrokers());
            status.put("topics", client.getTopicMetadata());
            status.put("brokerCount", client.getBrokers().size());
            status.put("topicCount", client.getTopicMetadata().size());
            status.put("healthy", true);
        } catch (Exception e) {
            status.put("healthy", false);
            status.put("error", e.getMessage());
            log.error("Failed to get cluster status", e);
        }
        return status;
    }

    // -------------------------------------------------------------------------
    // 2. TOPIC MANAGEMENT
    // -------------------------------------------------------------------------

    /**
     * POST /kafka/topics
     * Creates a new topic
     * Body: { "name": "orders", "partitions": 3, "replication": 2 }
     */
    @PostMapping("/topics")
    public Map<String, Object> createTopic(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String topic = (String) request.get("name");
            int partitions = (int) request.get("partitions");
            short replication = ((Integer) request.get("replication")).shortValue();

            boolean created = client.createTopic(topic, partitions, replication);
            response.put("success", created);
            response.put("topic", topic);
            response.put("partitions", partitions);
            response.put("replication", replication);
            response.put("message", created
                    ? "Topic created successfully"
                    : "Topic creation failed");
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            log.error("Failed to create topic", e);
        }
        return response;
    }

    // -------------------------------------------------------------------------
    // 3. PRODUCE MESSAGES
    // -------------------------------------------------------------------------

    /**
     * POST /kafka/produce
     * Sends a single message to a random partition
     * Body: { "message": "Hello Kafka!" }
     */
    @PostMapping("/produce")
    public Map<String, Object> produce(@RequestBody Map<String, String> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String message = request.get("message");
            long offset = producer.send(message);
            response.put("success", true);
            response.put("message", message);
            response.put("offset", offset);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            log.error("Failed to produce message", e);
        }
        return response;
    }

    /**
     * POST /kafka/produce/batch
     * Sends multiple messages and returns all offsets
     * Body: { "messages": ["msg1", "msg2", "msg3"] }
     */
    @PostMapping("/produce/batch")
    public Map<String, Object> produceBatch(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();
        try {
            @SuppressWarnings("unchecked")
            List<String> messages = (List<String>) request.get("messages");

            for (String message : messages) {
                Map<String, Object> result = new HashMap<>();
                try {
                    long offset = producer.send(message);
                    result.put("message", message);
                    result.put("offset", offset);
                    result.put("success", true);
                } catch (Exception e) {
                    result.put("message", message);
                    result.put("success", false);
                    result.put("error", e.getMessage());
                }
                results.add(result);
            }

            response.put("results", results);
            response.put("total", messages.size());
            response.put("successful", results.stream()
                    .filter(r -> (boolean) r.get("success")).count());
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    /**
     * POST /kafka/produce/partition/{partitionId}
     * Sends a message to a SPECIFIC partition
     * Body: { "message": "Hello Partition 0!" }
     */
    @PostMapping("/produce/partition/{partitionId}")
    public Map<String, Object> produceToPartition(
            @PathVariable int partitionId,
            @RequestBody Map<String, String> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String message = request.get("message");
            long offset = producer.send(message, partitionId);
            response.put("success", true);
            response.put("message", message);
            response.put("partition", partitionId);
            response.put("offset", offset);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            log.error("Failed to produce to partition {}", partitionId, e);
        }
        return response;
    }

    // -------------------------------------------------------------------------
    // 4. CONSUME MESSAGES
    // -------------------------------------------------------------------------

    /**
     * GET /kafka/consume?offset=0&maxBytes=1048576
     * Fetches messages from the current consumer position
     */
    @GetMapping("/consume")
    public Map<String, Object> consume(
            @RequestParam(defaultValue = "0") long offset,
            @RequestParam(defaultValue = "1048576") int maxBytes) {
        Map<String, Object> response = new HashMap<>();
        try {
            consumer.seek(offset);
            List<byte[]> rawMessages = consumer.poll();

            List<String> messages = rawMessages.stream()
                    .map(m -> new String(m, StandardCharsets.UTF_8))
                    .toList();

            response.put("success", true);
            response.put("messages", messages);
            response.put("count", messages.size());
            response.put("startOffset", offset);
            response.put("nextOffset", consumer.getCurrentOffset());
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            log.error("Failed to consume messages", e);
        }
        return response;
    }

    /**
     * GET /kafka/consume/all
     * Reads ALL messages from offset 0
     */
    @GetMapping("/consume/all")
    public Map<String, Object> consumeAll() {
        Map<String, Object> response = new HashMap<>();
        List<String> allMessages = new ArrayList<>();
        try {
            consumer.seekToBeginning();

            List<byte[]> batch;
            do {
                batch = consumer.poll();
                batch.stream()
                        .map(m -> new String(m, StandardCharsets.UTF_8))
                        .forEach(allMessages::add);
            } while (!batch.isEmpty());

            response.put("success", true);
            response.put("messages", allMessages);
            response.put("totalCount", allMessages.size());
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            log.error("Failed to consume all messages", e);
        }
        return response;
    }

    /**
     * GET /kafka/consume/range?from=0&to=10
     * Reads messages between two specific offsets
     */
    @GetMapping("/consume/range")
    public Map<String, Object> consumeRange(
            @RequestParam long from,
            @RequestParam long to) {
        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> messages = new ArrayList<>();
        try {
            consumer.seek(from);
            long currentOffset = from;

            while (currentOffset < to) {
                List<byte[]> batch = consumer.poll();
                if (batch.isEmpty()) break;

                for (byte[] msg : batch) {
                    if (currentOffset >= to) break;
                    Map<String, Object> msgInfo = new HashMap<>();
                    msgInfo.put("offset", currentOffset);
                    msgInfo.put("message",
                            new String(msg, StandardCharsets.UTF_8));
                    messages.add(msgInfo);
                    currentOffset++;
                }
            }

            response.put("success", true);
            response.put("messages", messages);
            response.put("from", from);
            response.put("to", to);
            response.put("count", messages.size());
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    // -------------------------------------------------------------------------
    // 5. FAULT TOLERANCE TESTING
    // -------------------------------------------------------------------------

    /**
     * POST /kafka/test/fault-tolerance
     * Sends messages, simulates checking replication
     * Body: { "messageCount": 10 }
     */
    @PostMapping("/test/fault-tolerance")
    public Map<String, Object> testFaultTolerance(
            @RequestBody Map<String, Integer> request) {
        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            int count = request.get("messageCount");

            for (int i = 0; i < count; i++) {
                String message = "FaultTest-Message-" + i
                        + "-timestamp-" + System.currentTimeMillis();
                Map<String, Object> result = new HashMap<>();
                try {
                    long offset = producer.send(message);
                    result.put("message", message);
                    result.put("offset", offset);
                    result.put("produced", true);
                } catch (Exception e) {
                    result.put("message", message);
                    result.put("produced", false);
                    result.put("error", e.getMessage());
                }
                results.add(result);
            }

            // Verify all messages are readable
            consumer.seekToBeginning();
            List<byte[]> consumed = new ArrayList<>();
            List<byte[]> batch;
            do {
                batch = consumer.poll();
                consumed.addAll(batch);
            } while (!batch.isEmpty());

            long successCount = results.stream()
                    .filter(r -> (boolean) r.get("produced")).count();

            response.put("messagesSent", count);
            response.put("messagesSuccessful", successCount);
            response.put("messagesReadBack", consumed.size());
            response.put("dataConsistent",
                    consumed.size() >= successCount);
            response.put("results", results);
            response.put("clusterMetadata", client.getBrokers());

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            log.error("Fault tolerance test failed", e);
        }
        return response;
    }

    /**
     * GET /kafka/test/replication
     * Verifies data is replicated by checking partition metadata
     */
    @GetMapping("/test/replication")
    public Map<String, Object> testReplication() {
        Map<String, Object> response = new HashMap<>();
        try {
            client.refreshMetadata();
            Map<String, Object> replicationStatus = new HashMap<>();

            client.getTopicMetadata().forEach((topicName, metadata) -> {
                List<Map<String, Object>> partitionInfo = new ArrayList<>();
                metadata.getPartitions().forEach(partition -> {
                    Map<String, Object> pInfo = new HashMap<>();
                    pInfo.put("partitionId", partition.getId());
                    pInfo.put("leader", partition.getLeader());
                    pInfo.put("followers", partition.getFollowers());
                    pInfo.put("replicationFactor",
                            1 + partition.getFollowers().size());
                    pInfo.put("fullyReplicated",
                            !partition.getFollowers().isEmpty());
                    partitionInfo.add(pInfo);
                });
                replicationStatus.put(topicName, partitionInfo);
            });

            response.put("success", true);
            response.put("topics", replicationStatus);
            response.put("activeBrokers", client.getBrokers().size());
            response.put("replicationHealthy",
                    client.getBrokers().size() >= 2);

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    // -------------------------------------------------------------------------
    // 6. BROKER RECOVERY TESTING
    // -------------------------------------------------------------------------

    /**
     * GET /kafka/test/recovery?messagesBeforeRestart=5
     * Sends messages, tells you what to do, then verifies after recovery
     */
    @GetMapping("/test/recovery")
    public Map<String, Object> testRecovery(
            @RequestParam(defaultValue = "5") int messagesBeforeRestart) {
        Map<String, Object> response = new HashMap<>();
        List<Long> offsets = new ArrayList<>();

        try {
            // Send messages BEFORE broker restart
            for (int i = 0; i < messagesBeforeRestart; i++) {
                String message = "PreRestart-Message-" + i;
                long offset = producer.send(message);
                offsets.add(offset);
            }

            response.put("messagesSentBeforeRestart", messagesBeforeRestart);
            response.put("offsets", offsets);
            response.put("nextStep",
                    "Now restart broker-1 with: docker restart broker-1");
            response.put("thenVerify",
                    "Then call GET /kafka/test/recovery/verify to confirm data survived");
            response.put("currentBrokers", client.getBrokers());

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    /**
     * GET /kafka/test/recovery/verify
     * Verifies all messages survived after a broker restart
     */
    @GetMapping("/test/recovery/verify")
    public Map<String, Object> verifyRecovery() {
        Map<String, Object> response = new HashMap<>();
        try {
            // Refresh to see current cluster state
            client.refreshMetadata();

            // Read all messages back
            consumer.seekToBeginning();
            List<String> allMessages = new ArrayList<>();
            List<byte[]> batch;
            do {
                batch = consumer.poll();
                batch.stream()
                        .map(m -> new String(m, StandardCharsets.UTF_8))
                        .forEach(allMessages::add);
            } while (!batch.isEmpty());

            response.put("success", true);
            response.put("messagesAfterRecovery", allMessages.size());
            response.put("messages", allMessages);
            response.put("activeBrokers", client.getBrokers());
            response.put("datasurvived", !allMessages.isEmpty());
            response.put("clusterHealthy",
                    client.getBrokers().size() >= 2);

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }
}