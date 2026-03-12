package com.example.build_your_own_kafka.client;

import com.example.build_your_own_kafka.broker.BrokerInfo;
import com.example.build_your_own_kafka.broker.Protocol;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client for interacting with Build Your Own Kafka brokers.
 */
@Slf4j
@Service
public class SimpleKafkaClient {

    @Value("${kafka.bootstrap.host}")
    private String bootstrapBroker;

    @Value("${kafka.bootstrap.port}")
    private int bootstrapPort;

    @Value("${kafka.buffer.size:4096}")
    private int defaultBufferSize;

    private final Map<String, TopicMetadata> topicMetadata = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final AtomicInteger correlationId = new AtomicInteger(0);


    /**
     * Initialize the client by fetching cluster metadata.
     */
    @PostConstruct
    public void initialize() throws IOException {
        refreshMetadata();
    }


    /**
     * Refresh metadata about brokers and topics from the cluster
     */
    public void refreshMetadata() throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(bootstrapBroker, bootstrapPort));

            channel.write(Protocol.encodeMetadataRequest());

            ByteBuffer response = ByteBuffer.allocate(defaultBufferSize);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response);
            if (!result.isSuccess()) {
                throw new IOException("Failed to fetch metadata: " + result.getError());
            }

            brokers.clear();
            result.getBrokers().forEach(broker ->
                    brokers.put(broker.getId(),
                            new BrokerInfo(broker.getId(), broker.getHost(), broker.getPort())));

            topicMetadata.clear();
            result.getTopics().forEach(topic -> {
                List<PartitionInfo> partitions = topic.getPartitions().stream()
                        .map(p -> new PartitionInfo(p.getId(), p.getLeader(), p.getReplicas()))
                        .toList();
                topicMetadata.put(topic.getName(),
                        new TopicMetadata(topic.getName(), partitions));
            });

            log.info("Metadata refreshed: {} brokers, {} topics",
                    brokers.size(), topicMetadata.size());
        }
    }


    /**
     * Create a new topic in the cluster
     */
    public boolean createTopic(String topic, int numPartitions,
                               short replicationFactor) throws IOException {
        if (brokers.isEmpty()) {
            refreshMetadata();
            if (brokers.isEmpty()) {
                throw new IOException("No brokers available");
            }
        }

        BrokerInfo broker = brokers.values().iterator().next();

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));

            channel.write(Protocol.encodeCreateTopicRequest(
                    topic, numPartitions, replicationFactor));

            ByteBuffer response = ByteBuffer.allocate(defaultBufferSize);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            byte responseType = response.get();
            if (responseType != Protocol.CREATE_TOPIC_RESPONSE) {
                if (responseType == Protocol.ERROR_RESPONSE) {
                    short errorLength = response.getShort();
                    byte[] errorBytes = new byte[errorLength];
                    response.get(errorBytes);
                    log.warn("Error creating topic: {}", new String(errorBytes));
                    return false;
                }
                throw new IOException("Invalid create topic response type: " + responseType);
            }

            boolean success = response.get() == 0;
            if (success) {
                refreshMetadata(); // update local metadata with new topic
            }
            return success;
        }
    }


    /**
     * Send a message to a topic partition.
     */
    public long send(String topic, int partition, byte[] message) throws IOException {
        BrokerInfo leader = findLeader(topic, partition);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            channel.write(Protocol.encodeProduceRequest(topic, partition, message));

            ByteBuffer response = ByteBuffer.allocate(defaultBufferSize);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);
            if (!result.isSuccess()) {
                throw new IOException("Failed to produce message: " + result.getError());
            }

            return result.getOffset();
        }
    }

    /**
     * Fetch messages from a topic partition starting at a given offset
     */
    public List<byte[]> fetch(String topic, int partition,
                              long offset, int maxBytes) throws IOException {
        BrokerInfo leader = findLeader(topic, partition);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            channel.write(Protocol.encodeFetchRequest(topic, partition, offset, maxBytes));

            ByteBuffer response = ByteBuffer.allocate(defaultBufferSize);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            Protocol.FetchResult result = Protocol.decodeFetchResponse(response);
            if (!result.isSuccess()) {
                throw new IOException("Failed to fetch messages: " + result.getError());
            }

            return List.of(result.getMessages());
        }
    }


    public Map<String, TopicMetadata> getTopicMetadata() {
        return new HashMap<>(topicMetadata);
    }

    public TopicMetadata getTopicMetadata(String topic) {
        return topicMetadata.get(topic);
    }

    public Map<Integer, BrokerInfo> getBrokers() {
        return new HashMap<>(brokers);
    }


    /**
     * Find the leader broker for a given topic-partition.
     * Refreshes metadata automatically if not found.
     */
    private BrokerInfo findLeader(String topic, int partition) throws IOException {
        if (!topicMetadata.containsKey(topic)) {
            refreshMetadata();
            if (!topicMetadata.containsKey(topic)) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        PartitionInfo partitionInfo = topicMetadata.get(topic)
                .getPartitions()
                .stream()
                .filter(p -> p.getId() == partition)
                .findFirst()
                .orElseThrow(() -> new IOException("Partition not found: " + partition));

        int leaderId = partitionInfo.getLeader();
        BrokerInfo leader = brokers.get(leaderId);

        if (leader == null) {
            refreshMetadata();
            leader = brokers.get(leaderId);
            if (leader == null) {
                throw new IOException("Leader broker not found: " + leaderId);
            }
        }

        return leader;
    }


    @Getter
    @AllArgsConstructor
    @ToString
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionInfo> partitions;

        public List<PartitionInfo> getPartitions() {
            return new ArrayList<>(partitions);
        }
    }

    @Getter
    @AllArgsConstructor
    @ToString
    public static class PartitionInfo {
        private final int id;
        private final int leader;
        private final List<Integer> followers;

        public List<Integer> getFollowers() {
            return new ArrayList<>(followers);
        }
    }
}
