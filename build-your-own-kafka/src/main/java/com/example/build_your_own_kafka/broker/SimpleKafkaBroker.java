package com.example.build_your_own_kafka.broker;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simplified implementation of a Kafka-like broker.
 */
@Slf4j
@Service
public class SimpleKafkaBroker {

    @Value("${broker.id}")
    private int brokerId;

    @Value("${broker.host}")
    private String brokerHost;

    @Value("${broker.port}")
    private int brokerPort;

    @Value("${broker.data-dir}")
    private String dataDir;

    @Autowired
    private ZookeeperClient zkClient;

    private final Map<String, List<Partition>> topics = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isController = new AtomicBoolean(false);
    private final Map<Integer, BrokerInfo> clusterMetadata = new ConcurrentHashMap<>();
    private ServerSocketChannel serverChannel;


    /**
     * Start the broker — called automatically by Spring after all
     * dependencies are injected
     */
    @PostConstruct
    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {
            File dataDirFile = new File(dataDir + File.separator + brokerId);
            if (!dataDirFile.exists()) {
                dataDirFile.mkdirs();
            }

            serverChannel = ServerSocketChannel.open();
            serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));
            serverChannel.configureBlocking(false);

            log.info("SimpleKafka broker started on {}:{}", brokerHost, brokerPort);

            registerWithZookeeper();

            electController();

            loadTopics();

            executor.submit(this::acceptConnections);
        }
    }

    /**
     * Stop the broker — called automatically by Spring on graceful shutdown
     */
    @PreDestroy
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                log.info("Stopping SimpleKafka broker...");

                serverChannel.close();

                for (List<Partition> partitions : topics.values()) {
                    for (Partition partition : partitions) {
                        partition.close();
                    }
                }

                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);

                log.info("SimpleKafka broker stopped");
            } catch (Exception e) {
                log.error("Error stopping broker", e);
            }
        }
    }


    /**
     * Register this broker with ZooKeeper
     */
    private void registerWithZookeeper() {
        try {
            String brokerPath = "/brokers/" + brokerId;
            String brokerData = brokerHost + ":" + brokerPort;
            zkClient.createEphemeralNode(brokerPath, brokerData);

            clusterMetadata.put(brokerId, new BrokerInfo(brokerId, brokerHost, brokerPort));

            zkClient.watchChildren("/brokers", this::onBrokersChanged);

            log.info("Registered with ZooKeeper at {}", zkClient.getConnectString());
        } catch (Exception e) {
            log.error("Failed to register with ZooKeeper", e);
        }
    }

    /**
     * Handle changes in the broker list from ZooKeeper
     */
    private void onBrokersChanged(List<String> brokerIds) {
        log.info("Broker change detected. Current brokers: {}", brokerIds);

        for (String id : brokerIds) {
            try {
                int brokerId = Integer.parseInt(id);
                if (!clusterMetadata.containsKey(brokerId)) {
                    String brokerData = zkClient.getData("/brokers/" + id);
                    String[] hostPort = brokerData.split(":");
                    BrokerInfo info = new BrokerInfo(
                            brokerId,
                            hostPort[0],
                            Integer.parseInt(hostPort[1]));
                    clusterMetadata.put(brokerId, info);
                    log.info("Added broker: {}", info);
                }
            } catch (Exception e) {
                log.warn("Failed to process broker info", e);
            }
        }

        List<Integer> toRemove = new ArrayList<>();
        for (Integer id : clusterMetadata.keySet()) {
            if (!brokerIds.contains(String.valueOf(id))) {
                toRemove.add(id);
            }
        }
        toRemove.forEach(id -> {
            clusterMetadata.remove(id);
            log.info("Removed broker: {}", id);
        });

        if (!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            isController.set(false);
            log.info("This broker is no longer in the cluster, giving up controller status");
        } else if (isController.get()) {
            rebalancePartitions();
        } else {
            electController();
        }
    }

    /**
     * Participate in controller election
     */
    private void electController() {
        try {
            String controllerPath = "/controller";

            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                    log.info("Deleted empty controller node");
                }
            }

            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(
                        controllerPath, String.valueOf(brokerId));
            }

            if (becameController) {
                isController.set(true);
                log.info("This broker is now the active controller");
                rebalancePartitions();
            } else {
                String controllerId = zkClient.getData(controllerPath);
                if (controllerId == null || controllerId.trim().isEmpty()) {
                    log.warn("Controller node exists but has no data. Retrying...");
                    executor.submit(() -> {
                        try {
                            Thread.sleep(1000);
                            electController();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                    return;
                }

                log.info("Current controller is broker {}", controllerId);
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (Exception e) {
            log.error("Controller election failed", e);
            executor.submit(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    /**
     * Handle controller change notification
     */
    private void onControllerChange() {
        log.info("Controller changed, initiating new election");
        electController();
    }



    /**
     * Rebalance partitions across available brokers
     */
    private void rebalancePartitions() {
        if (!isController.get()) return;

        log.info("Rebalancing partitions across cluster");

        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            for (Partition partition : partitions) {
                if (partition.getLeader() == -1
                        || !clusterMetadata.containsKey(partition.getLeader())) {

                    List<Integer> brokers = new ArrayList<>(clusterMetadata.keySet());
                    if (!brokers.isEmpty()) {
                        int newLeader = brokers.get(0);
                        partition.setLeader(newLeader);

                        List<Integer> followers = new ArrayList<>();
                        for (int i = 1; i < Math.min(brokers.size(), 3); i++) {
                            followers.add(brokers.get(i));
                        }
                        partition.setFollowers(followers);

                        updatePartitionMetadata(topic, partition);

                        log.info("Reassigned partition {} of topic {} to leader {} with followers {}",
                                partition.getId(), topic, newLeader, followers);
                    }
                }
            }
        }
    }

    /**
     * Update partition metadata in ZooKeeper
     */
    private void updatePartitionMetadata(String topic, Partition partition) {
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            StringBuilder data = new StringBuilder(partition.getLeader() + ";");
            for (int follower : partition.getFollowers()) {
                data.append(follower).append(",");
            }

            if (zkClient.exists(path)) {
                zkClient.setData(path, data.toString());
            } else {
                zkClient.createPersistentNode(path, data.toString());
            }
        } catch (Exception e) {
            log.error("Failed to update partition metadata", e);
        }
    }


    /**
     * Load all topics from ZooKeeper
     */
    public void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");
            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    log.error("Failed to load topic: {}", topic, e);
                }
            }
            log.info("Loaded {} topics", topics.size());
        } catch (Exception e) {
            log.error("Failed to load topics", e);
        }
    }

    /**
     * Load topic metadata from ZooKeeper
     */
    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) {
            log.info("Topic already loaded: {}", topic);
            return;
        }

        String topicPath = "/topics/" + topic;
        if (!zkClient.exists(topicPath)) {
            throw new Exception("Topic does not exist in ZooKeeper: " + topic);
        }

        String topicDir = dataDir + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();

        for (String partitionId : partitionIds) {
            int id = Integer.parseInt(partitionId);
            String partitionData = zkClient.getData(topicPath + "/partitions/" + partitionId);

            String[] parts = partitionData.split(";");
            int leader = Integer.parseInt(parts[0]);

            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                for (String followerId : parts[1].split(",")) {
                    if (!followerId.isEmpty()) {
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }

            String partitionDir = topicDir + File.separator + id;
            new File(partitionDir).mkdirs();

            partitions.add(new Partition(id, leader, followers, partitionDir));
            log.info("Loaded partition {} for topic {}, leader: {}, followers: {}",
                    id, topic, leader, followers);
        }

        topics.put(topic, partitions);
        log.info("Successfully loaded topic: {} with {} partitions", topic, partitions.size());
    }

    /**
     * Create a new topic with the specified configuration
     */
    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            log.warn("Only the controller can create topics");
            return;
        }

        try {
            String topicDir = dataDir + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            String topicPath = "/topics/" + topic;
            if (!zkClient.exists(topicPath)) {
                zkClient.createPersistentNode(topicPath, "");
                zkClient.createPersistentNode(topicPath + "/partitions", "");
            }

            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());

            for (int i = 0; i < numPartitions; i++) {
                String partitionDir = topicDir + File.separator + i;
                new File(partitionDir).mkdirs();

                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);

                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++) {
                    followers.add(brokerIds.get((leaderIndex + j) % brokerIds.size()));
                }

                partitions.add(new Partition(i, leaderId, followers, partitionDir));

                StringBuilder partitionData = new StringBuilder(leaderId + ";");
                for (int follower : followers) {
                    partitionData.append(follower).append(",");
                }
                zkClient.createPersistentNode(topicPath + "/partitions/" + i,
                        partitionData.toString());

                log.info("Created partition {} for topic {} with leader {} and followers {}",
                        i, topic, leaderId, followers);
            }

            topics.put(topic, partitions);

            for (int id : brokerIds) {
                if (id != this.brokerId) {
                    notifyBrokerForTopicCreation(id, topic);
                }
            }
        } catch (Exception e) {
            log.error("Failed to create topic", e);
        }
    }

    /**
     * Notify a broker about topic creation
     */
    private void notifyBrokerForTopicCreation(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null) return;

        executor.submit(() -> {
            try (SocketChannel brokerChannel = SocketChannel.open()) {
                brokerChannel.connect(
                        new InetSocketAddress(broker.getHost(), broker.getPort()));

                ByteBuffer request = ByteBuffer.allocate(3 + topic.length());
                request.put(Protocol.TOPIC_NOTIFICATION);
                request.putShort((short) topic.length());
                request.put(topic.getBytes());
                request.flip();

                brokerChannel.write(request);

                ByteBuffer response = ByteBuffer.allocate(1);
                brokerChannel.read(response);
            } catch (IOException e) {
                log.warn("Failed to notify broker {} about topic creation", brokerId, e);
            }
        });
    }



    /**
     * Accept client connections
     */
    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);
                    log.info("Accepted connection from {}", clientChannel.getRemoteAddress());
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(100);
            } catch (Exception e) {
                if (isRunning.get()) {
                    log.error("Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Handle a client connection
     */
    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);

                if (bytesRead > 0) {
                    buffer.flip();
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    clientChannel.close();
                    break;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                log.error("Error handling client", e);
            }
        } finally {
            try {
                if (clientChannel.isOpen()) clientChannel.close();
            } catch (IOException e) {
                log.warn("Error closing client channel", e);
            }
        }
    }

    /**
     * Process client message based on wire protocol
     */
    private void processClientMessage(SocketChannel clientChannel,
                                      ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();

        switch (messageType) {
            case Protocol.PRODUCE         -> handleProduceRequest(clientChannel, buffer);
            case Protocol.FETCH           -> handleFetchRequest(clientChannel, buffer);
            case Protocol.METADATA        -> handleMetadataRequest(clientChannel, buffer);
            case Protocol.CREATE_TOPIC    -> handleCreateTopicRequest(clientChannel, buffer);
            case Protocol.REPLICATE       -> handleReplicateRequest(clientChannel, buffer);
            case Protocol.TOPIC_NOTIFICATION -> handleTopicNotification(clientChannel, buffer);
            default -> {
                log.warn("Unknown message type: {}", messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
            }
        }
    }



    private void handleProduceRequest(SocketChannel clientChannel,
                                      ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        log.info("Produce request for topic: {}, partition: {}", topic, partitionId);

        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        Partition targetPartition = findPartition(topic, partitionId);
        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        if (targetPartition.getLeader() != brokerId) {
            forwardProduceToLeader(clientChannel, topic, partitionId,
                    message, targetPartition.getLeader());
            return;
        }

        long offset = targetPartition.append(message);
        replicateToFollowers(topic, targetPartition, message, offset);

        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) (offset > -1 ? 0 : 1));
        response.flip();
        clientChannel.write(response);
    }

    private void forwardProduceToLeader(SocketChannel clientChannel, String topic,
                                        int partition, byte[] message, int leaderId) throws IOException {
        BrokerInfo leader = clusterMetadata.get(leaderId);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
            return;
        }

        try (SocketChannel leaderChannel = SocketChannel.open()) {
            leaderChannel.connect(
                    new InetSocketAddress(leader.getHost(), leader.getPort()));

            ByteBuffer request = ByteBuffer.allocate(9 + topic.length() + message.length);
            request.put(Protocol.PRODUCE);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(partition);
            request.putInt(message.length);
            request.put(message);
            request.flip();

            leaderChannel.write(request);

            ByteBuffer response = ByteBuffer.allocate(10);
            leaderChannel.read(response);
            response.flip();
            clientChannel.write(response);
        } catch (IOException e) {
            log.error("Failed to forward produce request to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
        }
    }

    private void replicateToFollowers(String topic, Partition partition,
                                      byte[] message, long offset) {
        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId) continue;

            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) continue;

            executor.submit(() -> {
                try (SocketChannel followerChannel = SocketChannel.open()) {
                    followerChannel.connect(
                            new InetSocketAddress(follower.getHost(), follower.getPort()));

                    ByteBuffer request = ByteBuffer.allocate(
                            17 + topic.length() + message.length);
                    request.put(Protocol.REPLICATE);
                    request.putShort((short) topic.length());
                    request.put(topic.getBytes());
                    request.putInt(partition.getId());
                    request.putLong(offset);
                    request.putInt(message.length);
                    request.put(message);
                    request.flip();

                    followerChannel.write(request);

                    ByteBuffer response = ByteBuffer.allocate(1);
                    followerChannel.read(response);
                    response.flip();

                    byte ack = response.get();
                    log.info("Replication to follower {} {}",
                            followerId,
                            ack == Protocol.REPLICATE_ACK ? "succeeded" : "failed");
                } catch (IOException e) {
                    log.error("Replication to follower {} failed", followerId, e);
                }
            });
        }
    }

    private void handleReplicateRequest(SocketChannel clientChannel,
                                        ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        log.info("Replication request for topic: {}, partition: {}, offset: {}",
                topic, partitionId, offset);

        Partition targetPartition = findPartition(topic, partitionId);

        ByteBuffer response = ByteBuffer.allocate(1);
        if (targetPartition != null) {
            targetPartition.append(message);
            response.put(Protocol.REPLICATE_ACK);
        } else {
            response.put((byte) 0); // failed
        }
        response.flip();
        clientChannel.write(response);
    }

    private void handleFetchRequest(SocketChannel clientChannel,
                                    ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        log.info("Fetch request for topic: {}, partition: {}, offset: {}, maxBytes: {}",
                topic, partitionId, offset, maxBytes);

        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        Partition targetPartition = findPartition(topic, partitionId);
        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        if (offset >= targetPartition.getLogEndOffset()) {
            ByteBuffer response = ByteBuffer.allocate(5);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0);
            response.flip();
            clientChannel.write(response);
            return;
        }

        List<byte[]> messages = targetPartition.readMessages(offset, maxBytes);

        int totalSize = 5;
        for (byte[] msg : messages) totalSize += 12 + msg.length;

        ByteBuffer response = ByteBuffer.allocate(totalSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset++);
            response.putInt(msg.length);
            response.put(msg);
        }
        response.flip();
        clientChannel.write(response);
    }

    private void handleMetadataRequest(SocketChannel clientChannel,
                                       ByteBuffer buffer) throws IOException {
        int size = 5;
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            size += 6 + entry.getKey().length();
            size += entry.getValue().size() * 12;
            for (Partition partition : entry.getValue()) {
                size += partition.getFollowers().size() * 4;
            }
        }
        size += 4;
        for (BrokerInfo broker : clusterMetadata.values()) {
            size += 10 + broker.getHost().length();
        }

        ByteBuffer response = ByteBuffer.allocate(size);
        response.put(Protocol.METADATA_RESPONSE);

        response.putInt(clusterMetadata.size());
        for (BrokerInfo broker : clusterMetadata.values()) {
            response.putInt(broker.getId());
            response.putShort((short) broker.getHost().length());
            response.put(broker.getHost().getBytes());
            response.putInt(broker.getPort());
        }

        response.putInt(topics.size());
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            response.putShort((short) topic.length());
            response.put(topic.getBytes());
            response.putInt(partitions.size());

            for (Partition partition : partitions) {
                response.putInt(partition.getId());
                response.putInt(partition.getLeader());
                List<Integer> followers = partition.getFollowers();
                response.putInt(followers.size());
                for (Integer follower : followers) response.putInt(follower);
            }
        }
        response.flip();
        clientChannel.write(response);
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel,
                                          ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        log.info("Create topic request: {}, partitions: {}, replication: {}",
                topic, numPartitions, replicationFactor);

        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists");
            return;
        }

        if (numPartitions <= 0 || replicationFactor <= 0
                || replicationFactor > clusterMetadata.size()) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration");
            return;
        }

        if (isController.get()) {
            createTopic(topic, numPartitions, replicationFactor);

            ByteBuffer response = ByteBuffer.allocate(2);
            response.put(Protocol.CREATE_TOPIC_RESPONSE);
            response.put((byte) 0);
            response.flip();
            clientChannel.write(response);
        } else {
            forwardCreateTopicToController(clientChannel, topic,
                    numPartitions, replicationFactor);
        }
    }

    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic,
                                                int numPartitions, short replicationFactor) throws IOException {
        int controllerId = -1;
        try {
            controllerId = Integer.parseInt(zkClient.getData("/controller"));
        } catch (Exception e) {
            log.error("Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available");
            return;
        }

        try (SocketChannel controllerChannel = SocketChannel.open()) {
            controllerChannel.connect(
                    new InetSocketAddress(controller.getHost(), controller.getPort()));

            ByteBuffer request = ByteBuffer.allocate(9 + topic.length());
            request.put(Protocol.CREATE_TOPIC);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(numPartitions);
            request.putShort(replicationFactor);
            request.flip();

            controllerChannel.write(request);

            ByteBuffer response = ByteBuffer.allocate(2);
            controllerChannel.read(response);
            response.flip();
            clientChannel.write(response);
        } catch (IOException e) {
            log.error("Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
        }
    }

    private void handleTopicNotification(SocketChannel clientChannel,
                                         ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        log.info("Received topic notification for: {}", topic);

        ByteBuffer response = ByteBuffer.allocate(1);
        try {
            loadTopic(topic);
            response.put((byte) 0); // success
        } catch (Exception e) {
            log.error("Failed to load topic: {}", topic, e);
            response.put((byte) 1); // error
        }
        response.flip();
        clientChannel.write(response);
    }



    private Partition findPartition(String topic, int partitionId) {
        List<Partition> partitions = topics.get(topic);
        if (partitions == null) return null;
        return partitions.stream()
                .filter(p -> p.getId() == partitionId)
                .findFirst()
                .orElse(null);
    }
}