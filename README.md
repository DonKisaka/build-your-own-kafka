# Build Your Own Kafka

A simplified Apache Kafka implementation built from scratch in Java to deeply understand how distributed message brokers work. This project implements the core mechanics of Kafka — a segmented commit log, a custom binary wire protocol, leader/follower replication, ZooKeeper-based coordination, and a producer/consumer client library — all wired together with Spring Boot.

## The Problem

Apache Kafka is one of the most important pieces of infrastructure in modern distributed systems, yet its internals are notoriously opaque. Documentation explains *what* Kafka does, but understanding *how* it actually works — how bytes flow from a producer to a segmented log file, how a controller assigns partition leaders, how replication keeps data safe — requires reading through a massive, highly optimized codebase.

This project strips Kafka down to its essential mechanics and rebuilds them from first principles. The goal was never to produce something production-ready, but to answer questions like:

- What does the storage layer actually look like on disk?
- How does a broker know which messages to return for a given offset?
- What happens when a broker dies and a new leader needs to be elected?
- How do brokers discover each other and coordinate topic creation?

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                        REST API Layer                          │
│                     (KafkaTestController)                      │
├──────────────────────┬─────────────────────────────────────────┤
│   SimpleKafkaProducer│      SimpleKafkaConsumer                │
├──────────────────────┴─────────────────────────────────────────┤
│                      SimpleKafkaClient                         │
│            (metadata, send, fetch, topic creation)             │
├────────────────────────────────────────────────────────────────┤
│                    Binary Wire Protocol                        │
│              (Protocol.java — encode/decode)                   │
├───────────────┬───────────────┬────────────────────────────────┤
│   Broker 1    │   Broker 2    │   Broker 3                     │
│ ┌───────────┐ │ ┌───────────┐ │ ┌───────────┐                 │
│ │ Partition  │ │ │ Partition  │ │ │ Partition  │                │
│ │ (segments) │ │ │ (segments) │ │ │ (segments) │                │
│ └───────────┘ │ └───────────┘ │ └───────────┘                 │
├───────────────┴───────────────┴────────────────────────────────┤
│                        ZooKeeper                               │
│        (broker registry, topic metadata, controller election)  │
└────────────────────────────────────────────────────────────────┘
```

The system has four distinct layers, each built bottom-up:

### 1. Storage: Segmented Log with Index Files (`Partition.java`)

Each partition stores messages in a Kafka-style segmented log. When a segment reaches 1 MB, a new one is created.

**On-disk layout:**

```
data/1/orders/0/
├── 00000000000000000000.log      # message data: [4-byte size][message bytes]...
├── 00000000000000000000.index    # offset index: [8-byte offset][8-byte file position]...
├── 00000000000000001024.log      # next segment, starting at offset 1024
└── 00000000000000001024.index
```

**Why segments + indexes?** A single monolithic log file would require scanning from the beginning to find any message. Segments let us binary-search to the right file by base offset, then use the index to jump directly to the byte position within that file. This is the same strategy real Kafka uses — the index turns an O(n) scan into an O(1) lookup.

**Why `ReadWriteLock`?** Multiple consumers can read the same partition concurrently (read lock), but only one thread can append at a time (write lock). This avoids the overhead of copying data for snapshot isolation while still preventing corruption.

**Why `force(true)` after every write?** Every append is flushed to disk immediately. This trades throughput for durability — a crash won't lose acknowledged messages. A production system would batch flushes, but for a learning project, correctness is more valuable than speed.

### 2. Wire Protocol (`Protocol.java`)

A custom binary protocol handles all communication — both client-to-broker and broker-to-broker.

| Byte Code | Message | Direction |
|-----------|---------|-----------|
| `0x01` | PRODUCE | Client -> Broker |
| `0x02` | FETCH | Client -> Broker |
| `0x03` | METADATA | Client -> Broker |
| `0x04` | CREATE_TOPIC | Client -> Broker |
| `0x11` | PRODUCE_RESPONSE | Broker -> Client |
| `0x12` | FETCH_RESPONSE | Broker -> Client |
| `0x13` | METADATA_RESPONSE | Broker -> Client |
| `0x14` | CREATE_TOPIC_RESPONSE | Broker -> Client |
| `0x1F` | ERROR_RESPONSE | Broker -> Client |
| `0x21` | REPLICATE | Leader -> Follower |
| `0x22` | REPLICATE_ACK | Follower -> Leader |
| `0x23` | TOPIC_NOTIFICATION | Controller -> Broker |

**Why a custom binary protocol instead of using Kafka's actual protocol?** Kafka's wire protocol is extremely detailed (100+ API keys, versioned request/response schemas, tagged fields). Implementing it faithfully would have consumed the entire project. A simplified protocol lets the focus stay on *system design* — how leaders forward requests, how replication flows, how metadata is exchanged — without drowning in serialization details.

**Why binary over something like JSON?** The protocol carries raw message bytes. A text-based format would require Base64-encoding every message payload, roughly doubling the on-wire size. Binary also keeps the encode/decode code straightforward: put a byte, put a short, put N bytes. No parsing ambiguity.

### 3. Cluster Coordination (`ZookeeperClient.java`, `SimpleKafkaBroker.java`)

ZooKeeper manages three things:

```
/brokers/{brokerId}                          # ephemeral — broker host:port
/topics/{topic}/partitions/{partitionId}     # persistent — leaderId;follower1,follower2
/controller                                  # ephemeral — which broker is controller
```

**Controller election** uses ZooKeeper's ephemeral node semantics: the first broker to successfully create `/controller` becomes the controller. When that broker dies, the ephemeral node vanishes, ZooKeeper fires a watch, and the remaining brokers race to re-create it.

**Why ZooKeeper instead of Raft/KRaft?** Real Kafka is actively migrating to KRaft (an internal Raft implementation) to remove the ZooKeeper dependency. But ZooKeeper is conceptually simpler to integrate: it gives you ephemeral nodes, watches, and atomic creates out of the box. Using it here keeps the focus on broker logic rather than consensus algorithms. It also mirrors how Kafka worked for most of its history.

**Partition rebalancing** is handled by the controller. When a broker joins or leaves (detected via ZooKeeper watches on `/brokers`), the controller scans all partitions and reassigns any that have a dead leader. Partitions are distributed round-robin across available brokers:

```
partition 0 → leader: broker 1, followers: [broker 2]
partition 1 → leader: broker 2, followers: [broker 3]
partition 2 → leader: broker 3, followers: [broker 1]
```

**Replication** is fire-and-forget: after appending a message locally, the leader sends `REPLICATE` requests to each follower asynchronously. This is a deliberate simplification — real Kafka tracks an "in-sync replica set" (ISR) and only acknowledges a produce once a configurable number of replicas have it. The trade-off here is that a leader crash immediately after acknowledging could lose the message if no follower received it yet.

### 4. Client Library (`SimpleKafkaClient.java`, `SimpleKafkaProducer.java`, `SimpleKafkaConsumer.java`)

The client is split into three layers:

- **`SimpleKafkaClient`** — low-level: metadata refresh, send, fetch, topic creation. Maintains a local cache of broker addresses and topic-partition-leader mappings. Automatically refreshes metadata when a topic or leader isn't found.
- **`SimpleKafkaProducer`** — sends string messages to a topic, picking a random partition if none is specified. Auto-creates the topic on startup if it doesn't exist.
- **`SimpleKafkaConsumer`** — offset-tracked polling consumer with a background thread. Supports `seek()`, `seekToBeginning()`, and a `MessageHandler` callback interface.

**Why doesn't the producer hash keys to partitions?** Real Kafka producers use a hash of the message key to determine the partition, guaranteeing ordering per key. This implementation uses random partition selection for simplicity — it demonstrates the produce path without introducing key-based routing.

**Why no consumer groups?** Consumer group coordination (group membership, partition assignment, offset commits) is a substantial protocol of its own in real Kafka. This project uses a single-consumer-per-partition model to keep the focus on the broker internals.

## Running the Cluster

### Prerequisites

- Docker and Docker Compose

### Start a 3-broker cluster

```bash
docker compose up --build
```

This starts:
- 1 ZooKeeper instance (port 2181)
- 3 Kafka brokers (ports 9092, 9093, 9094)

### REST API

A Spring Boot REST layer exposes every operation over HTTP for easy testing:

```bash
# Cluster status
curl http://localhost:9092/kafka/status

# Create a topic
curl -X POST http://localhost:9092/kafka/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "events", "partitions": 3, "replication": 2}'

# Produce a message
curl -X POST http://localhost:9092/kafka/produce \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello Kafka!"}'

# Produce a batch
curl -X POST http://localhost:9092/kafka/produce/batch \
  -H "Content-Type: application/json" \
  -d '{"messages": ["msg1", "msg2", "msg3"]}'

# Produce to a specific partition
curl -X POST http://localhost:9092/kafka/produce/partition/0 \
  -H "Content-Type: application/json" \
  -d '{"message": "Partition zero only"}'

# Consume from an offset
curl http://localhost:9092/kafka/consume?offset=0

# Consume all messages
curl http://localhost:9092/kafka/consume/all

# Consume a range
curl "http://localhost:9092/kafka/consume/range?from=0&to=10"

# Test replication status
curl http://localhost:9092/kafka/test/replication

# Fault tolerance test
curl -X POST http://localhost:9092/kafka/test/fault-tolerance \
  -H "Content-Type: application/json" \
  -d '{"messageCount": 10}'

# Recovery test (sends messages, then tells you to restart a broker)
curl http://localhost:9092/kafka/test/recovery
curl http://localhost:9092/kafka/test/recovery/verify
```

### Local development (single broker)

```bash
# Start ZooKeeper (e.g., via Docker)
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.9

# Run the Spring Boot app
./mvnw spring-boot:run
```

Default configuration in `application.properties` runs a single broker on `localhost:9092`.

## Project Structure

```
src/main/java/com/example/build_your_own_kafka/
├── BuildYourOwnKafkaApplication.java    # Spring Boot entry point
├── broker/
│   ├── BrokerInfo.java                  # Broker identity (id, host, port)
│   ├── Partition.java                   # Segmented log storage engine
│   ├── Protocol.java                    # Binary wire protocol
│   ├── SimpleKafkaBroker.java           # Broker server (accept, route, replicate)
│   └── ZookeeperClient.java            # ZooKeeper integration
├── client/
│   ├── SimpleKafkaClient.java           # Low-level client (metadata, send, fetch)
│   ├── SimpleKafkaProducer.java         # High-level producer
│   └── SimpleKafkaConsumer.java         # High-level consumer with polling loop
└── controller/
    └── KafkaTestController.java         # REST API for testing
```

## What's Not Implemented

This project intentionally omits significant parts of real Kafka to keep the core mechanics clear:

- **Consumer groups** — no group coordination, partition assignment, or offset commits
- **In-sync replica tracking** — replication is fire-and-forget; no ISR management
- **Kafka's actual wire protocol** — uses a simplified custom binary format
- **Transactions / exactly-once semantics** — no transactional produce or idempotent writes
- **Log compaction** — segments are append-only and never compacted
- **Authentication / authorization** — no SASL, SSL, or ACLs
- **KRaft mode** — uses ZooKeeper (mirrors Kafka's original architecture)
- **Schema registry / Kafka Connect / Kafka Streams** — ecosystem tools are out of scope

## Tech Stack

- **Java 25** with **Spring Boot 4.0.3**
- **Apache ZooKeeper 3.8.6** for cluster coordination
- **Maven** for builds
- **Docker** with a multi-stage build (Maven build -> Spring Boot layer extraction -> custom JRE via `jlink` -> Alpine)
- **Lombok** for reducing boilerplate
