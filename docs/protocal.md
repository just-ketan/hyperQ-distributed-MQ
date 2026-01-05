# HyperQ Protocol Specification

## 1. Introduction

This document defines the communication protocol between HyperQ clients (Producer/Consumer) and the Broker. Current implementation uses in-process function calls; can be extended to network-based protocols (gRPC, TCP).

---

## 2. Message Format

### 2.1 Message Structure

```
Message
├── offset: uint64_t          // Position in partition log
├── key: string               // Optional routing key
├── value: string             // Message content
├── topic: string             // Topic name
├── partition: int            // Partition ID
└── timestamp: long           // Message timestamp
```

### 2.2 Message Serialization (Future)

**Proposed JSON format for network transmission:**

```json
{
  "type": "Message",
  "offset": 42,
  "key": "customer_123",
  "value": "Order placed: 5 items",
  "topic": "orders",
  "partition": 2,
  "timestamp": 1673020800000
}
```

---

## 3. Producer Protocol

### 3.1 Produce Request

**Method:** `Broker::produce(topic, message, key)`

**Parameters:**
```cpp
string topic;           // Target topic name
string message;         // Message content
string key;            // Optional routing key (default: "")
```

**Request Flow:**
```
Producer
  │
  ├─ Select partition:
  │   ├─ if key.empty(): partition = partition_counter++ % num_partitions
  │   └─ else: partition = hash(key) % num_partitions
  │
  └─ Append to partition
     └─ Commit to persistent log
```

### 3.2 Produce Response

**Response Structure:**
```cpp
struct ProduceResponse {
    bool success;               // true if message accepted
    string topic;               // Echoed topic name
    int partition;              // Assigned partition
    uint64_t offset;            // Assigned offset in partition
    string error_message;       // Error details if !success
};
```

**Success Response Example:**
```json
{
  "success": true,
  "topic": "orders",
  "partition": 2,
  "offset": 1042,
  "error_message": ""
}
```

**Error Response Example:**
```json
{
  "success": false,
  "topic": "orders",
  "partition": -1,
  "offset": 0,
  "error_message": "Topic 'orders' does not exist"
}
```

### 3.3 Produce Error Codes

| Code | Meaning | Recovery |
|------|---------|----------|
| TOPIC_NOT_FOUND | Topic doesn't exist | Create topic first |
| PARTITION_NOT_LEADER | Non-leader write (future) | Retry to leader |
| WRITE_FAILED | Disk write error | Retry or escalate |

---

## 4. Consumer Protocol

### 4.1 Fetch Request

**Method:** `Broker::consume(topic, partition, group_id, offset)`

**Parameters:**
```cpp
string topic;           // Source topic
int partition;          // Source partition
string group_id;        // Consumer group ID
uint64_t offset;        // Starting offset (0 = last committed)
```

**Request Flow:**
```
Consumer
  │
  ├─ If offset == 0:
  │   └─ Fetch last committed offset from Coordinator
  │
  ├─ Read messages from partition starting at offset
  │
  ├─ Calculate consumer lag
  │   └─ lag = high_watermark - (offset + batch_size)
  │
  └─ Return messages to client
```

### 4.2 Fetch Response

**Response Structure:**
```cpp
struct FetchResponse {
    bool success;                  // true if messages fetched
    string topic;                  // Echoed topic
    int partition;                 // Echoed partition
    vector<Message> messages;      // Fetched messages
    uint64_t next_offset;          // Next offset to fetch
    uint64_t consumer_lag;         // Messages behind
    string error_message;          // Error details if !success
};
```

**Success Response Example:**
```json
{
  "success": true,
  "topic": "orders",
  "partition": 1,
  "messages": [
    {
      "offset": 10,
      "key": "customer_5",
      "value": "Order #101: $199.99",
      "timestamp": 1673020800000
    },
    {
      "offset": 11,
      "key": "customer_3",
      "value": "Order #102: $50.00",
      "timestamp": 1673020801000
    }
  ],
  "next_offset": 12,
  "consumer_lag": 3,
  "error_message": ""
}
```

**Error Response Example:**
```json
{
  "success": false,
  "topic": "orders",
  "partition": 1,
  "messages": [],
  "next_offset": 0,
  "consumer_lag": 0,
  "error_message": "Partition 1 does not exist"
}
```

### 4.3 Fetch Error Codes

| Code | Meaning | Recovery |
|------|---------|----------|
| TOPIC_NOT_FOUND | Topic doesn't exist | Create topic first |
| PARTITION_NOT_FOUND | Partition doesn't exist | Use valid partition |
| OFFSET_OUT_OF_RANGE | Offset beyond log end | Use valid offset |
| CONSUMER_OFFSET_NOT_COMMITTED | No offset committed | Fetch from 0 |

---

## 5. Consumer Group Protocol

### 5.1 Join Group Request

**Method:** `Broker::get_coordinator().join_group(group_id, consumer_id, topics)`

**Parameters:**
```cpp
string group_id;                    // Consumer group name
string consumer_id;                 // Unique consumer identifier
vector<string> topics;              // Topics to subscribe
```

**Semantics:**
```
Consumer joins group
  │
  ├─ Add consumer to group membership
  ├─ Create offset tracking structure
  └─ Subscribe to topic partitions
```

### 5.2 Commit Offset Request

**Method:** `Coordinator::commit_offset(group_id, topic, partition, offset)`

**Parameters:**
```cpp
string group_id;        // Consumer group
string topic;           // Topic name
int partition;          // Partition ID
uint64_t offset;        // Offset to commit
```

**Storage:**
```cpp
map<string,           // group_id
    map<string,       // topic
        map<int,      // partition
            uint64_t  // committed_offset
        >
    >
> offsets_;
```

### 5.3 Get Committed Offset Request

**Method:** `Coordinator::get_offset(group_id, topic, partition)`

**Returns:** Last committed offset for this group/topic/partition

**Semantics:**
```
If offset found: return offset
Else: return 0 (read from beginning)
```

### 5.4 Get Consumer Lag Request

**Method:** `Coordinator::get_consumer_lag(group_id, topic, partition, latest_offset)`

**Returns:** `latest_offset - committed_offset`

**Interpretation:**
```
lag = 0  → Consumer is caught up
lag > 0  → Consumer is behind (more messages to read)
lag < 0  → Invalid state (should not occur)
```

---

## 6. Broker State Management

### 6.1 Topic State

```
Topics
├── Topic1
│   ├── Partition 0
│   │   ├── High Watermark: 100
│   │   ├── Leader: Broker 1
│   │   └── Replicas: [Broker 2, Broker 3]
│   ├── Partition 1
│   │   ├── High Watermark: 95
│   │   ├── Leader: Broker 2
│   │   └── Replicas: [Broker 1, Broker 3]
│   └── Partition 2
│       ├── High Watermark: 102
│       ├── Leader: Broker 3
│       └── Replicas: [Broker 1, Broker 2]
└── Topic2
    └── ...
```

### 6.2 Consumer Group State

```
Consumer Groups
├── Group1 (payment-service)
│   ├── Consumer1 (processor-1)
│   │   └── Subscriptions: [orders]
│   ├── Consumer2 (processor-2)
│   │   └── Subscriptions: [orders, payments]
│   └── Offsets
│       ├── orders:0 → 42
│       ├── orders:1 → 38
│       ├── payments:0 → 100
│       └── payments:1 → 95
└── Group2 (analytics)
    └── ...
```

---

## 7. Ordering Guarantees

### 7.1 Per-Partition Ordering

**Guarantee:** Messages within a partition are processed in order

```
Partition 0: [msg1, msg2, msg3, msg4, msg5]
              ↑     ↑     ↑     ↑     ↑
          offset 0  1     2     3     4

Consumer reads in order: msg1 → msg2 → msg3 → msg4 → msg5
```

### 7.2 Key-Based Ordering

**Guarantee:** Messages with same key always go to same partition

```
Messages:
  - {key: "customer_1", value: "order1"}  → Partition 1
  - {key: "customer_2", value: "order1"}  → Partition 0
  - {key: "customer_1", value: "order2"}  → Partition 1  ← Same partition!

Result: Customer orders processed in order
```

### 7.3 No Ordering Guarantee Across Partitions

```
Partition 0: [A, B, C]
Partition 1: [D, E, F]

Consumer may receive: [A, D, B, E, C, F] or [D, A, E, B, F, C]
No global ordering across partitions
```

---

## 8. Durability Guarantees

### 8.1 Persistence Model

```
Producer sends message
        │
        ▼
   In-memory buffer
        │
        ▼
   Disk write (fsync)
        │
        ▼
   Return ProduceResponse (success)
        │
        ▼
   Producer receives confirmation
```

### 8.2 Consumer Offset Durability

**Current:** In-memory (lost on restart)
**Future:** Persistent to disk or separate log

```
Consumer commits offset
        │
        ▼
   Coordinator updates in-memory map
        │
        ▼
   Consumer can safely re-start
        │
        ▼
   Rejoins group and continues from saved offset
```

---

## 9. Error Handling

### 9.1 Error Propagation

**Producer Errors:**
```
try {
    auto response = broker.produce(topic, message, key);
    if (!response.success) {
        // Handle error: response.error_message
    }
} catch (const exception& e) {
    // Critical failure
}
```

**Consumer Errors:**
```
try {
    auto response = broker.consume(topic, partition, group, offset);
    if (!response.success) {
        // Handle error: response.error_message
    } else {
        // Process response.messages
    }
} catch (const exception& e) {
    // Critical failure
}
```

### 9.2 Error Recovery

| Error | Recovery Strategy |
|-------|-------------------|
| TOPIC_NOT_FOUND | Admin creates topic |
| PARTITION_NOT_FOUND | Admin creates partition |
| OFFSET_OUT_OF_RANGE | Consumer fetches from beginning (0) |
| WRITE_FAILED | Producer retries or fails over |

---

## 10. Network Protocol (Future)

### 10.1 Proposed gRPC Service Definition

```protobuf
service HyperQ {
  rpc Produce(ProduceRequest) returns (ProduceResponse);
  rpc Fetch(FetchRequest) returns (FetchResponse);
  rpc CommitOffset(CommitOffsetRequest) returns (CommitOffsetResponse);
  rpc GetOffset(GetOffsetRequest) returns (GetOffsetResponse);
  rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
}
```

### 10.2 Network Message Format

```json
{
  "request_id": 123,
  "timestamp": 1673020800000,
  "operation": "Produce",
  "payload": {
    "topic": "orders",
    "message": "Order placed",
    "key": "customer_1"
  }
}
```

---

## 11. Performance Characteristics

### 11.1 Latency Profile

| Operation | Typical Latency |
|-----------|-----------------|
| Produce | <1ms |
| Fetch | <1ms |
| Commit Offset | <0.1ms |
| Join Group | <0.5ms |

### 11.2 Throughput

| Workload | Throughput |
|----------|-----------|
| Small messages (1KB) | ~10K msgs/sec |
| Large messages (1MB) | ~100 msgs/sec |
| Batch fetches | ~50K msgs/sec |

---

## 12. Backward Compatibility

### 12.1 Version Compatibility

**Current Version:** 1.0
**Status:** Stable

**Future Versions:**
- 1.1: Compression support
- 2.0: Network protocol + multi-broker
- 2.1: Transactions

**Compatibility Guarantee:** 
- Minor versions (1.x) backward compatible
- Major versions (x.0) may break compatibility

---

## 13. Security Considerations

### 13.1 Current Security Level

**Authentication:** None
**Encryption:** None
**Authorization:** None

### 13.2 Future Security Enhancements

1. **SASL/SCRAM** for authentication
2. **TLS** for encryption in transit
3. **ACLs** for fine-grained authorization
4. **Audit logging** for compliance

---

## 14. Examples

### 14.1 Simple Produce-Consume

```cpp
// Producer sends message
Broker broker(1, "/tmp/hyperq");
broker.create_topic("orders", 2, 1);

Producer producer(broker);
auto produce_resp = producer.send("orders", "Order #123", "customer_1");
// Returns: { success: true, partition: 1, offset: 0 }

// Consumer reads message
Consumer consumer(broker, "payment-group");
auto consume_resp = consumer.consume("orders", 1);
// Returns: { success: true, messages: [Order #123], next_offset: 1 }
```

### 14.2 Consumer Group

```cpp
// Multiple consumers in same group
Consumer c1(broker, "payment-group", "processor-1");
Consumer c2(broker, "payment-group", "processor-2");

// Consume from partition 0
c1.consume("orders", 0);

// Consume from partition 1
c2.consume("orders", 1);

// Both have offset tracking in group "payment-group"
```

### 14.3 Offset Management

```cpp
// Consumer processes messages
auto response = consumer.consume("orders", 0);
// Process messages...
// Offset automatically committed after consume

// On restart
Consumer new_consumer(broker, "payment-group");
auto response = new_consumer.consume("orders", 0);
// Automatically resumes from committed offset
```

---

## 15. Conclusion

HyperQ protocol provides:
- ✅ Simple request/response semantics
- ✅ Reliable message delivery
- ✅ Consumer group offset tracking
- ✅ Error handling and recovery
- ✅ Extensible to network protocols

**Version:** 1.0  
**Status:** Stable  
**Last Updated:** January 5, 2026
