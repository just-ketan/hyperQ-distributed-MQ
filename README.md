
# HyperQ Distributed Message Queue

A Kafka-like distributed message queue implementation in C++17.

## Features

- ✅ Multiple topics with partitions
- ✅ Producer and consumer APIs
- ✅ Consumer groups with offset tracking
- ✅ Exactly-once semantics
- ✅ Replication for durability
- ✅ Thread-safe operations
- ✅ Command-line tools

## Building

```bash
mkdir build && cd build
cmake ..
make -j4
```

## Running

```bash
# Demo
./hyperq-demo

# Broker
./hyperq-broker

# Producer CLI
./hyperq-producer

# Consumer CLI
./hyperq-consumer
```

## Testing

```bash
ctest --output-on-failure
```

## Documentation

See `docs/` directory for detailed documentation.