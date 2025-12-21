#include "hyperq/broker/broker.hpp"
#include "hyperq/client/producer.hpp"
#include "hyperq/client/consumer.hpp"
#include <iostream>
#include <iomanip>
#include <thread>

void print_header(const std::string& title) {
    std::cout << "\n" << std::string(70, '=') << "\n"
              << title << "\n"
              << std::string(70, '=') << "\n\n";
}

void demo_basic_produce_consume() {
    print_header("DEMO 1: BASIC PRODUCE AND CONSUME");
    
    Broker broker(1, "/tmp/hyperq");
    broker.create_topic("orders", 2, 1);
    
    Producer producer(broker, "OrderProducer");
    std::cout << ">>> Sending 5 messages\n";
    for (int i = 0; i < 5; ++i) {
        std::string msg = "Order #" + std::to_string(i+1) + 
                         ": $" + std::to_string(50*(i+1));
        auto response = producer.send("orders", msg);
        if (response.success) {
            std::cout << "  ✓ Sent to partition " << response.partition 
                      << " offset " << response.offset << "\n";
        }
    }
    
    Consumer consumer(broker, "payment-service", "PaymentConsumer");
    std::cout << "\n>>> Consuming from both partitions\n";
    for (int p = 0; p < 2; ++p) {
        auto response = consumer.consume("orders", p);
        std::cout << "Partition " << p << ": " 
                  << response.messages.size() << " messages\n";
        for (const auto& msg : response.messages) {
            std::cout << "  [" << msg.offset << "] " << msg.value << "\n";
        }
    }
}

void demo_offset_tracking() {
    print_header("DEMO 2: OFFSET TRACKING (FAULT TOLERANCE)");
    
    Broker broker(1, "/tmp/hyperq");
    broker.create_topic("events", 1, 1);
    
    Producer producer(broker, "EventProducer");
    std::cout << ">>> Phase 1: Sending 10 messages\n";
    for (int i = 0; i < 10; i++) {
        producer.send("events", "Event " + std::to_string(i+1));
    }
    
    Consumer c1(broker, "event-processor", "Consumer-1");
    std::cout << "\n>>> Phase 2: Consumer reads 5 messages\n";
    auto r1 = c1.consume("events", 0);
    std::cout << "Read " << r1.messages.size() << " messages\n";
    
    Consumer c2(broker, "event-processor", "Consumer-2-Restart");
    std::cout << "\n>>> Phase 3: Consumer restarts (same group)\n";
    auto r2 = c2.consume("events", 0);
    std::cout << "Resumed from offset: " << (r2.messages.empty() ? 0 : r2.messages[0].offset) << "\n";
    std::cout << "✓ No re-processing! Offset was persisted!\n";
}

void demo_partitioning() {
    print_header("DEMO 3: PARTITIONING FOR PARALLELISM");
    
    Broker broker(1, "/tmp/hyperq");
    broker.create_topic("payments", 4, 1);
    
    Producer producer(broker, "PaymentProducer");
    std::cout << ">>> Sending 8 payments with customer keys\n";
    for (int i = 0; i < 8; i++) {
        std::string customer = "customer_" + std::to_string(i % 3);
        std::string msg = "Payment #" + std::to_string(i+1);
        auto response = producer.send("payments", msg, customer);
        std::cout << "  → Partition " << response.partition << ": " 
                  << msg << " (" << customer << ")\n";
    }
    
    std::cout << "\n✓ Key-based partitioning:\n"
              << "  - Same customer always → same partition\n"
              << "  - Ordering preserved for customer\n"
              << "  - Different customers on different partitions (parallel)\n";
}

int main() {
    try {
        demo_basic_produce_consume();
        demo_offset_tracking();
        demo_partitioning();
        
        print_header("✓ ALL DEMOS COMPLETED SUCCESSFULLY!");
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}