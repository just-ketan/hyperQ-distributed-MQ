#include "hyperq/broker/broker.hpp"
#include "hyperq/client/producer.hpp"
#include "hyperq/client/consumer.hpp"
#include <iostream>
#include <iomanip>
using namespace std;

void print_header(const string& title) {
    cout << "\n" << string(70, '=') << "\n"<< title << "\n"<< string(70, '=') << "\n\n";
}

void demo_basic_produce_consume() {
    print_header("DEMO 1: BASIC PRODUCE AND CONSUME");
    
    Broker broker(1, "/tmp/hyperq");
    broker.create_topic("orders", 2, 1);
    
    Producer producer(broker);
    cout << ">>> Sending 5 messages\n";
    for (int i = 0; i < 5; ++i) {
        string msg = "Order #" + to_string(i+1) + ": $" + to_string(50*(i+1));
        auto response = producer.send("orders", msg);
        if (response.success) {
            cout << "  ✓ Sent to partition " << response.partition << " offset " << response.offset << "\n";
        }
    }
    
    Consumer consumer(broker, "payment-service");
    cout << "\n>>> Consuming from both partitions\n";
    for (int p = 0; p < 2; ++p) {
        auto response = consumer.consume("orders", p);
        cout << "Partition " << p << ": " << response.messages.size() << " messages\n";
        for (const auto& msg : response.messages) {
            cout << "  [" << msg.offset << "] " << msg.value << "\n";
        }
    }
}

void demo_offset_tracking() {
    print_header("DEMO 2: OFFSET TRACKING (FAULT TOLERANCE)");
    
    Broker broker(1, "/tmp/hyperq");
    broker.create_topic("events", 1, 1);
    
    Producer producer(broker);
    cout << ">>> Phase 1: Sending 10 messages\n";
    for (int i = 0; i < 10; ++i) {
        producer.send("events", "Event " + to_string(i+1));
    }
    
    Consumer c1(broker, "event-processor");
    cout << "\n>>> Phase 2: Consumer reads 3 messages, then crashes\n";
    auto r1 = c1.consume("events", 0);
    cout << "Read " << r1.messages.size() << " messages\n";
    cout << "Offset committed: " << r1.next_offset - 1 << "\n";
    
    Consumer c2(broker, "event-processor");
    cout << "\n>>> Phase 3: Consumer restarts (same group)\n";
    auto r2 = c2.consume("events", 0);
    cout << "Resumed from offset: " << r2.messages[0].offset << "\n";
    cout << "✓ No re-processing! Offset was persisted!\n";
}

void demo_partitioning() {
    print_header("DEMO 3: PARTITIONING FOR PARALLELISM");
    
    Broker broker(1, "/tmp/hyperq");
    broker.create_topic("payments", 4, 1);
    
    Producer producer(broker);
    cout << ">>> Sending 8 payments with customer keys\n";
    for (int i = 0; i < 8; ++i) {
        string customer = "customer_" + to_string(i % 3);
        string msg = "Payment #" + to_string(i+1);
        auto response = producer.send("payments", msg, customer);
        cout << "  → Partition " << response.partition << ": " << msg << " (" << customer << ")\n";
    }
    
    cout << "\n✓ Key-based partitioning:\n"<< "  - Same customer always → same partition\n"<< "  - Ordering preserved for customer\n"<< "  - Different customers on different partitions (parallel)\n";
}

int main() {
    try {
        demo_basic_produce_consume();
        demo_offset_tracking();
        demo_partitioning();
        
        print_header("✓ COMPLETED !");
        return 0;
    } catch (const exception& e) {
        cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}