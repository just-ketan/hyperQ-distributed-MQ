#include "hyperq/broker/broker.hpp"
#include <iostream>
#include <thread>
#include <chrono>
using namespace std;

int main(int argc, char* argv[]) {
    int broker_id = 1;
    string log_dir = "/tmp/hyperq";
    
    // Parse command-line arguments
    if (argc > 1) {
        broker_id = std::stoi(argv[1]);
    }
    if (argc > 2) {
        log_dir = argv[2];
    }
    
    cout << "Starting HyperQ Broker\n";
    cout << "  Broker ID: " << broker_id << "\n";
    cout << "  Log Directory: " << log_dir << "\n\n";
    
    try {
        Broker broker(broker_id, log_dir);
        
        // Create default topics
        broker.create_topic("orders", 3, 1);
        broker.create_topic("payments", 4, 1);
        broker.create_topic("events", 2, 1);
        
        cout << "\n✓ Broker ready for connections\n";
        cout << "Press Ctrl+C to stop\n\n";
        
        // Keep broker running
        while (true) {
            broker.print_status();
            this_thread::sleep_for(std::chrono::seconds(30));
        }
    } catch (const std::exception& e) {
        cerr << "Broker error: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}