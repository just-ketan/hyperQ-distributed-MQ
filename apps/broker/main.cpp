#include "hyperq/broker/broker.hpp"
#include <iostream>
#include <thread>
#include <chrono>
using namespace std;

int main(){
    cout<<"---------------HyperQ MQ Demo--------------\n\n";
    Broker broker(1, "/tmp/hyperq");
    broker.create_topic("orders", 2, 1);
    cout<<"--------------Basic Produce-Consume--------\n";
    {
        Producer producer(broker);
        for(int i=0; i<5; ++i){
            string msg = "order#" + to_string(i+1);
            producer.send("orders", msg);
        }

        Consumer consumer(broker, "order-service");
        auto response = consumer.consume("orders",0);
        cout<<"Consumed"<<response.messages.size()<<"messages\n";
    }

    cout<<"---------Offset Tracking for Fault Tolerance------------\n\n";
    {
        Producer producer(broker);
        for(int i=0; i<10; i++){
            producer.send("orders", "Message " + to_string(i));
        }

        Consumer c1(broker, "service-group");
        auto resp1 = c1.consume("orders",0);
        cout<<"Consumer 1 read" << resp1.messages.size() << "messages\n";

        Consumer c2(broker, "service-group");
        auto resp2 = c2.consume("orders", 0);
        cout<<"Consumer 2 read"<< resp2.messages.size()<<"messages from offest"<<resp2.next_offset<<"\n";
    }

    cout<<"------------Partitioning for paralellism-----------\n";
    {
        Producer producer(broker);
        for(int i=0; i<8; i++){
            string key = "customer_"+to_string(i%3);
            string msg = "payment"+to_string(i);
            producer.send("orders", msg, key);
        }

        cout<<"Messages distributed across 2 partitions\n";
        cout<<"Same customer always on same partition (ordering preserved)\n";
    }
    broker.print_status();
    cout<<"DONE";
    return 0;
}