#include "hyperq/broker/broker.hpp"
#include "hyperq/client/producer.hpp"
#include "hyperq/client/consumer.hpp"
#include <cassert>
#include <iostream>
using namespace std;
void test_produce_and_consume(){
    cout<<"Integration tests for produce and consume\n";
    Broker broker(1,"/tmp/hyperq-test");
    broker.create_topic("integration",2,1);

    Producer producer(broker);
    producer.send("integration","message6");
    producer.send("integration","message14");
    producer.send("integration","message25");

    Consumer consumer(broker,"test-group");
    auto response = consumer.consume("integration",0);
    assert(response.success);
    assert(response.messages.size()>0);
    cout<<"passed\n";
}

void test_multiple_producers_consumers(){
    cout<<"testing multiple producers and consumers\n";
    Broker broker(1,"/tmp/hyperq-test");
    broker.create_topic("multi",3,1);
    Producer p1(broker, "p1");
    Producer p2(broker, "p2");
    for(int i=0; i<5; i++){
        p1.send("multi","multi-p1-msg"+to_string(i));
        p2.send("multi","multi-p2-msg"+to_string(i));
    }

    Consumer c1(broker, "multi-group", "c1");
    Consumer c2(broker, "multi-group", "c2");
    c1.consume("multi",0);
    c2.consume("multi",1);
    cout<<"passed\n";
}

int main(){
    try{
        test_produce_and_consume();
        test_multiple_producers_consumers();
        cout<<"passed all integration tests\n";
        return 0;
    }catch(const exception& e){
        cerr<<"integration tests failed: "<<e.what()<<"\n";
        return 1;
    }
}