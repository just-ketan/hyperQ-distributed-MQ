#include "hyperq/broker/broker.hpp"
#include <cassert>
#include <iostream>

void test_create_topic(){
    cout<<" Test to create topic \n";
    Broker broker(1,"/tmp/hyperq-test");
    broker.create_topic("test-topic", 3, 1);
    assert(broker.get_topic_count() == 1);
    cout<<" passed \n";
}

void test_produce_message(){
    cout<<" Test to produce message\n";
    Broker broker(1,"/tmp/hyperq-test");
    broker.create_topic("prod-test",2,1);
    auto response = broker.produce("prod-test","test-message");
    assert(response.success);
    assert(response.offset==0);
    cout<<" passed \n";
}

void test_produce_with_key(){
    cout<< "Test produce with key\n";
    Broker broker(1, "/tmp/hyperq-test");
    broker.create_topic("key-test",4,1);
    auto r1 = broker.produce("key-test","msg1","key1");
    auto r2 = broker.produce("key-test", "msg2", "key1");
    assert(r1.partition == r2.partition);
    cout<<" passed \n";
}

int main(){
    try{
        test_create_topic();
        test_produce_message();
        test_produce_with_key();
        cout<<"\n All test cases passed\n";
        return 0;
    }catch (const exception& e){
        cerr<<" Test failed: " << e.what()<<"\n";
        return 1;
    }
}