#pragma once
#include "hyperq/broker/broker.hpp"
#include "hyperq/common/types.hpp"
#include <string>
#include <vector>
#include <iostream>
using namespace std;

// Customer : client that reads from broker
class Consumer{
    public:
        explicit Consumer(Broker& broker, const string& group_id, const string& name="Consumer") : broker_(broker), group_id_(group_id), name_(name), consumer_count_(0){
            cout<<"["<<name_<<"] Started in group: "<<group_id_<<"\n";
        }

        ~Consumer(){
            cout<<"["<<name_<<"] Stopeed. Consumed: "<<consumer_count_<<" messages\n";
        }

        // consume messages from partition (last commit offset)
        // get last offset -> read from that offset onwards -> process messages -> commit new offset to coordinator

        FetchResponse consume(const string& topic, int partition, size_t max_messages=10){
            (void)max_messages; // broker uses fixed batch so unused rn
            FetchResponse response = broker_.consume(topic, partition, group_id_, 0);
            if(response.success){
                consumer_count_ += response.messages.size();
                cout<<"["<<name_<<"] Consumed from "<< topic<<":"<<partition<<" count "<< response.messages.size()<<"\n";

                for(const auto& msg : response.messages){
                    cout<<" Offset "<<msg.offset<<":"<<msg.value<<"\n";
                }
            }else{
                cout<<"["<<name_<<"] ERROR: "<<response.error_message<<"\n";
            }
            return response;
        }

        // consume from multiple partition
        int consume_partitions(const string& topic, const vector<int>& partitions){
            int consumer_count_ = 0;
            for(int partition : partitions){
                auto response = consume(topic, partition);
                if(response.success){
                    consumer_count_+=response.messages.size();
                }
            }
            return consumer_count_;
        }

        int get_consumer_count_() const {
            return consumer_count_;
        }
        string get_name() const {
            return name_;
        }
        string get_group_id() const {
            return group_id_;
        }

        // get committed offset for topic : partition
        uint64_t get_committed_offset(const string& topic, int partition) const {
            return broker_.get_coordinator().get_offset(group_id_, topic, partition);
        }
        uint64_t get_lag(const string& topic, int partition, uint64_t latest_offset){
            return broker_.get_coordinator().get_consumer_lag(group_id_, topic, partition, latest_offset);
        }
    private:
        Broker& broker_;
        string group_id_;
        string name_;
        size_t consumer_count_;
};