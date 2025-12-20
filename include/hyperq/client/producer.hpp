#pragma once
#include "hyperq/broker/broker.hpp"
#include "hyperq/common/types.hpp"
#include <string>
#include <vector>
#include <iostream>
using namespace std;

// Producer : the client that sens message to broker
class Producer{
    public:
        // create producer
        explicit Producer(Broker& broker, const string& name="Producer"):broker_(broker), name_(name),produced_count_(0){
            cout<<"["<<name_<<"] Started \n";
        }
        ~Producer(){
            cout<<"["<<name_<<"] Stopped, Produced: "<<produced_count_<<" messages\n";
        }

        // send message to topic, routes to broker which selects the partition
        ProduceResponse send(const string& topic, const string& message, const string& key=""){
            ProduceResponse response = broker_.produce(topic, message, key);
            if(response.success){
                produced_count_++;
                cout<<"[ "<<name_<<"] sent to "<<topic<<":"<<response.partition<<"offset "<<response.offset<<"\n";
            }else{
                cout<<"["<<name_<<"] Error: "<<response.error_message<<"\n";
            }
            return response;
        }

        // batch processing
        int send_batch(const string& topic, const vector<string>& messages, const string& key=""){
            int success_cnt = 0;
            for(const auto& message : messages){
                auto response = send(topic, message, key);
                if(response.success)    success_cnt++;
            }
            return success_cnt;
        }

        int get_produced_count() const{
            return produced_count_;
        }
        string get_name() const{
            return name_;
        }
    private:
        Broker& broker_;
        string name_;
        int produced_count_;
};