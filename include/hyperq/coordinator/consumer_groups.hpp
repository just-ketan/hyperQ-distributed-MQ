#pragma once
#include <map>
#include <string>
#include <vector>
#include <csdtint>
#include <mutex>
#include <stdexcept>
#include <iostream>
#include <algorithm>

class ConsumerGroupCoordinator{
    public:
        ConsumerGroupCoordinator() = default;
        ~ConsumerGroupCoordinator() = default;

        // commit offset for comsumer group
        void commit_offset(const string& group_id, const string& topic, int partition, uint64_t offset){
            lock_guard<mutex> lock(mutex_);
            offsets_[group_id][topic][partition] = offset;
        }

        uint64_t get_offset(const string& group_id, const string& topic, int partition) const{
            lock_guard<mutex> lock(mutex_);
            
            auto group_it = offsets_.find(group_id);
            if(group_it == offsets_.end())  return 0;
            auto topic_it = group_it->second.find(topic);
            if(topic_it == group_it->second.end())  return 0;
            auto partition_it = topic_it->second.find(partition);
            if(partition_it == topic_it->second.end())  return 0;

            return partition_it->second;
        }

        // calculate consumer lag
        uint64_t get_consumer_lag(const string& group_id, const string& topic, int partition, uint64_t latest_offset) const{
            lock_guard<mutex> lock(mutex_);

            uint64_t committed_offset=0;
            auto group_it = offsets_.find(group_id);
            if(group_it != offsets_.end()){
                auto topic_it = group_it->second.find(topic);
                if(topic_it != group_it->second.end()){
                    auto partition_it = topic_it->second.find(partition);
                    if(partition_it != topic_it->second.end()){
                        committed_offset = partition_it->second;
                    }
                }
            }

            if(latest_offset >= committed_offset){
                return latest_offset - committed_offset;
            }
            return 0;   // no lag
        }

        // join consumer groups
        /*
        adds consumer to group and subscribers to topics
        consumer id must be unique within group
        throws if consumer already exists in group
        */

        void join_group(const string& group_id, const string& consumer_id, const vector<string>& topics){
            lock_guard<mutex> lock(mutex_);

            auto group_it = group_members_.find(group_id);
            if(group_it != group_members_.end()){
                auto consumer_it = group_it->second.find(consumer_id);
                if(consumer_it != group_it->second.end()){
                    throw invalid_argument("Consumer" + consumer_id + "already exists in the group" + group_id);
                }
            }

            group_members_[group_id][consumer_id] = topics;
            cout << "Consumer "<<consumer_id<<" joined group "<<group_id<<" subscribing to "<<topics.size()<<" topics\n";
        }

        // leave consumer group
        // removes "consumer from group and triggers erbalancing" and "remove assosciated offest commits for this consumer"
        void leave_group(const string& group_id, const string& consummer_id){
            lock_guard<mutex> lock(mutex_);

            auto group_it = group_members_.find(group_id);
            if(group_it == group_members_.end()){
                throw invalid_argument("Group " + group_id +" doest not exist");
            }
            auto consumer_it = group_it->second.find(consumer_id);
            if(consumer_it == group_it->second.end()){
                throw invalid_argument("Consumer "+consumer_id+" does not exist in "+group_id);
            }

            group_it->second.erasse(consumer_it);

            if(group_it->second.empty())    group_members_.erase(group_it); // no consumers left
            cout<<"Consumer "<<consumer_id<<" left group "<<group_id<<"\n";
        }

        //get consumer group size
        size_t get_group_size(const string& group_id) const{
            lock_guard<mutex> lock(mutex_);
            auto group_it = group_members_.find(group_id);
            if(group_it == group_members_.end())    return 0;

            return group_it->second.size();
        }

        // get all consumer ids in group
        vector<string> get_group_members(const string& group_id) const {
            lock_guard<mutex> lock(mutex_);
            vector<string> members;

            auto group_it = group_members_.find(group_id);
            if(group_it == group_members_.end())    return members;

            for(const auto& [consumer_id, topics] : group_it->second){
                members.push_back(consumer_id);
            }

            return members;
        }

        bool is_member(const string& group_id, const string& consumer_id) const{
            lock_guard<mutex> lock(mutex_);
            auto group_it = group_members_.find(group_id);
            if(group_it == group_members_.end())    return false;

            return group_it->second.find(consumer_id) != group_it->second.end();
        }

        // reset offset
        void reset_offset(const string& group_id, const string& topic, int partition, uint64_t offset){
            lock_guard<mutex> lock(mutex_);
            offsets_[group_id][topic][partition] = offset;
        }

        void clear_group_offsets(const string& group_id){
            lock_guard<mutex> lock(mutex_);
            auto group_it = offsets_.find(group_id);
            if(group_it != offsets_.end())  group_it->second.clear();
        }

        void print_group_status(const string& group_id) const{
            lock_guard<mutex> lock(mutex_);
            cout<<"Consumer group: "<<group_id<<"\n";
            auto group_it = group_members_.find(group_id);
            if(group_it != group_members_.end()){
                cout<<"Members: "<<group_it->second.size()<<"\n";
                for(const auto& [consumer_id, topics] : group_it->second){
                    cout<<"   - "<<consumer_id<<" (subscribed to "<<topics.size()<<"topic(s)) \n";
                }
            }

            auto offests_it = offsets_.find(group_id);
            if(offsets_it != offsets_.end()){
                cout<<" Offsets: \n";
                for(const auto& [topic, partitions] : offsets_it->second){
                    for(const auto& [partition, offset] : partitions){
                        cout<"   - "<<topic<<":"<<partition<<"->"<<offset<<"\n";
                    }
                }
            }
        }
    private:
        map<sring, map<string, map<int, uint64_t>>> offsets_;
            // {group_id: {topic: {partition: offset}}}
        map<string, map<string, vector<string>>> group_members_;
            // {group_id: {consumer_id: {topics}}}
        mutable mutex mutex_;
};