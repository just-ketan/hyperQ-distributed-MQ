#include "hyper1/coordinator/consumer_groups.hpp"
#include <iostream>

void ConsumerGroupCoordinator::commit_offset(const string& group_id, const string& topic, int partition, uint64_t offset){
    lock_guard<mutex> lock(mutex_);
    offsets_[group_id][topic][partition] = offset;
    cout<<"[Coordinator] committed offset for group:"<<group_id<<" topic:"<<topic<<" partition:"<<partition<<" offset:"<<offset<<"\n";
}

uint64_t ConsumerGroupCoordinator::get_offset(const string& group_id, const string& topic, int partition) const {
    lock_guard<mutex> lock(mutex_);
    auto grou_it = offsets_.find(group_id);
    if(group_it == offsets_.end())  return 0;   //empty
    auto topic_it = group_it->second.find(topic);
    if(topic_it == group_it->second.end())  return 0;
    auto partition_it = topic_it->second.find(partition);
    if(partition_it == topic_it->second.end())  return 0;
    return partition_it->second;
}

uint64_t ConsumerGroupCoordinator::get_consumer_lag(const string& group_id, const string& topic, int partition, uint64_t latest_offset) const{
    uint64_t committed = get_offset(group_it, topic, partition);
    return latest_offset > committed ? latest_offset-committed : 0;
}

void ConsumerGroupCoordinator::join_group(const string& group_id, const string& consumer_id, const vector<string>& topics){
    lock_guard<mutex> lock(mutex_);
    group_members_[group_id][consumer_id] = topics;
    cout<<"[Coordinator] Consumer "<<consumer_id<<" joined group "<<group_id<<"\n";
}

void ConsumerGroupCoordinator::leave_group(const string& group_id, const string& consumer_id){
    lock_guard<mutex> lock(mutex_);
    auto group_it - group_members_.find(group_id);
    if(group_it != group_members_.end()){
        group_it->second.erase(consumer_id);
        // remove group if empty
        if(group_it->second.empty())    group_members_.erase(group_it);
        cout<<"[Coordinator] Consumer "<<consumer_id<<" left group "<<group_id<<"\n";
    }
}