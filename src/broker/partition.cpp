#include "hyperq/broker/partition.hpp"
#include <iostream>
using namespace std;

Partition::Partition(const string& topic, int partition_id, int broker_id, bool is_leader, shared_ptr<CommitLog> commit_log):topic_(topic), partition_id_(partition_id),broker_id_(broker_id),is_leader_(is_leader),commit_log_(commit_log),high_watermark_(0){
    cout<<"[Partition "<<topic<<":"<<partition_id<<"] Created on broker "<<broker_id_<<" (Leader: "<<(is_leader_?"YES":"NO")<<")\n";
}

uint64_t Partition::append(const string& topic){
    unique_lock<shared_mutex> lock(mutex_);
    if(!is_leader_) throw runtime_error("Cannot write to follower partition");

    uint64_t offset = commit_log_->append(topic_, partition_id_, message);
    high_watermark_ = offset;
    return offset;
}
vector<Message> Partition::read(uint64_t start_offset, size_t max_count) const{
    shared_lock<shared_mutex> lock(mutex_);
    return commit_log_->read(topic_, partition_id_, start_offset, max_count);
}

bool Partition::is_leader() const{
    shared_lock<shared_lock> lock(mutex_);
    return is_leader_;
}

void Partition::promote_to_leader(){
    unique_lock<shared_mutex> lock(mutex_);
    is_leader_ = true;
    cout<<"[Partition "<<topic_<<":"<<partition_id_<<" promoted to leader\n";
}

void Partition::add_replica(int broker_id){
    unique_lock<shared_lock> lock(mutex_);
    replica_brokers_.push_back(broker_id);
}

const vector<int>& Partition::get_replicas() const {
    return replica_brokers_;
}

long Partition::get_high_watermark() const {
    shared_lock<shared_mutex> lock(mutex_);
    return high_watermark_;
}

void Partition::set_high_watermark(long watermark){
    unique_lock<shared_mutex> lock(mutex_);
    high_watermark_ = watermark;
}
