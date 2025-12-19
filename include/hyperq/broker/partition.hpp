#pragma once
#include "hyperq/storage/commit_log.hpp"
#include <memory>
#include <shared_mutex>
#include <vector>

class Partition{
    public:
        Partition(const string& topic, int partition_id, int broker_id, bool is_leader, shared_ptr<CommitLog> log) : topic_(topic), partition_id_(partition_id), broker_id_(broker_id), is_leader_(is_leader), log_(log), high_watermark_(-1), replicas_({broker_id}) {}

        // append to leader 
        uint64_t append(const string& message){
            if(!is_leader_){
                throw runtime_error("Not leader for append");
            }
            unique_lock<shared_mutex> lock(mutex_);
            return log_->append(topic_, partition_id_, message);
        }
        // read from any replica
        vector<Message> read(uint64_t offset, size_t count) const{
            shaed_lock<shared_mutex> lock(mutex_);
            return log_->read(topic_, partition_id_, offset, count);
        }

        bool is_leader() const { return is_leader_; }

        void promote_to_leader(){
            unique_lock<shared_mutex> lock(mutex_);
            is_leader_ = true;
        }
        void add_replica(int broker_id){
            unique_lock<shared_mutex> lock(mutex_);
            replicas.push_back(broker_id);
        }
    
    private:
        string topic_;
        int partition_id_, broker_id_;
        bool is_leader_;
        shared_ptr<CommitLog> log_;
        long high_watermark_;
        vector<int> replicas;
        mutable shared_mutex mutex_;
}