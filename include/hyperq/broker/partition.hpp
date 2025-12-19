#pragma once
#include "hyperq/storage/commit_log.hpp"
#include "hyperq/common/types.hpp"
#include <memory>
#include <shared_mutex>
#include <vector>
#include <stdexcept>
#include <iostream>

using namespace std;

class Partition {
public:
    Partition(const string& topic,
              int partition_id,
              int broker_id,
              bool is_leader,
              shared_ptr<CommitLog> commit_log)
        : topic_(topic),
          partition_id_(partition_id),
          broker_id_(broker_id),
          is_leader_(is_leader),
          commit_log_(commit_log),
          high_watermark_(-1) {
        if (!commit_log_) {
            throw invalid_argument("commit_log cannot be null");
        }
    }

    ~Partition() {} // CommitLog is shared_ptr, cleans itself

    // Append to leader only
    uint64_t append(const string& message) {
        unique_lock<shared_mutex> lock(mutex_);

        if (!is_leader_) {
            throw runtime_error(
                "Cannot append to partition " + to_string(partition_id_) +
                ": not leader (broker " + to_string(broker_id_) + ")"
            );
        }

        // Write to commit log with fsync (inside CommitLog::append)
        uint64_t offset = commit_log_->append(topic_, partition_id_, message);
        high_watermark_ = offset;
        return offset;
    }

    // Read from any replica
    vector<Message> read(uint64_t start_offset, size_t max_count) const {
        shared_lock<shared_mutex> lock(mutex_);
        auto messages =
            commit_log_->read(topic_, partition_id_, start_offset, max_count);
        return messages;
    }

    bool is_leader() const {
        shared_lock<shared_mutex> lock(mutex_);
        return is_leader_;
    }

    // Promote follower to leader
    void promote_to_leader() {
        unique_lock<shared_mutex> lock(mutex_);
        if (is_leader_) {
            throw runtime_error(
                "Partition " + to_string(partition_id_) +
                " is already leader on broker " + to_string(broker_id_));
        }
        is_leader_ = true;
        cout << "[Broker " << broker_id_ << "] "
             << topic_ << "-" << partition_id_
             << " promoted to LEADER\n";
    }

    // Add replica broker id
    void add_replica(int broker_id) {
        unique_lock<shared_mutex> lock(mutex_);
        for (int existing : replica_brokers_) {
            if (existing == broker_id) {
                return; // already added
            }
        }
        replica_brokers_.push_back(broker_id);
    }

    const vector<int>& get_replicas() const {
        return replica_brokers_;
    }

    long get_high_watermark() const {
        shared_lock<shared_mutex> lock(mutex_);
        return high_watermark_;
    }

    void set_high_watermark(long watermark) {
        unique_lock<shared_mutex> lock(mutex_);
        high_watermark_ = watermark;
    }

    string get_topic() const {
        return topic_;
    }

    int get_partition_id() const {
        return partition_id_;
    }

    int get_broker_id() const {
        return broker_id_;
    }

    // Get last offset
    uint64_t get_last_offset() const {
        shared_lock<shared_mutex> lock(mutex_);
        return commit_log_->get_last_offset(topic_, partition_id_);
    }

    // Get size of partition in bytes
    size_t get_size() const {
        shared_lock<shared_mutex> lock(mutex_);
        return commit_log_->get_log_size(topic_, partition_id_);
    }

    void print_status() const {
        shared_lock<shared_mutex> lock(mutex_);
        cout << "Partition " << topic_ << "-" << partition_id_ << ":\n"
             << "  Broker: " << broker_id_ << "\n"
             << "  Leader: " << (is_leader_ ? "YES" : "NO") << "\n"
             << "  High Watermark: " << high_watermark_ << "\n"
             << "  Last Offsets: " << get_last_offset() << "\n"
             << "  Size: " << get_size() << " bytes\n"
             << "  Replicas: ";
        for (int broker : replica_brokers_) {
            cout << broker << " ";
        }
        cout << "\n";
    }

private:
    string topic_;
    int partition_id_;
    int broker_id_;
    bool is_leader_;
    shared_ptr<CommitLog> commit_log_;
    long high_watermark_;
    vector<int> replica_brokers_;
    mutable shared_mutex mutex_;
};
