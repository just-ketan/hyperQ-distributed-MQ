#pragma once
#include "hyperq/broker/partition.hpp"
#include "hyperq/storage/commit_log.hpp"
#include "hyperq/coordinator/consumer_groups.hpp"
#include "hyperq/common/types.hpp"
#include <map>
#include <memory>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <algorithm>
#include <functional>
using namespace std;

/*
 * Broker: Main MQ server
 * Responsibilities:
 * 1. Manage topics and partitions
 * 2. Handle producer writes
 * 3. Handle consumer reads
 * 4. Track consumer groups
 * 5. Manage replication (simplified)
*/

class Broker {
public:
    // Create broker
    explicit Broker(int broker_id, const string& log_dir = "/tmp/hyperq")
        : broker_id_(broker_id),
          commit_log_(make_shared<CommitLog>(log_dir)),
          partition_counter_(0) {
        cout << "[Broker " << broker_id_ << "] Started\n";
    }

    ~Broker() {
        cout << "[Broker " << broker_id_ << "] Stopped\n";
    }

    //Create topic with partitions
    void create_topic(const string& topic,int num_partitions,int replication_factor) {
        lock_guard<mutex> lock(mutex_);

        // Check if topic already exists
        if (topics_.find(topic) != topics_.end()) {
            throw invalid_argument("Topic " + topic + " already exists");
        }

        vector<unique_ptr<Partition>> partitions;

        // Create partitions
        for (int p = 0; p < num_partitions; p++) {
            bool is_leader = (p == 0);  // First partition is leader

            auto partition = make_unique<Partition>(
                topic, p, broker_id_, is_leader, commit_log_
            );
            // add replications
            for (int r = 1; r <= replication_factor; r++) {
                partition->add_replica(r);
            }
            partitions.push_back(move(partition));
        }

        topics_[topic] = move(partitions);
        cout << "[Broker " << broker_id_ << "] Created topic: " << topic<< " with " << num_partitions << " partition(s)\n";
    }

    // Produce message to topic
    ProduceResponse produce(const string& topic,const string& message,const string& key = "") {
        lock_guard<mutex> lock(mutex_);

        // Check if topic exists
        auto topic_it = topics_.find(topic);
        if (topic_it == topics_.end()) {
            return ProduceResponse{
                false, topic, -1, 0,
                "Topic " + topic + " does not exist"
            };
        }

        auto& partitions = topic_it->second;
        int partition_count = partitions.size();

        // Select partition
        int partition_id;
        if (key.empty()) {
            // Round-robin if no key
            partition_id = partition_counter_ % partition_count;
            partition_counter_++;
        } else {
            hash<string> hasher;  // hash the key
            partition_id = hasher(key) % partition_count;
        }

        Partition* partition = partitions[partition_id].get();

        // Write to leader
        try {
            uint64_t offset = partition->append(message);

            cout << "[Broker " << broker_id_ << "] Produced to "<< topic << ":" << partition_id << " offset " << offset<< "\n";

            return ProduceResponse{
                true, topic, partition_id, offset, ""
            };
        } catch (const exception& e) {
            return ProduceResponse{
                false, topic, partition_id, 0,
                "Write failed: " + string(e.what())
            };
        }
    }

    // Consume messages from topic
    FetchResponse consume(const string& topic,int partition,const string& group_id,uint64_t offset = 0) {
        lock_guard<mutex> lock(mutex_);

        // Check if topic exists
        auto topic_it = topics_.find(topic);
        if (topic_it == topics_.end()) {
            return FetchResponse{
                false, {}, 0, 0,
                "Topic " + topic + " does not exist"
            };
        }

        auto& partitions = topic_it->second;

        // Check if partition exists
        if (partition >= partitions.size()) {
            return FetchResponse{
                false, {}, 0, 0,
                "Partition " + to_string(partition) + " does not exist"
            };
        }

        Partition* part = partitions[partition].get();

        // Get offset
        if (offset == 0) {
            // Get last committed offset from coordinator
            offset = group_coordinator_.get_offset(group_id, topic, partition);
        }

        // Read from partition
        try {
            auto messages = part->read(offset, 10);

            // Commit new offset if we read messages
            if (!messages.empty()) {
                uint64_t last_offset = messages.back().offset;
                group_coordinator_.commit_offset(
                    group_id, topic, partition, last_offset
                );
            }

            cout << "[Broker " << broker_id_ << "] Consumed from "<< topic << ":" << partition << " group " << group_id<< " messages: " << messages.size() << "\n";

            uint64_t next_offset = messages.empty() ? offset : messages.back().offset + 1;
            uint64_t lag = part->get_high_watermark() > offset ? part->get_high_watermark() - offset : 0;

            return FetchResponse{
                true, messages, next_offset, lag, ""
            };
        } catch (const exception& e) {
            return FetchResponse{
                false, {}, 0, 0,
                "Read failed: " + string(e.what())
            };
        }
    }

    //Print broker status (debugging)
    void print_status() const {
        lock_guard<mutex> lock(mutex_);

        cout << "\n========== BROKER " << broker_id_ << " STATUS ==========\n";

        for (const auto& [topic, partitions] : topics_) {
            cout << "Topic: " << topic << " (" << partitions.size()<< " partitions)\n";

            for (size_t p = 0; p < partitions.size(); p++) {
                const auto* partition = partitions[p].get();
                cout << "  Partition " << p << ":\n"<< "    Leader: " << (partition->is_leader() ? "YES" : "NO")<< "\n"<< "    High Watermark: " << partition->get_high_watermark()<< "\n";
            }
        }

        cout << "===================================\n\n";
    }

    int get_broker_id() const {
        return broker_id_;
    }

    size_t get_topic_count() const {
        lock_guard<mutex> lock(mutex_);
        return topics_.size();
    }

    //Get partition for topic
    Partition* get_partition(const string& topic, int partition_id) {
        lock_guard<mutex> lock(mutex_);

        auto topic_it = topics_.find(topic);
        if (topic_it == topics_.end()) {
            return nullptr;
        }

        if (partition_id >= topic_it->second.size()) {
            return nullptr;
        }

        return topic_it->second[partition_id].get();
    }

    //Get consumer group coordinator
    ConsumerGroupCoordinator& get_coordinator() {
        return group_coordinator_;
    }

private:
    int broker_id_;
    // {topic: [partitions]}
    map<string, vector<unique_ptr<Partition>>> topics_;
    shared_ptr<CommitLog> commit_log_;
    ConsumerGroupCoordinator group_coordinator_;
    mutable mutex mutex_;
    int partition_counter_;  // For round-robin partition selection
};