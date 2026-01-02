#ifndef HYPERQ_STORAGE_COMMIT_LOG_HPP
#define HYPERQ_STORAGE_COMMIT_LOG_HPP

#include "hyperq/common/types.hpp"
#include <fstream>
#include <map>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

class CommitLog {
private:
    string log_dir_;
    uint64_t current_offset_;
    map<string, fstream> file_cache_;
    map<string, uint64_t> offsets_;

public:
    explicit CommitLog(const string& log_dir) 
        : log_dir_(log_dir), current_offset_(0) {
        // Ensure log directory exists
        mkdir(log_dir_.c_str(), 0755);
    }

    // Append message and return offset
    uint64_t append(const string& topic, const string& message, int partition) {
        string key = get_partition_key(topic, partition);
        fstream& file = get_or_create_file(key);
        
        if (file.is_open()) {
            file << current_offset_ << ":" << message << "\n";
            file.flush();
            // Note: fsync requires FILE*, use flush() instead for std::fstream
            offsets_[key]++;
            return current_offset_++;
        }
        return -1;
    }

    // Read messages from offset
    vector<Message> read(const string& topic, int partition, uint64_t start_offset, size_t max_count) const {
        vector<Message> messages;
        string key = get_partition_key(topic, partition);
        ifstream infile(get_file_path(key));
        
        if (infile.is_open()) {
            string line;
            uint64_t current_offset = 0;
            
            while (getline(infile, line)) {
                if (current_offset < start_offset) {
                    current_offset++;
                    continue;
                }
                
                if (messages.size() >= max_count) break;
                
                size_t colon_pos = line.find(':');
                if (colon_pos == string::npos) continue;
                
                string offset_str = line.substr(0, colon_pos);
                string message_data = line.substr(colon_pos + 1);
                
                Message msg;
                msg.offset = stoul(offset_str);
                msg.value = message_data;
                msg.partition = partition;
                messages.push_back(msg);
                current_offset++;
            }
        }
        
        return messages;
    }

    // Get last offset for a topic-partition
    uint64_t get_last_offset(const string& topic, int partition) const {
        string key = get_partition_key(topic, partition);
        if (offsets_.find(key) != offsets_.end()) {
            return offsets_.at(key);
        }
        return 0;
    }

    // Get log size for a topic-partition
    size_t get_log_size(const string& topic, int partition) const {
        string key = get_partition_key(topic, partition);
        string file_path = get_file_path(key);
        
        ifstream file(file_path);
        if (!file.is_open()) return 0;
        
        file.seekg(0, ios::end);
        return file.tellg();
    }

    virtual ~CommitLog() {
        for (auto& p : file_cache_) {
            if (p.second.is_open()) {
                p.second.close();
            }
        }
    }

private:
    // Helper: Create topic-partition key
    string get_partition_key(const string& topic, int partition) const {
        return topic + "-" + to_string(partition);
    }

    // Helper: Get file path for partition
    string get_file_path(const string& key) const {
        return log_dir_ + "/" + key + ".log";
    }

    // Helper: Get or create file for partition
    fstream& get_or_create_file(const string& key) {
        if (file_cache_.find(key) == file_cache_.end()) {
            string file_path = get_file_path(key);
            file_cache_[key].open(file_path, ios::in | ios::out | ios::app);
            if (!file_cache_[key].is_open()) {
                // Create file if doesn't exist
                ofstream touch_file(file_path);
                touch_file.close();
                file_cache_[key].open(file_path, ios::in | ios::out | ios::app);
            }
        }
        return file_cache_[key];
    }
};

#endif // HYPERQ_STORAGE_COMMIT_LOG_HPP
