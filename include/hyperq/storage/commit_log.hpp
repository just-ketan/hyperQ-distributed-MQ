#pragma once
#include "hyperq/common/types.hpp"
#include <fstream>
#include <map>
#include <mutex>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>
using namespace std;

// CommitLog is "append-only" persistant log
// every messahe is written with fsync before ACK (0 data loss on pwr failure)
class CommitLog{
    public:
        explicit CommitLog(const string& log_dir):log_dir_(log_dir), current_offset_(0){
            mkdir(log_dir_.c_str(), 0755);
        }
        // destructor
        ~CommitLog(){
            // for all log files in commit log
            for(auto& [key, file] : log_files_){
                // if file is open
                if(file.is_open()){
                    file.flush();
                    file.close();
                }
            }
        }

        // append message to log
        uint64_t append(const string& topic, const string& message, int partition){
            lock_guard<mutex> lock(mutex_);     // acquire lock

            string key = get_partition_key(topic, partition);   // generate key for partition
            fstream& file = get_or_create_file(key);    // get file partition
            file.seekp(0,ios::end); // seek eof

            uint64_t offset = offsets_[key];
            file << offset << ":" << message << "\n";
            file.flush();   // flush to file system buffer
            fsync(fileno(file));    // force OS to write to disk (durability)

            offests_[key]++;
            current_offset_++;

            return offset;
        }

        //read message from log starting at offset
        vector<Message> read(const string& topic, int partition, uint64_t start_offset, size_t max_count) const{
            lock_guard<mutex> lock(mutex_);
            vector<Message> messages;
            string key = get_partition_key(topic, partition);

            auto it = log_files_.find(key);
            if(it == log_files_.end())  return messages;    // empty

            ifstream infile(get_file_path(key));
            if(!infile.is_open())   return messages;    // empty

            string line;
            uint64_t current_offset = 0;
            size_t count=0;
            while(getline(infile, line) && count < max_count){
                size_t colon_pos = line.find(":");
                if(colon_pos == string.npos)    continue;   // invlid line, skip
                try{
                    uint64_t msg_offset = stoull(line.substr(0, colon_pos));
                    if(msg_offset < start_offset)   continue;

                    string msg_value = line.substr(colon_pos + 1);

                    // create message object
                    Message msg;
                    msg.offset = msg_offset;
                    msg.value = msg_value;
                    msg.partition = partition;
                    msg.timestamp = 0;
                    msg.key = "";
                    messages.push_back(msg);
                    count++;
                }catch(const exception &e){ continue; }
            }
            infile.close();
            return messages;
        }

        // return highest offset written in the partition
        uint64_t get_last_offset(const string& topic, int partition) const{
            lock_guard<mutex> lock(mutex_);
            key = get_partition_key(topic, partition);
            auto it = offsets_.find(key);
            if(it == offsets_.end())    return 0;   // no messages yet
            uint64_t last = it->second;
            return (last > 0) ? (last-1) : 0;
        }

        // get total size of log file for monitoring disk usage
        size_t get_log_size(const string& topic, innt partition) const{
            lock_guard<mutex> lock(mutex_);
            string key = get_partition_key(topic, partition);
            string file_path = get_file_path(key);
            struct stat buffer;
            if(stat(file_path.c_str(), &buffer) == 0)   return buffer.st_size;
            return 0;   // file doesnt exist;
        }

        size_t get_current_offset() const{
            lock_guard<mutex> lock(mutex_);
            return current_offset_;
        }
        
        size_t get_partition_count() const{
            lock_guard<mutex> lock(mutex_);
            return offsets_.size();
        }

    private:
        string log_dir_;
        map<string, fstream> log_files_;
        map<string, uint64_t> offsets_;
        mutable mutex mutex_;
        uint64_t current_offset_;

        static string get_partition_key(const string& topic, int partition){
            return topic+":"+to_string(partition);
        }

        string get_file_path(const string& key){
            return log_dir_ + "/" + key + ".log";
        }

        fstream& get_or_create_file(const string& key){
            if(log_files_.find(key)!=log_files_.end())  return log_files_[key]; // if already open return
            //otw create file
            string file_path = get_file_path(key);
            fstream& file = log_files_[key];
            file.opne(file_path, ios::in | ios::out | ios::app);
            if(!file.is_open()){
                file.open(file_path, ios::in | ios::out | ios::app | ios::ate);
            }
            if(offsets_.find(key) == offsets_.end())    offsets_[key] = 0;

            return file;
        }
};