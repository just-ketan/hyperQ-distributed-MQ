#pragma once
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <cstdint>  // c stdin t
#include <unistd.h> // unicode ds
using namespace std;

// message ds
struct message{
    uint64_t offset;
    string data;
    uint64_t timestamp;
};

class CommitLog{
    public:
        explicit CommitLog(const string& log_dir):log_dir_(log_dir){
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

        // fsync() for durability
        uint64_t append(const string& topic, int partition, const string& message){
            string key = topic + "-" + to_string(partition);
            lock_guard<mutex> lock(mutex_);

            // lazy open file
            if(log_files_.find(key) == log_files_.end()){
                string path = log_dir_ + "/" + key + ".log";
                log_files_[key].open(path, ios::binary | ios::app);
                if(!log_files_[key].is_open()){
                    throw runtime_error("Cannot Open:" + path);
                }
            }
            auto& file = log_files_[key];
            // write format is 4 bytes
            uint32_t size = message.length();
            file.write(reinterpret_cast<FILE*>(const_cast<filebuf*>(file.rdbuf())));
            // force to disk
            uint64_t offset = offsets_[key]++;
            return offset;
        }

        vector<Message> read(const string& topic, int partition, uint64_t start_offset, size_t max_count) const{
            string key = topic + "-" + to_string(partition);
            string path = log_dir_ + "/" + key + ".log";
            vector<Message> messages;

            ifstream file(path, ios::binary);
            if(!file.is_open())  return messages;

            uint64_t current_offset = 0;
            while(messages.size() < max_count && file.good()){
                uint32_t size;
                file.read(reinterpret_cast<char*>(&size), sizeof(size));
                if(file.eof())  break;

                string data(size,'\0');
                file.read(&data[0], size);

                if(curent_offset >= start_offset){
                    messages.push_back({current_offset, data, 0});
                }
                current_offset++;
            }
            file.close();
            return messages;
        }

        uint64_t get_last_offset(const string& topic, int partition) const{
            string key = topic + "-" + to_string(partition);
            lock_guard<mutex> lock(mutex_);
            auto it = offset_.find(key);
            return it!= offsets_.end() ? it->second - 1 : -1;
        }
    
    private:
        string log_dir_;
        map<string, fstream> log_files_;
        map<string, uint64_t> offsets_;
        mutable mutex mutex_;
};