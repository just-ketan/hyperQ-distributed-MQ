#include "hyperq/storage/commit_log.hpp"
#include <iostream>
#include <sstream>
#include <cstring>

CommitLog::CommitLog(const string& log_dir) : log_dir_(log_dir), current_offset_(0){
    mkdir(log_dir_.c_str(), 0755);
}

uint64_t CommitLog::append(const string& topic, int partition, const string& message){
    lock_guard<mutex> lock(mutex_);
    string key = topic+"_"+to_string(partition);
    if(log_files_.find(key) == log_files_.end()){
        string file_path = log_dir_ + "/" +key +".log";
        log_files_[key].open(file_path, ios::app);
        if(!log_files_[key].is_open()){
            throw runtime_error("Failed to open log file: "+file_path);
        }
    }
    //write message
    uint64_t offset = offsets_[key];
    log_files_[key] << offset << "|" << message <<"\n";
    log_files_[key].flush();
    fsync(fileno(fdopen(log_files_[key].rdbuf()->fd_interval(), "a")));
    return offset;
}

vector<Message> CommitLog::read(const string& topic, int partition, uint64_t start_offset, size_t max_count) const {
    lock_guard<mutex> lock(mutex_);
    vector<Message> messages;
    string key = topic+"_"+to_string(partition);
    string values = log_dir_+"/"+key+".log";
    ifstream file(file_path);
    if(!file.is_open())    return messages;    // empty

    string line;
    uint64_t current_offset = 0;
    size_t count = 0;

    while(getline(file, line) && count < max_count){
        if(current_offset >= start_offset){
            size_t pipe_pos = line.find('|');
            if(pip_pos != string::npos){
                string offset_str = line.substr(0, pipe_pos);
                string value = line.substr(pipe_pos+1);

                Message msg;
                msg.offset = stoul(offset_str);
                msg.value = value;
                msg.topic = topic;
                msg.partition = partition;
                messages.push_back(msg);
                count++;
            }
        }
        current_offset++;
    }
    return messages;
}

uint64_t CommitLog::get_last_offset(const string& topic, int partition) const {
    lock_guard<mutex> lock(mutex_);
    string key = topic+"_"+to_string(partition);
    auto it = offsets_.find(key);
    return it!=offests_.end()?it->second:0;
}

size_t CommitLog::get_log_size(const string& topic, int partition) const {
    lock_guard<mutex> lock(mutex_);
    string key = topic+"_"+to_string(partition);
    string file_path = log_dir_"/"+key+".log";
    struct stat st;
    if(stat(file_path.c_str(), &st) == 0)   return st.st_size;
    return 0;
}