#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <chrono>
using namespace std;

struct Message{
    uint64_t offset;    // position within partition
    string key;
    string value;
    long timestamp; // processed ts
    int partition;  // which partition this message belongs to
    string topic;
    
    Message();
    Message(uint64_t offset, const string& key, const string& value, int partition);
    string to_string() const;
};

struct ProduceResponse{
    bool success;
    string topic;
    int partition;
    uint64_t offset;    // assigned offset
    string error_message;
    ProduceResponse(bool s = false,
                    const std::string& t = "",
                    int p = -1,
                    uint64_t o = 0,
                    const std::string& e = "")
        : success(s), topic(t), partition(p), offset(o), error_message(e) {}    string to_string() const;
};

struct FetchResponse{
    bool success;
    string topic;
    int partition;
    vector<Message> messages;
    uint64_t next_offset;
    uint64_t consumer_lag;
    string error_message;
    
    FetchResponse(bool s = false,
                  const string& t = "",
                  int p = -1,
                  const vector<Message>& m = {},
                  const string& e = "")
        : success(s), topic(t), partition(p), 
          messages(m), next_offset(0), 
          consumer_lag(0), error_message(e) {}
    
    size_t message_count() const;
    string to_string() const;
};

// this encloses the data types structure in our project
struct OffsetCommitResponse{
    bool succcess;
    string error_message;
    OffsetCommitResponse();
    string to_string() const;
};