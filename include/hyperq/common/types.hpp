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

struct ProduceRespoonse{
    bool success;
    string topic;
    int partition;
    uint64_t offset;    // assigned offset
    string error_message;
    ProduceRespoonse();
    string to_string() const;
};

struct FetchResponse{
    bool success;
    vector<Message> messages;
    uint64_t next_offset;   // next offset to fetch
    uint64_t consumer_lag;  // how far behind is the consumer
    string error_message;  // if any
    FetchResponse();
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