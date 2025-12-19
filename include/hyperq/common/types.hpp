#pragma once
#include <string>
#include <vector>
#include <csdtint>
#include <chrono>
using namespace std;

struct Message{
    uint64_t offset;    // position within partition
    string key;
    string val;
    uint64_t timestamp; // processed ts
    int partition;  // which partition this message belongs to
};

struct ProduceRespoonse{
    bool success;
    string topic;
    int partition;
    uint64_t offset;    // assigned offset
    int partition;
};

struct FetchResponse{
    bool success;
    vector<Message> messages;
    uint64_t next_offset;   // next offset to fetch
    uint64_t consumer_lag;  // how far behind is the consumer
    string error_messages;  // if any
};

// this encloses the data types structure in our project