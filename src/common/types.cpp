#include "hyperq/common/types.hpp"
#include <iostream>
#include <chrono>
using namespace std;

// Empty constructor
Message::Message() 
    : offset(0), timestamp(0), partition(0) {}

// Constructor with parameters
Message::Message(uint64_t offset, const string& key, const string& value, int partition) 
    : offset(offset), key(key), value(value), partition(partition) {
    timestamp = chrono::system_clock::now().time_since_epoch().count();
}

string Message::format_string() const {
    return "Message{offset=" + to_string(offset) + ", key=" + key + ", value=" + value 
           + ", partition=" + to_string(partition) + ", timestamp=" + to_string(timestamp) + "}";
}

// ProduceResponse implementations
ProduceResponse::ProduceResponse() 
    : success(false), partition(-1), offset(0) {}

string ProduceResponse::format_string() const {
    return "ProduceResponse{success=" + string(success ? "true" : "false") 
           + ", topic=" + topic + ", partition=" + to_string(partition) 
           + ", offset=" + to_string(offset) + ", error=" + error_message + "}";
}

// OffsetCommitResponse implementations
OffsetCommitResponse::OffsetCommitResponse() 
    : succcess(false) {}

string OffsetCommitResponse::format_string() const {
    return "OffsetCommitResponse{success=" + string(succcess ? "true" : "false") 
           + ", error=" + error_message + "}";
}
