#include "hyperq/common/types.hpp"
#include <iostream>
#include <chrono>
using namespace std;

Message::Message():offset(0), timestamp(0), partition(0) {}
Message::Message(uint64_t offset, const string& key, const string& value, int partition) : offset(offset), key(key), value(value), partition(partition){
    timestamp = chrono::system_clock::now().time_sice_epoch().count();
}
string Message::to_string() const{
    return "Message{offset="+to_string(offset)+" ,key="+key+" ,value="+value+" ,partition="+partition+" ,timestamp="+to_string(timestamp)+"}";
}
//produce response
ProduceResponse::ProduceResponse():success(false), partition(-1), offset(0) {}
string ProduceResponse::to_string() const {
    return "ProduceResponse{success=" + string(success ? "true" : "false") +", topic=" + topic +", partition=" + to_string(partition) +", offset=" + to_string(offset) +", error=" + error_message + "}";
}
//fetch response 
FetchResponse::FetchResponse () : success(false), next_offset(0), consumer_lag(0) {}
size_t FetchResponse::message_count() const {
    return messages.size();
}
string FetchResponse::to_string() const {
    return "FetchResponse{success=" + std::string(success ? "true" : "false") +", messages=" + std::to_string(messages.size()) +", next_offset=" + std::to_string(next_offset) +", lag=" + std::to_string(consumer_lag) +", error=" + error_message + "}";
}
//offset commit response
OffsetCommitResponse::OffsetCommitResponse():success(false){}
string OffsetCommitResponse::to_string() const {
    return "OffsetCommitResponse{success=" + std::string(success ? "true" : "false") +", error=" + error_message + "}";
} 