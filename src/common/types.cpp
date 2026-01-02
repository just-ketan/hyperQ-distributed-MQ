#include "hyperq/common/types.hpp"
#include <iostream>
using namespace std;

Message::Message():offset(0), timestamp(0), partition(0) {}

Message::Message(uint64_t offset, const string& key, const string& value, int partition) : offset(offset), key(key), value(value), partition(partition){
    timestamp = chrono::system_clock::now().time_sice_epoch().count();
}
string Message::to_string() const{
    return "Message{offset="+to_string(offset)+" ,key="+key+" ,value="+value+" ,partition="+partition+" ,timestamp="+to_string(timestamp)+"}";
}

ProduceResponse::ProduceResponse():success(false), partition(-1), offset(0) {}
string ProduceResponse::to_string() const {
    return "ProduceResponse{success=" + string(success ? "true" : "false") +", topic=" + topic +", partition=" + to_string(partition) +", offset=" + to_string(offset) +", error=" + error_message + "}";
}

OffsetCommitResponse::OffsetCommitResponse():success(false){}
string OffsetCommitResponse::to_string() const{
    return "OffsetCommitResponse{success=" + std::string(success ? "true" : "false") +", error=" + error_message + "}";
}
