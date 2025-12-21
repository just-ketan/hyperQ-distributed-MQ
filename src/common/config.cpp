#include "hyperq/common/config.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
using namespace std;

namespace hyperq{
namespace config{
    const string DEFAULT_LOG_DIR = "/tmp/hyperq";
    const int DEFAULT_NUM_PARTITIONS = 3;
    const int DEFAULT_REPLICATION_FACTOR = 3;
    const int DEFAULT_BROKER_PORT = 9092;
    const int DEFAULT_CONSUMER_BATH_SIZE = 10;
    const uint64_t DEFAULT_SEGMENT_SIZE = 1024*1024;
    const uint64_t DEFAULT_FLUSH_INTERVAL = 5000;

    class ConfigImpl{
        public:
        string log_dir;
        int num_partitions;
        int replication_factor;
        int broker_port;
        int consumer_batch_size;
        uint64_t segment_size;
        uint64_t flush_interval;

        ConfigImpl() : log_dir(DEFAULT_LOG_DIR), num_partitions(DEFAULT_NUM_PARTITIONS), replication_factor(DEFAULT_REPLICATION_FACTOR), broker_port(DEFAULT_BROKER_PORT), consumer_batch_size(DEFAULT_CONSUMER_BATH_SIZE), segment_size(DEFAULT_SEGMENT_SIZE), flush_interval(DEFAULT_FLUSH_INTERVAL);
    };

    static ConfigImpl g_config;
    void load_config(const string& config_file){
        ifstream file(config_file);
        if(!file.is_open()){
            cerr<<"Warning: Config file "<<config_file<<" not found using defaults.\n";
            return;
        }
        string line;
        while(getline(file, line)){
            if(line.empty() || line[0] == '#')  continue;
            
            istringstream iss(line);
            string key, value;
            if(!(iss>>key>>value))  continue;

            if(key == "log_dir"){
                g_config.log_dir = value;
            }else if(key == "num_partitions"){
                g_config.num_partitions = stoi(value);
            }else if(key == "replication_factor"){
                g_config.replication_factor = stoi(value);
            }else if(key == "broker_port"){
                g_config.broker_port = stoi(value);
            }else if(key == "consumer_batch_size"){
                g_config.consumer_batch_size = stoi(value);
            }else if(key == "segment_size"){
                g_config.segment_size = stoul(value);
            }else if(key == "flush_interval"){
                g_config.flush_interval = stoul(value);
            }
        }
    }

    string get_log_dir()    return g_config.log_dir;
    int get_num_partitions()    return g_config.num_partitions;
    int get_replication_factor()    return g_config.replication_factor;
    int get_broker_port()   return g_config.broker_port;
    int get_consumer_batch_size()   return g_config.consumer_batch_size;
    uint64_t get_segment_size() return g_config.segment_size;
    uint64_t get_flush_interval()   return g_config.flush_interval;
}   // config
}   // hyperq
