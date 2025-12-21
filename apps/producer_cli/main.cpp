#include "hyperq/broker/broker.hpp"
#include "hyperq/client/producer.hpp"
#include <iostream>
#include <string>

int main(int argc, char* argv[]){
    string log_dir = "/tmp/hyperq";
    if(argc > 1)    log_dir = argv[1];
    try{
        Broker broker(1, log_dir);
        broker.create_topic("cli-topic", 3,1);
        Producer Producer(broker, "CLIProducer");
        cout<<"hyperQ Producer CLI \n Enter Messages (quit to exit) \n Format: message [key] \n\n";
        string line;
        while(getline(cin, line)){
            if(line == "quit")  break;
            if(line.empty())    continue;

            istringstream iss(line);
            string message, key;
            iss >> message;
            if(!(iss >> key))   key = "";

            auto response = producer.send("cli-topic", message, key);
            if(response.success)    cout<<"sent to partition "<<response.partition<<" offset "<<response.offset<<"\n";
            else cout<<"Error: "<<response.error_message<<"\n";
        }
        cout<<"Producer stats: "<<producer.get_produced_count()<<" messages sent \n";
        return 0;
    }catch (const exception& e){
        cerr<<"Error: "<<string(e.what())<<"\n";
        return 1;
    }
}