#include "hyperq/broker/broker.hpp"
#include "hyperq/client/consumer.hpp"
#include <iostream>
#include <string>
using namespace std;

int main(int argc, char* argv[]){
    string log_dir = "/tmp/hyperq";
    string group_id = "cli-group";

    if(argc > 1)    log_dir = argv[1];
    if(argc > 2)    group_id = argv[2];

    try{
        Broker broker(1, log_dir);
        broker.create_topic("cli-topic", 3,1);
        Consumer consumer(broker, group_id, "CLIConsumer");
        cout << "HyperQ Consumer CLI\n";
        cout << "Group ID: " << group_id << "\n";
        cout << "Enter partition number (0-2) or 'quit' to exit\n\n";

        string line;
        while(getline(cin, line)){
            if(line == "quit")  break;
            if(line.empty())    continue;
            try{
                int partition = stoi(line);
                auto response = consumer.consume("cli-topic", partition);
                if(response.success){
                    cout<<"COnsumed "<<response.messages.size()<<" messages from partition "<<partition<<"\n";
                    for(const auto& msg : response.messages){
                        cout<<"["<<msg.offset<<"] "<<msg.value<<"\n";
                    }
                    cout<<" Lag: "<<response.consumer_lag<<" messages\n";
                }else{
                    cout<<" Error: "<<response.error_message<<"\n";
                }
            }catch (const exception& e){
                cout<< "Invalid partition or error: "<<e.what()<<"\n";
            }
        }
        cout<<"\n Consumer Stats: "<<consumer.get_consumer_count()<<" messages consumed from group "<<group_id<<"\n";
        return 0;
    }catch (const exception& e){
        cerr<<"Error: "<<e.what()<<"\n";
        return 1;
    }
}