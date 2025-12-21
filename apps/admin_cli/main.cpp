#include "hyperq/broker/broker.hpp"
#include <iostream>
#include <string>
#include <sstream>
using namespace std;

void print_menu(){
    cout << "\nHyperQ Admin CLI\n";
    cout << "  1. Create Topic\n";
    cout << "  2. Show Status\n";
    cout << "  3. List Topics\n";
    cout << "  4. Produce Message\n";
    cout << "  5. Consume Message\n";
    cout << "  6. Exit\n";
    cout << "Choice: ";
}

int main(){
    Broker  broker(1, "/tmp/hyperq");
    int choice;
    while(true){
        print_menu();
        string input;
        if(!getline(cin, input))    break;

        try{
            choice = stoi(input);
        }catch (...){
            cout<<"Invalid choice \n";
            continue;
        }

        switch(choice){
            case 1: {
                cout<<"Topic name: ";
                string topic;
                getline(cin, topic);
                cout<< "Partitions: ";
                int partitions;
                cin>>partitions;
                cin.ignore();

                broker.create_topic(topic, partitions, 1);
                cout<<"Topics created \n";
                break;
            }
            case 2:
                broker.print_status();
                break;
            case 3:
                cout<<"Topics: "<<broker.get_topic_count()<<"\n";
                break;
            case 4:
                cout << "Feature not implemented in CLI\n";
                break;
            case 5:
                cout << "Feature not implemented in CLI\n";
                break;
            case 6:
                return 0;
            default:
                cout<<"Invalid choice\n";
        }
    }
    return 0;
}