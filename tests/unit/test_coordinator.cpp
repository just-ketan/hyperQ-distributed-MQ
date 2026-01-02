#include <hyper1/coordinator/consumer_group.hpp"
#include <cassert>
#include <iostream>
using namespace std;

void test_commit_and_get_offset(){
    cout<<"Test commit and get offset\n";
    ConsummerGroupCoordinator coord;
    coord.commit_offset("group1","topic1",0,100);
    uint64_t offset = coord.get_offset("group1","topic1",0);
    assert(offset == 100);
    cout<<" PASSED \n";
}

void test_consumer_lag(){
    cout<<"Test consumer lag\n";
    ConsummerGroupCoordinator coord;
    coord.commit_offset("group1","topic1", 0,50);
    uint64_t lag = coord.get_consumer_lag("group1","topic1",0,100);
    assert(lag == 50);
    cout<<"passed \n";
}

void test_join_leave_group(){
    cout<<" Test join and leave group\n";
    ConsummerGroupCoordinator coord;
    coord.join_group("group1","consumer1",{"topic1","topic2"});
    coord.join_group("group1","consumer2",{"topic1"});
    //joined successfully
    coord.leave_group("group1","consumer1");    // consumer left group
    cout<<"passed\n";
}

int main(){
    try{
        test_commit_and_get_offset();
        test_consumer_lag();
        test_join_leave_group();
        cout<<"All  test cases passed\n";
        return 0;
    }catch (const exception& e){
        cerr<<"Tests Failed: "<<e.what()<<"\n";
        return 1;
    }
}