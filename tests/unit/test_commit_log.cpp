#include "hyperq/storage/commit_log.hpp"
#include <cassert>
#include <iostream>
using namespace std;

void test_append_and_read() {
    cout << "TEST: Append and Read\n";
    
    CommitLog log("/tmp/hyperq-test");
    
    // Append messages
    uint64_t offset1 = log.append("test-topic", 0, "message1");
    uint64_t offset2 = log.append("test-topic", 0, "message2");
    uint64_t offset3 = log.append("test-topic", 0, "message3");
    
    assert(offset1 == 0);
    assert(offset2 == 1);
    assert(offset3 == 2);
    
    // Read messages
    auto messages = log.read("test-topic", 0, 0, 10);
    assert(messages.size() == 3);
    assert(messages[0].value == "message1");
    assert(messages[1].value == "message2");
    assert(messages[2].value == "message3");
    
    cout << "✓ PASSED\n";
}

void test_read_from_offset() {
    cout << "TEST: Read from Offset\n";
    
    CommitLog log("/tmp/hyperq-test");
    
    log.append("offset-test", 0, "msg0");
    log.append("offset-test", 0, "msg1");
    log.append("offset-test", 0, "msg2");
    
    auto messages = log.read("offset-test", 0, 1, 10);
    assert(messages.size() == 2);
    assert(messages[0].value == "msg1");
    
    cout << "✓ PASSED\n";
}

void test_multiple_partitions() {
    cout << "TEST: Multiple Partitions\n";
    
    CommitLog log("/tmp/hyperq-test");
    
    log.append("multi", 0, "p0-msg1");
    log.append("multi", 1, "p1-msg1");
    log.append("multi", 0, "p0-msg2");
    log.append("multi", 1, "p1-msg2");
    
    auto p0_msgs = log.read("multi", 0, 0, 10);
    auto p1_msgs = log.read("multi", 1, 0, 10);
    
    assert(p0_msgs.size() == 2);
    assert(p1_msgs.size() == 2);
    assert(p0_msgs[0].value == "p0-msg1");
    assert(p1_msgs[0].value == "p1-msg1");
    
    cout << "✓ PASSED\n";
}

int main() {
    try {
        test_append_and_read();
        test_read_from_offset();
        test_multiple_partitions();
        
        cout << "\n✓ ALL TESTS PASSED\n";
        return 0;
    } catch (const exception& e) {
        cerr << "✗ TEST FAILED: " << e.what() << "\n";
        return 1;
    }
}