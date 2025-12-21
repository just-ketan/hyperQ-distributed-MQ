# Unit Tests (4)
add_executable(test_commit_log unit/test_commit_log.cpp)
target_link_libraries(test_commit_log PRIVATE hyperq Threads::Threads)
add_test(NAME CommitLogTest COMMAND test_commit_log)

add_executable(test_partition unit/test_partition.cpp)
target_link_libraries(test_partition PRIVATE hyperq Threads::Threads)
add_test(NAME PartitionTest COMMAND test_partition)

# ... more tests ...

# Integration Tests (3)
add_executable(test_produce_consume integration/test_produce_consume.cpp)
target_link_libraries(test_produce_consume PRIVATE hyperq Threads::Threads)
add_test(NAME ProduceConsumeTest COMMAND test_produce_consume)

# ... more tests ...
