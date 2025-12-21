add_executable(hyperq-demo demo/main.cpp)
target_link_libraries(hyperq-demo PRIVATE hyperq Threads::Threads)

add_executable(hyperq-broker broker/main.cpp)
target_link_libraries(hyperq-broker PRIVATE hyperq Threads::Threads)

add_executable(hyperq-producer producer_cli/main.cpp)
target_link_libraries(hyperq-producer PRIVATE hyperq Threads::Threads)

add_executable(hyperq-consumer consumer_cli/main.cpp)
target_link_libraries(hyperq-consumer PRIVATE hyperq Threads::Threads)
