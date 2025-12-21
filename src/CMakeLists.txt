set(HYPERQ_SOURCES
    common/types.cpp
    common/config.cpp
    storage/commit_log.cpp
    broker/partition.cpp
    broker/broker.cpp
    coordinator/consumer_groups.cpp
)

add_library(hyperq STATIC ${HYPERQ_SOURCES})
target_link_libraries(hyperq PUBLIC Threads::Threads)
target_include_directories(hyperq PUBLIC ../include)
