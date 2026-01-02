#!/bin/bash
mkdir -p "/tmp/hyperq-cluster"
echo "statrting hyperq local cluster..."
./build/hyperq-broker 1 /tmp/hyperq-cluster/broker1 & BROKER1_PID=$!
./build/hyperq-broker 2 /tmp/hyperq-cluster/broker2 & BROKER2_PID=$!
./build/hyperq-broker 3 /tmp/hyperq-cluster/broker3 & BROKER3_PID=$!

echo "Brokers started:"
echo " Broker 1 (PID: $BROKER1_PID)"
echo " Broker 2 (PID: $BROKER2_PID)"
echo " Broker 3 (PID: $BROKER3_PID)"
echo "Press Ctrl+C to stop"
# wait for all processes
wait 