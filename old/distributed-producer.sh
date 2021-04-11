#!/bin/bash

duration_seconds=$1
throughput=$2
kafka=$3
topic=$4


shift 5

ips=$@

size=${#ips[@]}
#size=$(echo "$size + 1" | bc)

throughput_per_node=$(echo "$throughput / $size" | bc)
remainder=$(echo "$throughput % $size" | bc )
master_throughput=$(echo "$throughput_per_node + $remainder" | bc)
num_records_per_node=$(echo "$throughput_per_node * $duration_seconds" | bc)
echo "Requested throughput: $throughput"
echo "Throughput per node: $throughput_per_node"
echo "Num records per node: $num_records_per_node"

prodindex=0
for ip in $ips ; do
		#ssh -o StrictHostKeyChecking=no ubuntu@$ip "timeout $duration_seconds /home/ubuntu/kafka/bin/kafka-producer-perf-test.sh --topic $topic --num-records $num_records_per_node --throughput $throughput_per_node --producer-props bootstrap.servers=$kafka key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer --payload-file /home/ubuntu/cracklib-small &> /dev/null" & 
		ssh -o StrictHostKeyChecking=no ubuntu@$ip "timeout $duration_seconds /home/ubuntu/kafka/bin/kafka-producer-perf-test.sh --dist-producer-index $prodindex --dist-producer-total $size --topic $topic --num-records $num_records_per_node --throughput $throughput_per_node --producer-props bootstrap.servers=$kafka key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer &> /dev/null" & 
		prodindex=$((prodindex+1))
done

#timeout $duration_seconds /home/ubuntu/kafka/bin/kafka-producer-perf-test.sh --topic $topic --num-records $num_records_per_node --throughput $throughput_per_node --producer-props bootstrap.servers=$kafka key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer --payload-file /home/ubuntu/cracklib-small &> /dev/null
