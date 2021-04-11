#!/bin/bash

duration_seconds=$1
throughput=$2
kafka=$3
topic=$4


shift 4

ips=( "$@" ) 

size=${#ips[@]}
num_prod_per_ip=2
num_prod_tot=$(( $num_prod_per_ip * $size ))
#size=$(echo "$size + 1" | bc)

throughput_per_prod=$(echo "$throughput / $num_prod_tot" | bc)
num_records_per_prod=$(echo "$throughput_per_prod * $duration_seconds" | bc)
echo "Num Producers: $num_prod_tot"
echo "Requested throughput: $throughput"
echo "Throughput per prod: $throughput_per_prod"
echo "Num records per prod: $num_records_per_prod"

prodindex=0
for ip in ${ips[@]} ; do
	for i in $(seq $num_prod_per_ip) ; do
		ssh -o StrictHostKeyChecking=no ubuntu@$ip "timeout $duration_seconds /home/ubuntu/kafka/bin/kafka-producer-perf-test.sh --dist-producer-index $prodindex --dist-producer-total $num_prod_tot --topic $topic --num-records $num_records_per_prod --throughput $throughput_per_prod --producer-props bootstrap.servers=$kafka key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer | grep -v 'records sent'" & 
		prodindex=$((prodindex+1))
	done
done

