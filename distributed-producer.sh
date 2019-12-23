#!/bin/bash

duration_seconds=$1
resolution=$2
throughput=$3
kafka=$4
topic=$5


shift 5

ips=$@
size=0

for ip in $ips ; do
	size=$(echo "$size + 1" | bc)
done
size=$(echo "$size + 1" | bc)

throughput_per_node=$(echo "$throughput / $size" | bc)
echo "Requested throughput: $throughput"
echo "Throughput per node: $throughput_per_node"

for ip in $ips ; do
		ssh -o StrictHostKeyChecking=no ubuntu@$ip "python3 ~/throughput-producer/Main.py $duration_seconds $resolution $throughput_per_node $kafka $topic" & 
done

python3 ~/throughput-producer/Main.py $duration_seconds $resolution $throughput_per_node $kafka $topic
