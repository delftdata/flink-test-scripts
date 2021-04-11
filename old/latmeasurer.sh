#!/bin/bash

jobid=$1
interval=$2
num_measurements=$3
dep=$4

srcdep=0
sinkdep=$(echo "$dep + 1" | bc)

response=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid")
vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))

vsrc=${vertex_ids[$srcdep]}
isrc=0

vop=${vertex_ids[$sinkdep]}
iop=0

for (( i=0; i<=num_measurements; i++ )); do
		latency_measured=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid/metrics?get=latency.source_id.$vsrc.source_subtask_index.$isrc.operator_id.$vop.operator_subtask_index.$iop.latency_mean" | jq '.[0].value' | tr -d '"')
		ts=$(( $(date '+%s%N') / 1000000))
		echo "$ts $latency_measured"
sleep $interval


done

