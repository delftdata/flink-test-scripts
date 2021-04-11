#!/bin/bash

FLINK_RPC=0.0.0.0:31234

get_job_id() {
  local jobid=$(curl -sS -X GET "http://$FLINK_RPC/jobs" | jq '.jobs[0].id' | tr -d '"')
  echo $jobid
}

get_job_vertexes() {
  local jobid=$1

  response=$(curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid")
  vertex_ids=($(echo $response | jq '.vertices[] | .id' | tr -d '"'))
  echo "${vertex_ids[@]}"
}

measure_latency_from_metrics() {
  local path=$1
  local p=$2
	local jobid=$(get_job_id)

	sleep 30

	rpc="http:/$FLINK_RPC/jobs/$jobid/metrics"
  local vertex_ids=($(get_job_vertexes $jobid))

  echo -e "TIME\tLATENCY_MEAN\tLATENCY_MEDIAN\tLATENCY_P99" >>$path

  while true; do

    local ts=$(date +%s%3N)

    local o=$((RANDOM % p))
		rpc_ops="$rpc?get=latency.operator_id.${vertex_ids[-1]}.operator_subtask_index.$o"

		echo "Curling: $rpc_ops.latency_mean" >> $path
    local lat_mean=$(curl -sS "$rpc_ops.latency_mean" | jq ".[0].value" | tr -d '"')
    local lat_mean=$(echo "${lat_mean%.*}")
    local lat_median=$(curl -sS "$rpc_ops.latency_median" | jq ".[0].value" | tr -d '"')
    local lat_median=$(echo "${lat_median%.*}")
    local lat_p99=$(curl -sS "$rpc_ops.latency_p99" | jq ".[0].value" | tr -d '"')
    local lat_p99=$(echo "${lat_p99%.*}")
    echo -e "$ts\t$lat_mean\t$lat_median\t$lat_p99" >> $path
    sleep 0.1 #125
  done
}

wait_for_job() {
	jobid=$(get_job_id)
	echo "JOBID: $jobid" >&2
	while [ "$jobid" == "null" ] ; do
		
		echo "KEEP WAITING" >&2
		jobid=$(get_job_id)
		sleep 5
	done

}

wait_for_job
measure_latency_from_metrics $1 $2
