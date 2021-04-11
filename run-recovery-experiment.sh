#!/bin/bash

#Any arguments passed are expected to be bencharker IPs, not used for local experiments
benchmarkers=$@


#DEFAULTS (does not matter if has "" or not )
D_CI=5000                   #Checkpoint interval
D_SS=1000000                #State size (100Mib=100000000)
D_P=1                       #Parallelism
D_D=1                       #Depth - Number of tasks, not counting sources and sinks
D_KD=2                      #Kill Depth (1 based, meaning 1 is sources)
D_ST=0                      #Sleep Time (Iterations in a non-yielding loop)
D_W_SIZE=1000               #Window Size (Only if operator is window)
D_W_SLIDE=100               #Window Slide (Only if operator is window)
D_AS=0.0001                 #Access State - Percentage of Records which access and mutate state
D_O="map"                   #Operator
D_T="false"                 #Transactional
D_TC="processing"           #Time Notion
D_LTI="0"                   #Latency Tracking interval (0 is disabled)
D_WM="200"                  #Watermark interval (0 is disabled)
D_DSD="1"                   #Determinant Sharing Depth
D_KT="single_kill"          #Kill Type
D_ATTEMPTS=1                #Number of attempts for the configuration
D_SYSTEM="clonos"           #System to test
D_SHUFFLE="true"            #Shuffle or fully data-parallel
D_GRAPH_ONLY="true"         #Graph only (data-generators in graph) or with Kafka
D_THROUGHPUT="1000"         #Target throughput (negative number for unlimited)
D_PTI="5"                   #Perioc Time Interval
D_KEYS_PER_PARTITION="1000" #Number of keys each partition will process (defines fragmentation of state)

GEN_TS="false"              #Generate Timestamps on every record?
GEN_RANDOM="false"          #Generate Random number on every record?
GEN_SERIALIZABLE="false"    #Generate Serializable test object (a Long) on every record?

#Local (docker-compose) or on Kubernetes
LOCAL_EXPERIMENT="true"

INPUT_TOPIC=benchmark-input
OUTPUT_TOPIC=benchmark-output

if [ "$LOCAL_EXPERIMENT" = "true" ]; then
  FLINK_RPC=localhost:8081
  KAFKA_BOOTSTRAP=172.17.0.1:9092
  KAFKA_EXTERNAL_LOCATION=172.17.0.1:9092
  ZK_LOC=localhost:2181
else
  #We are running with a kubernetes service that exposes it on all ports
  FLINK_RPC=0.0.0.0:31234

  #Needs to be internal kafka loc. kubectl get svc | grep kafka | grep ClusterIP
  KAFKA_BOOTSTRAP=$(kubectl get svc | grep ClusterIP | grep kafka | grep -v headless | awk {'print $3'}):9092
  NODE_EXTERNAL_IP=$(ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)
  KAFKA_PORT=31090
  KAFKA_EXTERNAL_LOCATION=$NODE_EXTERNAL_IP:$KAFKA_PORT
  ZK_LOC=$(kubectl get svc | grep ClusterIP | grep zookeeper | grep -v headless | awk {'print $3'}):2181
fi


#How long to let the system warm-up
SYSTEM_INITIALIZATION_TIME=30

#Total time to measure the systems throughput or latency
EXPERIMENT_DURATION=120

#How long into EXPERIMENT_DURATION is the kill performed
TIME_TO_KILL=30
SLEEP_BETWEEN_RANDOM_KILLS=5

#Total time the system will run (the time external producers have to be active)
TOTAL_EXPERIMENT_TIME=$((EXPERIMENT_DURATION + SYSTEM_INITIALIZATION_TIME))

SLEEP_AFTER_KILL=$((EXPERIMENT_DURATION - TIME_TO_KILL + 10))


declare -a work_queue

# Specify runs by pushing configurations to work_queue
# Parameter order is the order variables are defined above
work_queue+=("$D_CI;$D_SS;$D_P;$D_D;$D_KD;$D_ST;$D_W_SIZE;$D_W_SLIDE;$D_AS;$D_O;$D_T;$D_TC;$D_LTI;$D_WM;$D_DSD;$D_KT;$D_ATTEMPTS;$D_SYSTEM;$D_SHUFFLE;$D_GRAPH_ONLY;$D_THROUGHPUT;$D_PTI;$D_KEYS_PER_PARTITION")

clear_make_topics() {
  local p=$1
  echo "Trying to clear topic" >&2

  ./kafka/bin/kafka-configs.sh --zookeeper "$ZK_LOC" --alter --entity-type topics --add-config retention.ms=1000 --entity-name $INPUT_TOPIC >&2
  ./kafka/bin/kafka-configs.sh --zookeeper "$ZK_LOC" --alter --entity-type topics --add-config retention.ms=1000 --entity-name $OUTPUT_TOPIC >&2

  sleep 1

  echo "Remove deletion mechanism" >&2

  ./kafka/bin/kafka-configs.sh --zookeeper "$ZK_LOC" --alter --entity-type topics --delete-config retention.ms --entity-name $INPUT_TOPIC >&2
  ./kafka/bin/kafka-configs.sh --zookeeper "$ZK_LOC" --alter --entity-type topics --delete-config retention.ms --entity-name $OUTPUT_TOPIC >&2

  sleep 1
  echo "Now delete topic" >&2

  ./kafka/bin/kafka-topics.sh --zookeeper "$ZK_LOC" --topic $INPUT_TOPIC --delete >&2

  ./kafka/bin/kafka-topics.sh --zookeeper "$ZK_LOC" --topic $OUTPUT_TOPIC --delete >&2

  sleep 1
  echo "Create topic" >&2

  ./kafka/bin/kafka-topics.sh --create --zookeeper "$ZK_LOC" --topic $INPUT_TOPIC --partitions "$p" --replication-factor 1 >&2
  ./kafka/bin/kafka-topics.sh --create --zookeeper "$ZK_LOC" --topic $OUTPUT_TOPIC --partitions "$p" --replication-factor 1 >&2
  echo "Done creating" >&2

  sleep 1

  #./kafka/bin/kafka-configs.sh --zookeeper $ZK_LOC --alter --entity-type topics --add-config retention.ms=30000 --entity-name $INPUT_TOPIC >&2
  #./kafka/bin/kafka-configs.sh --zookeeper $ZK_LOC --alter --entity-type topics --add-config retention.ms=30000 --entity-name $OUTPUT_TOPIC >&2
  #sleep 1

}

get_job_vertexes() {
  local jobid=$1

  response=$(curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid")
  vertex_ids=($(echo $response | jq '.vertices[] | .id' | tr -d '"'))
  echo "${vertex_ids[@]}"
}

get_vertex_host() {
  local jobid=$1
  local vertex=$2
  local p=$3
  local tm=$(curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid/vertices/$vertex/subtasks/$p" | jq '.host' | tr -d '"')
  echo "$tm"
}

kill_taskmanager() {
  taskmanager_to_kill=$1
  if [ "$LOCAL_EXPERIMENT" = "true" ]; then
    docker kill "$taskmanager_to_kill"
  else
    kubectl delete --grace-period=0 --force pod "$taskmanager_to_kill"
  fi
  echo "Kiled taskmanager $taskmanager_to_kill" >&2
}

perform_failures() {
  local jobid=$1
  local path=$2
  local d=$3
  local p=$4
  local kd=$5
  local killtype=$6

  # Get taskmanagers used by job
  local vertex_ids=($(get_job_vertexes $jobid))

  local taskmanagers_used=($(for vid in ${vertex_ids[@]}; do curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host' | tr -d '"' | tr ":" " " | awk {'print $1'}; done))
  echo "VERTEX IDS: ${vertex_ids[@]}" >&2
  echo "TASKMANAGERS : ${taskmanagers_used[@]}" >&2

  local kill_time=$(date +%s%3N)

  if [ "$killtype" = "single_kill" ]; then
    #Kill par 0 at the requested depth
    depth_to_kill=$((kd - 1))
    par_to_kill=0
    index_to_kill=$((depth_to_kill * p + par_to_kill))
    echo "Index to kill $index_to_kill" >&2
    local taskmanager_to_kill=${taskmanagers_used[$index_to_kill]}
    kill_taskmanager "$taskmanager_to_kill"
    echo "$taskmanager_to_kill $depth_to_kill $par_to_kill $kill_time" >>"$path"/killtime
  elif [ "$killtype" = "multi_kill" ]; then
    #Iterate Depths, killing one at each depth at  roughly same time
    for kdi in $(seq 1 "$d"); do
      par_to_kill=0
      index_to_kill=$((kdi * p + par_to_kill))
      echo "Index to kill $index_to_kill" >&2
      local taskmanager_to_kill=${taskmanagers_used[$index_to_kill]}
      kill_taskmanager "$taskmanager_to_kill"
      echo "$taskmanager_to_kill $kdi $par_to_kill $kill_time" >>"$path"/killtime
    done
  elif [ "$killtype" = "random_kill" ]; then
    #Iterate Depths, killing one random task at each depth and sleeping between
    for kdi in $(seq 1 "$d"); do
      par_to_kill=$((RANDOM % $p))
      index_to_kill=$((kdi * p + par_to_kill))
      echo "Index to kill $index_to_kill" >&2
      local taskmanager_to_kill=${taskmanagers_used[$index_to_kill]}
      sleep $SLEEP_BETWEEN_RANDOM_KILLS
    done
  fi
}

push_job() {
  local job=$1
  local response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job-$job.jar" http://$FLINK_RPC/jars/upload)
  echo "PUSH: $response" >&2
  local id=$(echo "$response" | jq '.filename' | tr -d '"' | tr "/" "\n" | tail -n1)
  sleep 10
  echo "$id"
}

run_job() {
  local jarid=$1
  local jobstr=$2

  IFS=";" read -r -a params <<<"${jobstr}"

  local ci="${params[0]}"
  local ss="${params[1]}"
  local p="${params[2]}"
  local d="${params[3]}"
  #local kd="${params[4]}" NOT USED HERE
  local st="${params[5]}"
  local wsize="${params[6]}"
  local wslide="${params[7]}"
  local as="${params[8]}"
  local operator="${params[9]}"
  local trans="${params[10]}"
  local timechar="${params[11]}"
  local lti="${params[12]}"
  local wi="${params[13]}"
  local dsd="${params[14]}"
  #local killtype="${params[15]}" NOT USED HERE
  #local attempts="${params[16]}" NOT USED HERE
  #local system="${params[17]}" NOT USED HERE
  local shuffle="${params[18]}"
  local graph="${params[19]}"
  local throughput="${params[20]}"
  local pti="${params[21]}"
  local keys="${params[22]}"

  data_str="{\"programArgs\":\" --experiment-checkpoint-interval-ms $ci --experiment-state-size $ss --experiment-parallelism $p --experiment-depth $d --sleep $st --experiment-window-size $wsize --experiment-window-slide $wslide --experiment-access-state $as --experiment-operator $operator --experiment-transactional $trans --experiment-time-char $timechar --experiment-latency-tracking-interval $lti --experiment-watermark-interval $wi --experiment-determinant-sharing-depth $dsd --experiment-shuffle $shuffle --experiment-overhead-measurement $graph --target-throughput $throughput --experiment-time-setter-interval $pti --num-keys-per-partition $keys --bootstrap.servers $KAFKA_BOOTSTRAP  --experiment-gen-ts $GEN_TS --experiment-gen-random $GEN_RANDOM --experiment-gen-serializable $GEN_SERIALIZABLE \"}"
  local response=$(curl -sS -X POST --header "Content-Type: application/json;charset=UTF-8" --data "$data_str" "http://$FLINK_RPC/jars/$jarid/run?allowNonRestoredState=false")
  echo "RUN: $response" >&2

  local job_id=$(echo "$response" | jq ".jobid" | tr -d '"')
  sleep 5
  echo $job_id
}

cancel_job() {
  local jobid=$(get_job_id)
  curl -sS -X PATCH "http://$FLINK_RPC/jobs/$jobid?mode=cancel" >/dev/null
  sleep 3
}

get_job_id() {
  local jobid=$(curl -sS -X GET "http://$FLINK_RPC/jobs" | jq '.jobs[0].id' | tr -d '"')
  echo $jobid
}

measure_throughput_from_metrics() {
  local p=$1
  local jobid=$2
  local path=$3
  local duration_measure=$4
  local FLINK_RPC=$5


  local response=$(curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid")
  local vertex_ids=($(echo $response | jq '.vertices[] | .id' | tr -d '"'))

  echo -e "TIME\tTHROUGHPUT" >>$path/throughput

  local sleep_time=$(echo "scale=4 ; 0.5 - $p * 0.05" | bc)

  if [ "$p" = "20" ] || [ "$p" = "15" ]; then
    local sleep_time=0
  fi

  while true; do
    local ts=$(date +%s%3N)
    local accum=0
    for i in $(seq 0 $((p - 1))); do
      local vmax=$(curl -sS "http:/$FLINK_RPC/jobs/$jobid/vertices/${vertex_ids[0]}/subtasks/$i/metrics?get=numRecordsOutPerSecond" | jq ".[0].value" | tr -d '"')
      local vmax=$(echo "${vmax%.*}")
      accum=$((accum + vmax))
    done
    echo "Throughput measured: $accum" >&2
    echo -e "$ts\t$accum" >>$path/throughput
    sleep 5
  done

}

measure_latency_from_metrics() {
  local p=$1
  local d=$2
  local jobid=$3
  local path=$4
  local duration_measure=$5
  local shuffle=$6
  local FLINK_RPC=$7

  local response=$(curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid")
  local vertex_ids=($(echo $response | jq '.vertices[] | .id' | tr -d '"'))

  echo -e "TIME\tLATENCY" >>$path/latency

  while true; do
    local ts=$(date +%s%3N)
    local i=$((RANDOM % p))

    local o=$i

    if [ "$shuffle" = "true" ]; then
      local o=$((RANDOM % p))
    fi
    local lat=$(curl -sS "http:/$FLINK_RPC/jobs/$jobid/metrics?get=latency.source_id.${vertex_ids[0]}.operator_id.${vertex_ids[$((d + 2 - 1))]}.operator_subtask_index.$o.latency_mean" | jq ".[0].value" | tr -d '"')
    echo "Measured latency: $lat" >&2
    local lat=$(echo "${lat%.*}")
    echo -e "$ts\t$lat" >>$path/latency
    sleep 0.5 #125
  done
}

measure_sustainable() {
  local jobstr=$1
  local path=$2
  echo "=== Measure sustainable ===" >&2

  IFS=";" read -r -a params <<<"${jobstr}"

  local p="${params[2]}"
  local d="${params[3]}"
  local job="${params[17]}"
  local shuffle="${params[18]}"
  local graphonly="${params[19]}"

  local id=$(push_job "$job")


  if [ "$graphonly" = "false" ]; then
    clear_make_topics "$p"
		echo "Starting Kafka Producer!!!" >&2 
		if [ "$LOCAL_EXPERIMENT" = "true" ] ; then
			local num_records=$((throughput * TOTAL_EXPERIMENT_TIME))
			timeout $TOTAL_EXPERIMENT_TIME bash -c "./kafka/bin/kafka-producer-perf-test.sh --dist-producer-index 0 --dist-producer-total 1 --topic $INPUT_TOPIC --num-records $num_records --throughput $throughput --producer-props bootstrap.servers=$KAFKA_EXTERNAL_LOCATION key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer 2> kafka-err 1> kafka-out" >&2 &
		else
			./distributed-producer.sh $TOTAL_EXPERIMENT_TIME "$KAFKA_EXTERNAL_LOCATION" $INPUT_TOPIC "$benchmarkers" >&2 &
		fi
  fi

  local jobid=$(run_job $id $jobstr)

  sleep $SYSTEM_INITIALIZATION_TIME

  monitor_all_vertexes_network $EXPERIMENT_DURATION $path $jobid $p $d 2>/dev/null >&2

  export -f measure_latency_from_metrics
  export -f measure_throughput_from_metrics

  if [ "$graphonly" = "true" ]; then
    echo "Starting latency and throughput measurers" >&2
    timeout $EXPERIMENT_DURATION bash -c "measure_latency_from_metrics $p $d $jobid $path $EXPERIMENT_DURATION $shuffle $FLINK_RPC" &
    timeout $EXPERIMENT_DURATION bash -c "measure_throughput_from_metrics $p $jobid $path $EXPERIMENT_DURATION $FLINK_RPC" &

		#Column indices
		thr_index=2
		lat_index=2
  else
		local resolution=10000
    python3 ./endtoendlatency/Main.py -k "$KAFKA_EXTERNAL_LOCATION" -o "$OUTPUT_TOPIC" -r $resolution -p $p -d $EXPERIMENT_DURATION -mps 3 -t $trans >$path/latency &
    python3 ./throughput-measurer/Main.py $EXPERIMENT_DURATION 2 "$KAFKA_EXTERNAL_LOCATION" $OUTPUT_TOPIC verbose > $path/throughput &

		#Column indices
		thr_index=2
		lat_index=4
  fi
	#Sleep while waiting for measurements to complete
  sleep $((EXPERIMENT_DURATION + 2))

  local max=$(cat "$path"/throughput | awk {'print $'"$thr_index"} | tail -n+10 | tac | tail -n+10 | jq -s add/length)
  local max=$(echo "${max%.*}")
  echo "Average Thr: $max" >&2
  echo $max >>$path/avg-throughput
	
	local lat=$(cat "$path"/latency | awk {'print $'"$lat_index"} | tail -n+10 | tac | tail -n+10 | jq -s add/length)
	local lat=$(echo "${lat%.*}")
	echo "Average Lat: $lat" >&2
  echo "$lat" >>"$path"/avg-latency

  log_taskmanagers $jobid $path/logs $p $d

  cancel_job

  echo "$max"
}

monitor_all_vertexes_network() {
  local duration=$1
  local path=$2
  local jobid=$3
  local p=$4
  local d=$5

  mkdir -p $path/network

  local vertex_ids=($(get_job_vertexes $jobid))
  local tms_used=($(for vid in ${vertex_ids[@]}; do curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host' | tr -d '"' | tr ":" " " | awk {'print $1'}; done))

  for di in $(seq 0 $((d + 2 - 1))); do
    for pi in $(seq 0 $((p - 1))); do
      tm=${tms_used[$((di * p + pi))]}
      . ./monitor_network.sh $duration $tm $LOCAL_EXPERIMENT >>$path/network/network-$di-$pi &
    done
  done

  if [ "$LOCAL_EXPERIMENT" = "true" ]; then
    local all_tms=($(docker ps | grep taskmanager | awk {'print $1'}))
  else
    local all_tms=($(kubectl get pods | grep taskmanager | awk {'print $1'}))
  fi

  #Standby tms are the unique ones when both lists are joined
  local standby_tms=$(echo ${tms_used[@]} ${all_tms[@]} | tr ' ' '\n' | sort | uniq -u)

  for tm in ${standby_tms[@]}; do
    . ./monitor_network.sh $duration $tm $LOCAL_EXPERIMENT >>$path/network/network-standby-$tm &
  done

}

run_experiment() {
  local jobstr=$1
  local path=$2
  IFS=";" read -r -a params <<<"${jobstr}"

  local ci="${params[0]}"
  local ss="${params[1]}"
  local p="${params[2]}"
  local d="${params[3]}"
  local kd="${params[4]}"
  local st="${params[5]}"
  local wsize="${params[6]}"
  local wslide="${params[7]}"
  local as="${params[8]}"
  local operator="${params[9]}"
  local trans="${params[10]}"
  local timechar="${params[11]}"
  local lti="${params[12]}"
  local wi="${params[13]}"
  local dsd="${params[14]}"
  local killtype="${params[15]}"
  local attempts="${params[16]}"
  local system="${params[17]}"
  local shuffle="${params[18]}"
  local graphonly="${params[19]}"
  local throughput="${params[20]}"
  local pti="${params[21]}"
  local keys="${params[22]}"

  if [ "$graphonly" = "false" ]; then
    clear_make_topics $p
  fi

  id=$(push_job $system)

  local sustainable=$throughput
  echo "Setting throughput at $sustainable r/s" >&2

  mkdir -p "$path"

  if [ "$graphonly" = "false" ]; then
		if [ "$LOCAL_EXPERIMENT" = "true" ] ; then
			local num_records=$((throughput * TOTAL_EXPERIMENT_TIME))
			timeout $TOTAL_EXPERIMENT_TIME bash -c "./kafka/bin/kafka-producer-perf-test.sh --dist-producer-index 0 --dist-producer-total 1 --topic $INPUT_TOPIC --num-records $num_records --throughput $throughput --producer-props bootstrap.servers=$KAFKA_EXTERNAL_LOCATION key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer 2> kafka-err 1> kafka-out" >&2 &
		else
			echo "Starting distributed producer to $KAFKA_EXTERNAL_LOCATION" >&2
			. ./distributed-producer.sh $TOTAL_EXPERIMENT_TIME "$sustainable" "$KAFKA_EXTERNAL_LOCATION" $INPUT_TOPIC "$benchmarkers" >&2 &
		fi
  fi

  sleep 3

  local jobid=$(run_job "$id" "$jobstr")

  sleep $SYSTEM_INITIALIZATION_TIME

  local vertex_ids=($(get_job_vertexes $jobid))
  local resolution=$((sustainable / 5 + 1))
  local resolution=$(echo "${resolution%.*}")
  if [ "$operator" = "window" ]; then
    local resolution=$(((wsize / wslide) * 1000 / 5 + 1))
  fi
  local resolution=$(echo "${resolution%.*}")

  monitor_all_vertexes_network $EXPERIMENT_DURATION $path $jobid $p $d 2>/dev/null >&2
  if [ "$graphonly" = "false" ]; then
    python3 ./endtoendlatency/Main.py -k $KAFKA_EXTERNAL_LOCATION -o $OUTPUT_TOPIC -r $resolution -p $p -d $EXPERIMENT_DURATION -mps 3 -t $trans >$path/latency &
    python3 ./throughput-measurer/Main.py $EXPERIMENT_DURATION 3 $KAFKA_EXTERNAL_LOCATION $OUTPUT_TOPIC verbose >$path/throughput &
  else
    #export -f measure_latency_from_metrics
    export -f measure_throughput_from_metrics
    #timeout $EXPERIMENT_DURATION bash -c "measure_latency_from_metrics $p $d $jobid $path $EXPERIMENT_DURATION $shuffle $FLINK_RPC" &
    timeout $EXPERIMENT_DURATION bash -c "measure_throughput_from_metrics $p $jobid $path $EXPERIMENT_DURATION $FLINK_RPC" &
  fi

  sleep $TIME_TO_KILL

  perform_failures "$jobid" "$path" $d $p $kd $killtype

  sleep $SLEEP_AFTER_KILL

  if [ "$trans" = "true" ]; then
    ./extract-transaction-commits.sh $KAFKA_EXTERNAL_LOCATION $path
  fi

  log_taskmanagers $jobid $path/logs $p $d

  echo "Canceling the job with id $jobid" >&2
  cancel_job
}

log_taskmanagers() {
  jobid=$1
  path=$2
  p=$3
  d=$4

	mkdir -p $path

  local vertex_ids=($(get_job_vertexes $jobid))
  local tms_used=($(for vid in ${vertex_ids[@]}; do curl -sS -X GET "http://$FLINK_RPC/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host' | tr -d '"' | tr ":" " " | awk {'print $1'}; done))
  echo "VERTEX IDS: ${vertex_ids[@]}" >&2
  echo "TASKMANAGERS : ${tms_used[@]}" >&2

  for di in $(seq 0 $((d + 2 - 1))); do
    for pi in $(seq 0 $((p - 1))); do
      tm=${tms_used[$((di * p + pi))]}

      if [ "$LOCAL_EXPERIMENT" = "true" ]; then
        docker logs $tm >>$path/worker-$di-$pi
      else
        kubectl logs $tm >>$path/worker-$di-$pi
      fi
    done
  done

  if [ "$LOCAL_EXPERIMENT" = "true" ]; then
    local all_tms=($(docker ps | grep taskmanager | awk {'print $1'}))
  else
    local all_tms=($(kubectl get pods | grep taskmanager | awk {'print $1'}))
  fi

  #Standby tms are the unique ones when both lists are joined
  local standby_tms=$(echo "${tms_used[@]} ${all_tms[@]}" | tr ' ' '\n' | sort | uniq -u)

  for tm in ${standby_tms[@]}; do
    if [ "$LOCAL_EXPERIMENT" = "true" ]; then
      docker logs $tm >>$path/worker-standby-$tm
    else
      kubectl logs $tm >>$path/worker-standby-$tm
    fi
  done

  if [ "$LOCAL_EXPERIMENT" = "true" ]; then
    for i in $(docker ps | grep $system | grep jobm | awk {'print $1'}); do docker logs $i >$path/master; done
  else
    for i in $(kubectl get pods | grep $system | grep jobm | awk {'print $1'}); do kubectl logs $i >$path/master; done
  fi
}

reset_cluster() {
  num_nodes_req=$1
  if [ "$LOCAL_EXPERIMENT" = "true" ]; then
		docker-compose down -v && docker-compose up -d --scale taskmanager=$num_nodes_req
  else
    kubectl delete pod $(kubectl get pods | grep flink | awk {'print $1'})
  fi
  sleep 20 #Allow cluster to initialize
}

echo "Beggining experiments"

date=$(date +%Y-%m-%d_%H:%M)
results="results-$date"
mkdir -p "$results"

experiment_number=0
for jobstr in "${work_queue[@]}"; do
  IFS=";" read -r -a params <<<"${jobstr}"

  ci="${params[0]}"
  ss="${params[1]}"
  p="${params[2]}"
  d="${params[3]}"
  kd="${params[4]}"
  st="${params[5]}"
  wsize="${params[6]}"
  wslide="${params[7]}"
  as="${params[8]}"
  operator="${params[9]}"
  trans="${params[10]}"
  timechar="${params[11]}"
  lti="${params[12]}"
  wi="${params[13]}"
  dsd="${params[14]}"
  killtype="${params[15]}"
  attempts="${params[16]}"
  system="${params[17]}"
  shuffle="${params[18]}"
  graphonly="${params[19]}"
  throughput="${params[20]}"
  pti="${params[21]}"
  keys="${params[22]}"

  num_nodes_req=$((p * (d+ 2) * 2)) # * 2 for standbys
  mkdir -p "$results"/experiment-$experiment_number

  echo -e "CI\tSS\tP\tD\tKD\tST\tWSIZE\tWSLIDE\tAS\tOPERATOR\tTRANS\tTC\tLTI\tWI\tDSD\tKILLTYPE\tATTEMPTS\tSYSTEM\tSHUFFLE\tOVERHEAD\tTHROUGHPUT\tPTI\tKEYS" >>"$results"/experiment-$experiment_number/configuration
  echo -e "$ci\t$ss\t$p\t$d\t$kd\t$st\t$wsize\t$wslide\t$as\t$operator\t$trans\t$timechar\t$lti\t$wi\t$dsd\t$killtype\t$attempts\t$system\t$shuffle\t$graphonly\t$throughput\t$pti\t$keys" >>$results/experiment-$experiment_number/configuration

  path=$results/experiment-$experiment_number
  mkdir -p "$path"

  echo "Run experiment configuration:"
  echo "  System: $system"
  echo "  Graph topology: Source - $d operators - Sink"
  echo "  Parallelism degree: $p"
  echo "  State: $ss bytes"
  echo "  Num Keys: $keys"
  echo "  Checkpoint frequency: $ci milliseconds"
  echo "  Kill depth: $kd"
  echo "  Sleep time: $st milliseconds"
  echo "  Access state %: $as"
  echo "  Window size: $wsize"
  echo "  Operator: $operator"
  echo "  Transactional: $trans"
  echo "  Time Characteristic: $timechar"
  echo "  Watermark Interval: $wi"
  echo "  Determinant Sharing Depth: $dsd"
  echo "  Kill Type: $killtype"
  echo "  Shuffle: $shuffle"
  echo "  Attempts: $attempts"
  echo "  Overhead: $graphonly"
  echo "  Throughput: throughput"

  #max=0
  #maxes=""
  #for a in $(seq 1 "$attempts"); do
  #  reset_cluster $num_nodes_req
  #  full_path="$path"/sustainable/"$a"
  #  mkdir -p "$full_path"
  #  max=$(measure_sustainable "${jobstr}" "$full_path")
  #  echo -e "Maximum throughput: $max records/second"
  #  maxes="${maxes}$max\n"
  #done
  #echo -e "$maxes" >"$path"/avg-max-throughput
  #max=$(echo -e $maxes | jq -s add/length) #Average
  #max=$(echo "${max%.*}")
  #echo -e "Average of averages ($maxes): $max" >&2

	reset_cluster $num_nodes_req
  run_experiment "${jobstr}" $path


  experiment_number=$((experiment_number + 1))
done
