#!/bin/bash

date=`date +%Y-%m-%d_%H:%M`
results="results-$date"

mkdir -p $results

# Experiment dimensions
declare -a state_sizes
declare -a checkpoint_frequencies
declare -a graph_depths
declare -a parallelism_degrees
declare -a input_rates

# MB
state_sizes=( 100000000 500000000 1000000000)
# Milliseconds
checkpoint_frequencies=(100 1000 10000)
# Operators between source and sink
graph_depths=( 1 3 5 )
# Degrees
parallelism_degrees=( 1 5 10 20 )

default_ss=10000000
default_cf=1000
default_p=5
default_d=3
default_kd=1
default_st=0
default_as=false

attempts=1

flink_rpc=0.0.0.0:31234
#Needs to be internal kafka loc. kubectl get svc | grep kafka | grep ClusterIP
kafka_bootstrap=$(kubectl get svc | grep ClusterIP | grep kafka | grep -v headless | awk {'print $3'}):9092
node_external_ip=$(ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)
kafka_ext=$node_external_ip:31090
zk_port=$(kubectl get svc | grep  zookeeper | grep NodePort | awk '{print $5}' | tr ":" "\n" | tr "/" "\n" | head -n 2 | tail -n 1)
zk_loc=0.0.0.0:$zk_port

benchmarkers=$@


input_topic=benchmark-input
output_topic=benchmark-output

declare -a work_queue

# Specify runs by pushing configurations to work_queue
#access state
work_queue[0]="$default_cf;$default_ss;$default_p;1;$default_kd;$default_st;$default_as;1000000"
work_queue[1]="$default_cf;$default_ss;$default_p;5;$default_kd;$default_st;$default_as;1000000"
work_queue[2]="$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;true;1000000"

#1MB,100MB,1GB
#work_queue[0]="$default_cf;1000000;$default_p;$default_d;$default_kd;$default_st;$default_as;1000000"
#work_queue[0]="$default_cf;100000000;$default_p;$default_d;$default_kd;$default_st;$default_as;1000000"
#work_queue[0]="$default_cf;1000000000;$default_p;$default_d;$default_kd;$default_st;$default_as;1000000"


clear_make_topics() {
	local p=$1
	echo "Trying to clear topic" >&2

	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=1000 --entity-name $input_topic >&2
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=1000 --entity-name $output_topic >&2

	sleep 5

	echo "Remove deletion mechanism" >&2

	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --delete-config retention.ms --entity-name $input_topic >&2
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --delete-config retention.ms --entity-name $output_topic >&2

	sleep 5
	echo "Now delete topic" >&2

	./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $input_topic --delete >&2 

	./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $output_topic --delete >&2 

	sleep 10
	echo "Create topic" >&2


	./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $input_topic --partitions $p --replication-factor 1 >&2

	./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $output_topic --partitions $p --replication-factor 1  >&2
	echo "Done creating" >&2 

	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=30000 --entity-name $input_topic >&2
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=30000 --entity-name $output_topic >&2
	sleep 5

}

kill_taskmanager() {

    	local jobid=$1
	local path=$2
    	local depth_to_kill=$3
	# Get taskmanagers used by job
	local response=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$1")
	local vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))
	echo "Vertexes used: ${vertex_ids[@]}"

	local taskmanagers_used=($(for vid in ${vertex_ids[@]} ; do curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host'  |  tr -d '"' | tr ":" " " | awk {'print $1'}  ; done))
	echo "Taskmanagers used: ${taskmanagers_used[@]}"

	# Kill the zeroeth tm of subtask of index to kill
	local taskmanager_to_kill=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid/vertices/${vertex_ids[$depth_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')

	local kill_time=$(( $(date '+%s%N') / 1000000))
	kubectl delete pod $taskmanager_to_kill
	echo "Kiled taskmanager $taskmanager_to_kill at $kill_time"
	echo $kill_time > $path/killtime

}

push_job() {
    local response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job-marios.jar" http://0.0.0.0:31234/jars/upload)
    echo "$response" >&2
    local id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)
    echo "$id"
}

measure_sustainable_throughput() {
    local cf=$1
    local ss=$2
    local p=$3
    local d=$4
    local st=$5
    local as=$6
    local fg=$7

        echo "Run experiment configuration:" >&2
        echo "   Graph topology: Source - $d operators - Sink" >&2
        echo "   Parallelism degree: $p" >&2
        echo "   State: $ss bytes" >&2
        echo "   Frag Size: $fg bytes" >&2
        echo "   Checkpoint frequency: $cf milliseconds" >&2
        echo "   Sleep time: $st milliseconds" >&2
        echo "   Access state: $as" >&2
        echo "   Input rate: Sustainable" >&2
	echo "Kafka $kafka_bootstrap" >&2

   
    clear_make_topics $p
    local id=$(push_job)
    
    response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-checkpoint-interval-ms $cf --experiment-state-fragment-size $fg --experiment-access-state $as --experiment-depth $d --experiment-parallelism $p --sleep $st\"}" "http://0.0.0.0:31234/jars/$id/run?allowNonRestoredState=false")
    echo "$response" >&2
    sleep 15

     ./distributed-producer.sh 230 5 10000000 $kafka_ext $input_topic  $benchmarkers >&2 & 
    sleep 60
    local max1=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $output_topic silent)
    echo "measure 1 $max1" >&2
    local max2=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $output_topic silent)
    echo "measure 2 $max2" >&2
    local max3=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $output_topic silent)
    echo "measure 3 $max3" >&2
    local maxin=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $input_topic silent)
    echo "measure input $maxin" >&2
    local max4=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $output_topic silent)
    echo "measure 4 $max4" >&2
    local max5=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $output_topic silent)
    echo "measure 5 $max5" >&2
    local max6=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $output_topic silent)
    echo "measure 6 $max6" >&2
    local max7=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $output_topic silent)
    echo "measure 7 $max7" >&2

    local max=$(echo -e "$max1\n$max2\n$max3\n$max4\n$max5\n$max6\n$max7" | sort -n -r | head -n1)
    
    local jobid=$(curl -sS -X GET "http://0.0.0.0:31234/jobs" |  jq  '.jobs[0].id' | tr -d '"')
    curl -sS -X PATCH "http://0.0.0.0:31234/jobs/$jobid?mode=cancel" > /dev/null
    sleep 3
    
    kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})  >&2
    sleep 30
    echo "$max"
}

run_experiment() {
	local cf=$1
    local ss=$2
	local p=$3
	local d=$4
	local setting=$5
	local max=$6
	local path=$7
	local depth_to_kill=$8
	local st=$9
	local as=${10}
	local fg=${11}

        echo "Run experiment configuration:"
        echo "   Graph topology: Source - $d operators - Sink"
        echo "   Parallelism degree: $p"
        echo "   State: $ss bytes"
        echo "   Frag Size: $fg bytes"
        echo "   Checkpoint frequency: $cf milliseconds"
        echo "   Kill depth: $kd"
        echo "   Sleep time: $st milliseconds"
        echo "   Access state: $as"
        echo "   Input rate: Sustainable"

        echo "   Path: $path"
        echo "   Max: $max"
        echo "   setting: $setting"



	clear_make_topics $p
    id=$( push_job )

	local sustainable=$(echo "$setting * $max" | bc)
	echo "Setting throughput at $sustainable r/s" >&2
	mkdir -p $path/$setting
	echo $sustainable > $path/$setting/sustainable

	response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-state-fragment-size $fg --experiment-access-state $as --experiment-checkpoint-interval-ms $cf --experiment-depth $d --experiment-parallelism $p --sleep $st\"}" "http://0.0.0.0:31234/jars/$id/run?allowNonRestoredState=false")
	echo "Job run: $response"
	sleep 15
	local jobid=$(curl -sS -X GET "http://0.0.0.0:31234/jobs" |  jq  '.jobs[0].id' | tr -d '"')

	. ./distributed-producer.sh 200 5 $sustainable $kafka_ext $input_topic  $benchmarkers > /dev/null &
	sleep 60
	python3 ./throughput-measurer/Main.py  120 3 $kafka_ext $output_topic verbose > $path/$setting/throughput &
    . ./latmeasurer.sh $jobid 0.1 1200 $d >> $path/$setting/latmeasured &

	sleep 60
	kill_taskmanager $jobid $path/$setting $depth_to_kill
    local maxin=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $input_topic silent)
    echo "measure input $maxin" >&2
	sleep 60

	response=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid")
	vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))

	echo "Saving logs"
	local jobmanager=$(kubectl get pods | grep jobmanager | awk '{print $1}')
	local new_host=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid/vertices/${vertex_ids[$depth_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')
	local src_pod=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid/vertices/${vertex_ids[0]}/subtasks/0" | jq '.host' |  tr -d '"')
	local sink_pod=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid/vertices/${vertex_ids[$(( $d + 1 ))]}/subtasks/0" | jq '.host' |  tr -d '"')

	echo "Canceling the job with id $jobid"
	curl -sS -X PATCH "http://0.0.0.0:31234/jobs/$jobid?mode=cancel"


	kubectl logs $jobmanager > $path/$setting/JOBMANAGER-LOGS
	kubectl logs $new_host > $path/$setting/STANDBY-LOGS
	kubectl logs $src_pod > $path/$setting/SRC-LOGS
	kubectl logs $sink_pod > $path/$setting/SINK-LOGS

	#mkdir -p $path/$setting/logs
	#for pod in $(kubectl get pod | grep flink | awk {'print $1'}) ; do kubectl logs $pod > $path/$setting/logs/$pod ; done

	kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})

}


echo "Beggining experiments"


for job in "${work_queue[@]}"
do
    IFS=";" read -r -a arr <<< "${job}"

    cf="${arr[0]}"
    ss="${arr[1]}"
    p="${arr[2]}"
    d="${arr[3]}"
    kd="${arr[4]}"
    st="${arr[5]}"
    as="${arr[6]}"
    fg="${arr[7]}"

    for a in $(seq 1 $attempts) ; do #attempt
        path=$results/cf-$cf/ss-$ss/par-$p/dep-$d/kill-$kd/sleep-$st/as-$as/attempt-$a
        mkdir -p $path

        echo "Run experiment configuration:"
        echo "   Graph topology: Source - $d operators - Sink"
        echo "   Parallelism degree: $p"
        echo "   State: $ss bytes"
        echo "   Frag Size: $fg bytes"
        echo "   Checkpoint frequency: $cf milliseconds"
        echo "   Kill depth: $kd"
        echo "   Sleep time: $st milliseconds"
        echo "   Access state: $as"
        echo "   Input rate: Sustainable"

  		max=$( measure_sustainable_throughput $cf $ss $p $d $st $as $fg )
  		echo -e "Maximum throughput: $max records/second"

		sleep 15

  		########### Run throughput experiment

  		echo -e "\n\nRunning setting 0.9"

  		run_experiment $cf $ss $p $d 0.9 $max $path $kd $st $as $fg
  		 	
		sleep 10

  		echo -e "\n\nRunning setting 0.5"

  		########### Run latency experiment
  		run_experiment $cf $ss $p $d 0.5 $max $path $kd $st $as $fg

		#for pod in $(kubectl get pod | grep flink | awk {'print $1'}) ; do kubectl logs $pod > $path/logs/$pod ; done

		#kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})
		sleep 10

        done
done

