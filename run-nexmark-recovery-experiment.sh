#!/bin/bash

date=`date +%Y-%m-%d_%H:%M`
results="results-$date"

mkdir -p $results

d_query=3
d_p=5
d_kd=3
d_attempts=1
d_er=0

#We are running with a kubernetes service that exposes it on all ports
flink_rpc=0.0.0.0:31234

#Needs to be internal kafka loc. kubectl get svc | grep kafka | grep ClusterIP
kafka_bootstrap=$(kubectl get svc | grep ClusterIP | grep kafka | grep -v headless | awk {'print $3'}):9092
node_external_ip=$(ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)
kafka_ext=$node_external_ip:31090
#zk_port=$(kubectl get svc | grep  zookeeper | grep NodePort | awk '{print $5}' | tr ":" "\n" | tr "/" "\n" | head -n 2 | tail -n 1)

#We are running with a kubernetes service that exposes it on all ports
zk_loc=$(kubectl get svc | grep ClusterIP | grep zookeeper | grep -v headless | awk {'print $3'}):2181

benchmarkers=$@


input_topic=benchmark-input
output_topic=benchmark-output

system="clonos"

totalexperimenttime=230
inittime=60
timetokill=60
duration=$(( totalexperimenttime - inittime ))

sleep_between_kills=5

declare -a work_queue

# Specify runs by pushing configurations to work_queue

#Assuming we have local recovery + Standby recovery:
#We run most experiments with transactions off, to be able to better vizualize the recovery delay
#-------- Start off with local recovery

#Show our version stops less, using nontransactional system
#Get per partition data
#             |CHK INTERVAL|STATESIZE|PARALLELISM|DEPTHOFGRAPH|KILLDEPTH|SLEEPTIME  |WINDOWSIZE     |WINDOWSLIDE  |ACCESSSTATE|STATEFRAGMENTSIZE| OPERATOR| TRANSACTIONAL| TIME CHARACTERISTIC | WATERMARK_INTERVAL | KILL TYPE | NUM ATTEMPTS | SYSTEM | SHUFFLE |  OVERHEAD | THROUGHPUT
#=================================================================================== Overhead measurements


# QUERY|PAR|KD|ER|ATTEMPT|

#work_queue+=("4;5;1;500000;1")

#Kill the stateful task
#work_queue+=("3;5;2;500000;1")

#Kill the stateless task
#work_queue+=("3;5;1;500000;1")

# Q5 kill the stateful task
work_queue+=("5;5;1;500000;1")

#work_queue+=("8;10;1;500000;1")

#work_queue+=("3;5;2;1000000;1")


clear_make_topics() {
	local p=$1
	echo "Trying to clear topic" >&2

	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=1000 --entity-name $input_topic >&2
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=1000 --entity-name $output_topic >&2

	sleep 1

	echo "Remove deletion mechanism" >&2

	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --delete-config retention.ms --entity-name $input_topic >&2
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --delete-config retention.ms --entity-name $output_topic >&2

	sleep 1
	echo "Now delete topic" >&2

	./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $input_topic --delete >&2 

	./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $output_topic --delete >&2 

	sleep 1
	echo "Create topic" >&2


	./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $input_topic --partitions $p --replication-factor 1 >&2
	./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $output_topic --partitions $p --replication-factor 1  >&2
	echo "Done creating" >&2 

	sleep 1

	#./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=30000 --entity-name $input_topic >&2
	#./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=30000 --entity-name $output_topic >&2
	#sleep 1

}

get_job_vertexes() {
	local jobid=$1

	response=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid")
	vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))
	echo "${vertex_ids[@]}"
}

get_vertex_host() {
	local jobid=$1
	local vertex=$2
	local p=$3
	local tm=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/$vertex/subtasks/$p" | jq '.host' |  tr -d '"')
	echo "$tm"
}

kill_taskmanager() {

    	local jobid=$1
	local path=$2
    	local depth_to_kill=$3 #zero based
	local par_to_kill=$4 #zero based
	local p_tot=$5

	# Get taskmanagers used by job
	local vertex_ids=($(get_job_vertexes $jobid))

	echo "VERTEX IDS: ${vertex_ids[@]}" >&2
	local taskmanagers_used=($(for vid in ${vertex_ids[@]} ; do curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host'  |  tr -d '"' | tr ":" " " | awk {'print $1'}  ; done))
	echo "TASKMANAGERS : ${taskmanagers_used[@]}" >&2


	local kill_time=$(date +%s%3N) 

	
	echo "Index to kill $((depth_to_kill*p_tot + par_to_kill ))" >&2
	local taskmanager_to_kill=${taskmanagers_used[$((depth_to_kill*p_tot + par_to_kill ))]}
	kubectl delete --grace-period=0 --force pod $taskmanager_to_kill
	echo "Kiled taskmanager $taskmanager_to_kill at $kill_time" >&2
	echo "$taskmanager_to_kill $depth_to_kill $par_to_kill $kill_time" >> $path/killtime
}


run_job() {
	local jarid=$1
	local jobstr=$2

	IFS=";" read -r -a params <<< "${jobstr}"

	# QUERY|PAR|KD|ER|ATTEMPT|
	local q="${params[0]}"
	local par="${params[1]}"
	local kd="${params[2]}"
	local er="${params[3]}"
	local attempts="${params[4]}"
	
	sleep 5
	echo $job_id
}

cancel_job() {
    local jobid=$(get_job_id)
    curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel" > /dev/null
    sleep 3
}

get_job_id() {
    #local jobid=$(curl -sS -X GET "http://$flink_rpc/jobs" |  jq  '.jobs[0].id' | tr -d '"')
    local jobid=$(cat ./jobid)
    echo $jobid
}

monitor_all_vertexes_network() {
	local duration=$1
	local path=$2
	local jobid=$3
	local p=$4
	local d=$5

	mkdir -p $path/network

	local vertex_ids=($(get_job_vertexes $jobid))
	local pods_used=($(for vid in ${vertex_ids[@]} ; do curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host'  |  tr -d '"' | tr ":" " " | awk {'print $1'}  ; done))

	for di in $(seq 0 $((d+2-1))); do
		for pi in $(seq 0 $((p-1))); do
			. ./monitor_network.sh $duration ${pods_used[$((di*p + pi))]} >> $path/network/network-$di-$pi &
		done
	done

	local all_pods=($(kubectl get pods | grep taskmanager | awk {'print $1'}))

	local standby_tms=$(echo ${pods_used[@]} ${all_pods[@]} | tr ' ' '\n' | sort | uniq -u )
	for x in ${standby_tms[@]} ; do
			. ./monitor_network.sh $duration $x >> $path/network/network-$x &
	done

}

run_experiment() {
	local jobstr=$1
	local path=$2

	IFS=";" read -r -a params <<< "${jobstr}"

	local q="${params[0]}"
	local p="${params[1]}"
	local kd="${params[2]}"
	local er="${params[3]}"
	local attempts="${params[4]}"

	clear_make_topics $p
	
	echo "Running publisher"
	timeout $((duration + 35)) bash -c "./nexmark-publisher.sh $system $q 5 $kafka_bootstrap $er" &
	sleep 5

	echo "Running Job"
	timeout $((duration + 30)) bash -c "./nexmark-job.sh $system $q $p $kafka_bootstrap" &
	sleep 30

	sleep $inittime

	jobid=$(get_job_id)

	local vertex_ids=($(get_job_vertexes $jobid))
	local resolution=2
	
	monitor_all_vertexes_network $duration $path/$setting $jobid $p $d 2> /dev/null >&2
		python3 ./endtoendlatency/Main.py -k $kafka_ext  -o $output_topic -r $resolution -p $p -d $duration -mps 1 -t false > $path/$setting/latency &
		python3 ./throughput-measurer/Main.py  $duration 1 $kafka_ext $output_topic verbose > $path/$setting/throughput &

	echo "Single Kill" >&2
	sleep $timetokill
	kill_taskmanager $jobid $path/$setting  $kd 0 $p 

	sleep $(( duration - timetokill ))

	echo "Saving logs"
	local jobmanager=$(kubectl get pods | grep jobmanager | awk '{print $1}')
	local new_host=$(get_vertex_host $jobid "${vertex_ids[$kd]}" 0)

	local vertex_ids=($(get_job_vertexes $jobid))
	local pods=($(for vid in ${vertex_ids[@]} ; do curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host'  |  tr -d '"' | tr ":" " " | awk {'print $1'}  ; done))
	mkdir -p $path/$setting/logs
	for pod in ${pods[@]}; do
		kubectl logs $pod > $path/$setting/logs/$pod
	done

	for i in $(kubectl get pods | grep flink | grep jobm | awk {'print $1'}) ; do kubectl logs $i > $path/$setting/logs/$i ; done

	echo "Canceling the job with id $jobid"
	cancel_job
}


echo "Beggining experiments"

experiment_number=0
for jobstr in "${work_queue[@]}"
do
	IFS=";" read -r -a params <<< "${jobstr}"

	q="${params[0]}"
	p="${params[1]}"
	kd="${params[2]}"
	er="${params[3]}"
	attempts="${params[4]}"	

	mkdir -p $results/experiment-$experiment_number
	
	path=$results/experiment-$experiment_number
	mkdir -p $path
	
	echo "Run experiment configuration:"
	echo "   System: $system"
	echo "	 Query: $q"
	echo "	 Par: $p"
	echo "	 KD: $kd"
	echo "	 ER: $er"

	run_experiment "${jobstr}" $path 
	kubectl delete pod  $(kubectl get pods | grep flink | awk {'print $1'})
	sleep 4
	
	experiment_number=$((experiment_number + 1))
done

