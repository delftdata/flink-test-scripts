#!/bin/bash

date=`date +%Y-%m-%d_%H:%M`
results="results-$date"

mkdir -p $results


default_ss=10000000
#default_ss=1000000000
default_fs=10000000
default_cf=1000
default_p=2
default_d=2
default_kd=1
default_st=1000
default_as=false
default_w_size=5
default_w_slide=0.1
default_attempts=1


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

mapjob="clonos-map"
windowjob="clonos-window"

declare -a work_queue

# Specify runs by pushing configurations to work_queue

#Assuming we have local recovery + Standby recovery:
#We run most experiments with transactions off, to be able to better vizualize the recovery delay
#-------- Start off with local recovery

#Show our version stops less, using nontransactional system
#Get per partition data
#             |CHK INTERVAL|STATESIZE|PARALLELISM|DEPTHOFGRAPH|KILLDEPTH|SLEEPTIME  |WINDOWSIZE     |WINDOWSLIDE  |ACCESSSTATE|STATEFRAGMENTSIZE| JOB| TEST_TYPE | NUM ATTEMPTS | MEASURELAT

#======================= Common failures "Edge usecases"
# One kill every 3 seconds  "Progress under churn"
work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;random_kill;1;true")

##====================== Default cases, small to large graphs
#work_queue+=("$default_cf;$default_ss;1;1;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;3;true")
#
##Default case, Perform this one a few times to measure overhead
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;5;true")
##Largest graph
##Num CPUs: 10 * 10 * 2 = 200
#work_queue+=("$default_cf;$default_ss;10;8;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
#
##===================== Parallelism
#work_queue+=("$default_cf;$default_ss;1;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
##Default (5) already done work_queue[1]="$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;1000000;map"
#work_queue+=("$default_cf;$default_ss;10;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
##work_queue[5]="$default_cf;$default_ss;15;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;map"
##Max. Num CPUs: 20 * 5 * 2 = 200
#work_queue+=("$default_cf;$default_ss;20;3;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
#
##===================== Depth
#work_queue+=("$default_cf;$default_ss;$default_p;1;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
##Default (3) already done work_queue[1]="$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;1000000;map"
##work_queue[8]="$default_cf;$default_ss;$default_p;5;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;map"
#work_queue+=("$default_cf;$default_ss;$default_p;8;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
##Num CPUs: 5 * 20 * 2 = 200, here we want to see what the effect in number of determinants is.
#work_queue+=("$default_cf;$default_ss;$default_p;18;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
#
##===================== State Size
##work_queue+=("$default_cf;100000000;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
#work_queue+=("$default_cf;100000000;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
##work_queue+=("$default_cf;1000000000;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
#work_queue+=("$default_cf;5000000000;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;true")
#
##====================== Checkpoint Interval
#work_queue+=("1000;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
##work_queue+=("1000;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
#work_queue+=("10000;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
##Expect this one to stall due to lack of buffers
#work_queue+=("100000;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;false")
#
##========================
##Processing time windows
##Change window size
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;1;$default_w_slide;$default_as;$default_fs;$windowjob;single_kill;1;false")
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$windowjob;single_kill;1;false")
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$windowjob;random_kill;1;false")
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;5;$default_w_slide;$default_as;$default_fs;$windowjob;single_kill;1;false")
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;10;$default_w_slide;$default_as;$default_fs;$windowjob;single_kill;1;false")
#
##======================= Kill depth
##Kill depth affects the determinant state transfer size. Deeper = more determinants need to be retransfered
#
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;3;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;true")
##Kill source and sink. Only for our solution
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;0;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;true")
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;4;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;$mapjob;single_kill;1;true")
#Sink will not have exactly-once



#======================= Transactions
#work_queue+=("$default_cf;$default_ss;$default_p;$default_d;$default_kd;$default_st;$default_w_size;$default_w_slide;$default_as;$default_fs;map-trans;single_kill;1")




clear_make_topics() {
	local p=$1
	local measure_lat=$2
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

	if [ "$measure_lat" = "false" ]; then
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=30000 --entity-name $input_topic >&2
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=30000 --entity-name $output_topic >&2
	sleep 5
	fi

}

kill_taskmanager() {

    	local jobid=$1
	local path=$2
    	local depth_to_kill=$3
	local par_to_kill=$4
	# Get taskmanagers used by job
	local response=$(curl -sS -X GET "http://$flink_rpc/jobs/$1")
	local vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))
	echo "Vertexes used: ${vertex_ids[@]}"

	local taskmanagers_used=($(for vid in ${vertex_ids[@]} ; do curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host'  |  tr -d '"' | tr ":" " " | awk {'print $1'}  ; done))
	echo "Taskmanagers used: ${taskmanagers_used[@]}"

	# Kill the zeroeth tm of subtask of index to kill
	local taskmanager_to_kill=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$depth_to_kill]}/subtasks/$par_to_kill" | jq '.host' |  tr -d '"')

	local kill_time=$(( $(date '+%s%N') / 1000000))
	kubectl delete pod $taskmanager_to_kill
	echo "Kiled taskmanager $taskmanager_to_kill at $kill_time"
	echo "$taskmanager_to_kill $depth_to_kill $par_to_kill $kill_time" >> $path/killtime
}

push_job() {
    local job=$1
    local response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job-$job.jar" http://$flink_rpc/jars/upload)
    echo "$response" >&2
    local id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)
    echo "$id"
}


measure_sustainable_throughput() {
	jobstr=$1

	IFS=";" read -r -a params <<< "${jobstr}"
	
	local cf="${params[0]}"
	local ss="${params[1]}"
	local p="${params[2]}"
	local d="${params[3]}"
	local kd="${params[4]}"
	local st="${params[5]}"
	local wsize="${params[6]}"
	local wslide="${params[7]}"
	local as="${params[8]}"
	local frag="${params[9]}"
	local job="${params[10]}"
	local jobtype="${params[11]}"
	local attempts="${params[12]}"
	local measure_lat="${params[13]}"



    clear_make_topics $p $measure_lat
    local id=$(push_job $job)
    
    response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\" --bootstrap.servers $kafka_bootstrap --experiment-window-size $wsize --experiment-window-slide $wslide --experiment-state-size $ss --experiment-checkpoint-interval-ms $cf --experiment-state-fragment-size $frag --experiment-access-state $as --experiment-depth $d --experiment-parallelism $p --sleep $st\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
    echo "$response" >&2
    sleep 15

     ./distributed-producer.sh 220 2000000 $kafka_ext $input_topic  $benchmarkers >&2 & 
    sleep 60
    local max1=$(python3 ./throughput-measurer/Main.py 20 1 $kafka_ext $output_topic silent)
    echo "measure 1 $max1" >&2
    local max2=$(python3 ./throughput-measurer/Main.py 20 1 $kafka_ext $output_topic silent)
    echo "measure 2 $max2" >&2
    local max3=$(python3 ./throughput-measurer/Main.py 20 1 $kafka_ext $output_topic silent)
    echo "measure 3 $max3" >&2
    local max4=$(python3 ./throughput-measurer/Main.py 20 1 $kafka_ext $output_topic silent)
    echo "measure 4 $max4" >&2
    local max5=$(python3 ./throughput-measurer/Main.py 20 1 $kafka_ext $output_topic silent)
    echo "measure 5 $max5" >&2
    local max6=$(python3 ./throughput-measurer/Main.py 20 1 $kafka_ext $output_topic silent)
    echo "measure 6 $max6" >&2
    local max7=$(python3 ./throughput-measurer/Main.py 20 1 $kafka_ext $output_topic silent)
    echo "measure 7 $max7" >&2

    local max=$(echo -e "$max1\n$max2\n$max3\n$max4\n$max5\n$max6\n$max7" | sort -n -r | head -n1)
    
    local jobid=$(curl -sS -X GET "http://$flink_rpc/jobs" |  jq  '.jobs[0].id' | tr -d '"')
    curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel" > /dev/null
    sleep 3
    
    kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})  >&2
    sleep 30
    echo "$max"
}

run_experiment() {
	local jobstr=$1
	echo "${jobstr}" >&2
	local setting=$2
	local max=$3
	local path=$4
	IFS=";" read -r -a params <<< "${jobstr}"
	
	local cf="${params[0]}"
	local ss="${params[1]}"
	local p="${params[2]}"
	local d="${params[3]}"
	local kd="${params[4]}"
	local st="${params[5]}"
	local wsize="${params[6]}"
	local wslide="${params[7]}"
	local as="${params[8]}"
	local frag="${params[9]}"
	local job="${params[10]}"
	local jobtype="${params[11]}"
	local attempts="${params[12]}"
	local measure_lat="${params[13]}"

	clear_make_topics $p $measure_lat
	id=$( push_job $job )

	local sustainable=$(echo "$setting * $max" | bc)
	echo "Setting throughput at $sustainable r/s" >&2
	mkdir -p $path/$setting
	echo $sustainable > $path/$setting/sustainable

	response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-window-size $wsize --experiment-window-slide $wslide --experiment-state-size $ss --experiment-state-fragment-size $frag --experiment-access-state $as --experiment-checkpoint-interval-ms $cf --experiment-depth $d --experiment-parallelism $p --sleep $st\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
	echo "Job run: $response"
	sleep 15
	local jobid=$(curl -sS -X GET "http://$flink_rpc/jobs" |  jq  '.jobs[0].id' | tr -d '"')

	response=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid")
	vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))

	local src_pod=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[0]}/subtasks/0" | jq '.host' |  tr -d '"')
	local sink_pod=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$(( $d + 1 ))]}/subtasks/0" | jq '.host' |  tr -d '"')

	. ./distributed-producer.sh 200 $sustainable $kafka_ext $input_topic  $benchmarkers > /dev/null &
	sleep 60
	python3 ./throughput-measurer/Main.py  120 3 $kafka_ext $output_topic verbose > $path/$setting/throughput &
	#. ./latmeasurer.sh $jobid 0.1 1200 $d >> $path/$setting/latmeasured &
	. ./monitor_network.sh 120 $sink_pod >> $path/$setting/network &
	if [ "$jobtype" = "single_kill" ]; then
		sleep 60
		kill_taskmanager $jobid $path/$setting $kd 0
	else
		sleep 50
		for ki in {0..$d}; do
			random_par=$((RANDOM % $p))
			kill_taskmanager $jobid $path/$setting $kd $random_par
			sleep 5
		done
	fi
	local maxin=$(python3 ./throughput-measurer/Main.py  20 1 $kafka_ext $input_topic silent)
	echo "measure input $maxin" >&2
	echo "$maxin" > $path/$setting/input-throughput
	sleep 60

	if [ "$measure_lat" = "true" ]; then
 
		echo "Measuring latency, this may take a while" >&2
		sustrounded=$(echo "${sustainable%.*}")
		python3 ./endtoendlatency/Main.py -k $kafka_ext -i $input_topic -o $output_topic -r $((sustrounded / 10)) -p $p >> $path/$setting/latency
	fi


	echo "Saving logs"
	local jobmanager=$(kubectl get pods | grep jobmanager | awk '{print $1}')
	local new_host=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$kd]}/subtasks/0" | jq '.host' |  tr -d '"')

	echo "Canceling the job with id $jobid"
	curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel"

	kubectl logs $jobmanager > $path/$setting/JOBMANAGER-LOGS
	kubectl logs $new_host > $path/$setting/STANDBY-LOGS
	kubectl logs $src_pod > $path/$setting/SRC-LOGS
	kubectl logs $sink_pod > $path/$setting/SINK-LOGS

	#mkdir -p $path/$setting/logs
	#for pod in $(kubectl get pod | grep flink | awk {'print $1'}) ; do kubectl logs $pod > $path/$setting/logs/$pod ; done

	kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})

}


echo "Beggining experiments"

experiment_number=0
for jobstr in "${work_queue[@]}"
do
	IFS=";" read -r -a params <<< "${jobstr}"
	
	cf="${params[0]}"
	ss="${params[1]}"
	p="${params[2]}"
	d="${params[3]}"
	kd="${params[4]}"
	st="${params[5]}"
	wsize="${params[6]}"
	wslide="${params[7]}"
	as="${params[8]}"
	frag="${params[9]}"
	job="${params[10]}"
	jobtype="${params[11]}"
	attempts="${params[12]}"
	measure_lat="${params[13]}"

	mkdir -p $results/experiment-$experiment_number
	
	echo -e "CI\tSS\tP\tD\tKD\tST\tWSIZE\tWSLIDE\tAS\tFG\tJOB\tKILLTYPE\tATTEMPTS\tMEASURELAT" >> $results/experiment-$experiment_number/configuration
	echo -e "$cf\t$ss\t$p\t$d\t$kd\t$st\t$wsize\t$wslide\t$as\t$frag\t$job\t$jobtype\t$attempts\t$measure_lat" >> $results/experiment-$experiment_number/configuration
	
	for a in $(seq 1 $attempts) ; do #attempt
		path=$results/experiment-$experiment_number/attempt-$a
		mkdir -p $path
		
		echo "Run experiment configuration:"
		echo "   Graph topology: Source - $d operators - Sink"
		echo "   Parallelism degree: $p"
		echo "   State: $ss bytes"
		echo "   Frag Size: $frag bytes"
		echo "   Checkpoint frequency: $cf milliseconds"
		echo "   Kill depth: $kd"
		echo "   Sleep time: $st milliseconds"
		echo "   Access state: $as"
		echo "   Window size: $wsize"
		echo "   Window slide: $wslide"
		echo "   job: $job"
		echo "   jobtype: $jobtype"
		echo "   attempts: $attempts"
		echo "   measure_late: $measure_lat"
		echo "   Input rate: Sustainable"
		
		#max=$( measure_sustainable_throughput "${jobstr}" )
		max=100000
		echo -e "Maximum throughput: $max records/second"
		
		sleep 15
		
		########### Run throughput experiment
		
		echo -e "\n\nRunning setting 0.9"
		
		run_experiment "${jobstr}" 0.9 $max $path 
		 	
		sleep 10
		
		echo -e "\n\nRunning setting 0.5"
		
		########### Run latency experiment
		run_experiment "${jobstr}" 0.5 $max $path 
		
		#for pod in $(kubectl get pod | grep flink | awk {'print $1'}) ; do kubectl logs $pod > $path/logs/$pod ; done
		
		#kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})
		sleep 10
	
	done
	experiment_number=$((experiment_number + 1))
done

