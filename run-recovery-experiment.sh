#!/bin/bash

#Expects a folder kafka, containing the extracted source of kafka kafka_2.12-0.11.0.3 or kafka_2.11-0.11.0.3
#Expects job.jar the job to benchmark

date=`date +%Y-%m-%d_%H:%M`
results="results-$date"

echo "STATE_SIZE\t\tCHECKPOINT_INTERVAL\t\tDEPTH\t\tPARALLELISM\t\tINPUT_RATE\t\tRECOVERY_TIME"

# Experiment dimensions
declare -a state_sizes
declare -a checkpoint_frequencies
declare -a graph_depths
declare -a parallelism_degrees
declare -a input_rates

# MB
state_sizes=(10 100 1000)
# Milliseconds
checkpoint_frequencies=(500 1000 3000)
# Operators between source and sink
graph_depths=(1 2 5)
# Degrees
parallelism_degrees=(2 5 10)
# MB/second
input_rates=(50 100 200)

index_to_kill=1

flink_rpc=145.100.58.163:31234
kafka_bootstrap=145.100.58.163:31090

response=$(curl -X POST -H "Expect:" -F "jarfile=@job.jar" http://$flink_rpc/jars/upload)

id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)

c=1000
ss=10000000

#Fill kafka topic
for i in {1..100} ; do
	shuf -n5000 /usr/share/dict/cracklib-small | ./kafka/bin/kafka-console-producer.sh --broker-list $kafka_bootstrap --topic benchmark-input
done


#Start Benchmark
for g in ${graph_depths[@]}
do
	for p in ${parallelism_degrees[@]}
	do
		echo "Run experiment configuration:"
		echo "   Graph topology: Source - $g operators - Sink"
		echo "   Parallelism degree: $p"
		echo "   State: $ss bytes"
		echo "   Checkpoint frequency: $c milliseconds"
		echo "   Input rate: Pull rate"

		#Run job
		response=$(curl -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-checkpoint-interval-ms $c --experiment-depth $g --experiment-parallelism $p\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
		jobid=$(echo $response | jq '.jobid' |  tr -d '"')
		
		sleep 1s

		all_taskmanagers=$(kubectl get pods | grep taskmanager | awk '{print $1}')

		# Get taskmanagers used by job
		response=$(curl -X GET http://$flink_rpc/jobs/$jobid)
		vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))
		declare -a taskmanagers_used
		for vid in ${vertex_ids[@]} ; do
			taskmanagers_used+=("$(curl -X GET http://$flink_rpc/jobs/$jobid/vertices/$vid/taskmanagers | jq '.taskmanagers[0].host'  |  tr -d '"' )")
		done
		echo "Taskmanagers used: ${taskmanagers_used[@]}"

		sleep 5s

		taskmanager_to_kill=${taskmanagers_used[$index_to_kill]}

		# Compute tms not used
		standby_taskmanagers=("$all_taskmanagers[@]}")
		for tm in ${taskmanagers_used[@]}; do
			standby_taskmanagers=("standby_taskmanagers[@]/$tm")
		done
		echo "Taskmanagers standby: ${standby_taskmanagers[@]}"

		kubectl delete pod $taskmanager_to_kill
		time_rec_start=$(date +%s%3N)

		done=false
		while [ $done != true  ] ; do
			for pod in ${standby_taskmanagers[@]}; do
				#There should be only one log file
				nlines=$(kubectl logs $pod | wc -l)
				
				if [ "$nlines" -gt "105" ] ; then
					done=true
					time_rec_end=$(date +%s%3N)
					break
				fi
			done
		done
		
		## Cancel job
		curl -X PATCH http://$flink_rpc/jobs/$jobid?mode=cancel

		kubectl delete pod $(kubectl get pod | grep task | awk {'print $1'})

		#output to results file
		#"STATE_SIZE\t\tCHECKPOINT_INTERVAL\t\tDEPTH\t\tPARALLELISM\t\tINPUT_RATE\t\tRECOVERY_TIME"
		time_rec=$(echo "$time_rec_end - $time_rec_start" | bc)
		echo "Recovery time: $time_rec milliseconds"
		echo "$ss\t\t$c\t\t$g\t\t$p\t\tpull\t\t$time_rec" >> $results
		
		#Allow time for pods to reschedule
		sleep 10s

	done
done
