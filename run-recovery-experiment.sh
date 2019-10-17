#!/bin/bash

# sudo apt update
#sudo apt install bc
#sudo apt install jq

#Expects a folder kafka, containing the extracted source of kafka kafka_2.12-0.11.0.3 or kafka_2.11-0.11.0.3
#Expects job.jar the job to benchmark

date=`date +%Y-%m-%d_%H:%M`
results="results-$date"

mkdir -p $results


echo -e "STATE_SIZE\t\tCHECKPOINT_INTERVAL\t\tDEPTH\t\tPARALLELISM\t\tINPUT_RATE\t\tRECOVERY_TIME" > $results/data

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
graph_depths=(1 2 3 )
# Degrees
parallelism_degrees=(1 2)
# MB/second
input_rates=(50 100 200)

index_to_kill=1

attempts=3

flink_rpc=0.0.0.0:31234

#Needs to be internal kafka loc. kubectl get svc | grep kafka | grep ClusterIP
kafka_bootstrap=10.103.65.2:9092

c=1000
ss=1000000000

echo "Filling kafka topic"
#Fill kafka topic
#for i in {1..10} ; do
#	shuf -n5000 /usr/share/dict/cracklib-small | ./kafka/bin/kafka-console-producer.sh --broker-list $kafka_bootstrap --topic benchmark-input >> /dev/null
#done
echo "Done Filling Kafka Topic"

echo "Beggining experiments"
#Start Benchmark
for g in ${graph_depths[@]}
do
	mkdir -p $results/depth-$g
	for p in ${parallelism_degrees[@]}
	do
		mkdir -p $results/depth-$g/par-$p
		for a in $(seq 1 $attempts)
		do
			echo "Pushing job to Flink"
			response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job.jar" http://$flink_rpc/jars/upload)
			
			id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)
			echo "Jar id: $id"

			echo "Run experiment configuration:"
			echo "   Graph topology: Source - $g operators - Sink"
			echo "   Parallelism degree: $p"
			echo "   State: $ss bytes"
			echo "   Checkpoint frequency: $c milliseconds"
			echo "   Input rate: Pull rate"

			#Run job
			response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-checkpoint-interval-ms $c --experiment-depth $g --experiment-parallelism $p\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
			jobid=$(echo $response | jq '.jobid' |  tr -d '"')
			
			echo "Job submitted!"

			sleep 40

			
			#all_taskmanagers=$(kubectl get pods | grep taskmanager | awk '{print $1}')

			# Get taskmanagers used by job
			response=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid")
			vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))
			echo "Vertexes used: ${vertex_ids[@]}"
			
			taskmanagers_used=($(for vid in ${vertex_ids[@]} ; do curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host'  |  tr -d '"' | tr ":" " " | awk {'print $1'}  ; done))
			echo "Taskmanagers used: ${taskmanagers_used[@]}"
			
			# Kill the zeroeth tm of subtask of index to kill
			taskmanager_to_kill=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$index_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')

			# Compute tms not used
			#standby_taskmanagers=("$all_taskmanagers[@]}")
			#for tm in ${taskmanagers_used[@]}; do
			#	standby_taskmanagers=("${standby_taskmanagers[@]/$tm}")
			#done
			#echo "Taskmanagers standby: ${standby_taskmanagers[@]}"
			
			echo "Killing taskmanager $taskmanager_to_kill"
			kubectl delete pod $taskmanager_to_kill
			time_rec_start=$(date +%s%3N)
			echo "Time rec start: $time_rec_start"
		
			echo "Waiting for recovery to complete"
			finished=false
			while [ "$finished" = false  ] ; do
				attempt=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$index_to_kill]}/subtasks/0" | jq '.attempt')
				if [ "$attempt" -ge 1 ] ; then
					echo "Done"
					finished=true

				fi
				sleep 0.1
			done
			time_rec_end=$(date +%s%3N)
			

			
			sleep 5
			
			#save logs
			jobmanager=$(kubectl get pods | grep jobmanager | awk '{print $1}')
			new_host=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$index_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')

			echo "Saving logs"
			kubectl logs $jobmanager > $results/depth-$g/par-$p/attempt-$a-JOBMANAGER-LOGS
			kubectl logs $new_host > $results/depth-$g/par-$p/attempt-$a-STANDBY-LOGS
			
			## Cancel job
			echo "Canceling the job with id $jobid"
			curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel"

			kubectl delete pod $(kubectl get pod | grep jobmanager | awk {'print $1'})
			sleep 3
			kubectl delete pod $(kubectl get pod | grep task | awk {'print $1'})

			#output to results file
			#"STATE_SIZE\t\tCHECKPOINT_INTERVAL\t\tDEPTH\t\tPARALLELISM\t\tINPUT_RATE\t\tRECOVERY_TIME"
			time_rec=$(echo "$time_rec_end - $time_rec_start" | bc)
			echo "Recovery time: $time_rec milliseconds"
			echo -e "$ss\t\t$c\t\t$g\t\t$p\t\tpull\t\t$time_rec" >> $results/data
			
			#Allow time for pods to reschedule
			sleep 30
		done
	done
done
