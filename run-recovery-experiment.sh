#!/bin/bash

# sudo add-apt-repository -y ppa:openjdk-r/ppa && sudo apt update && sudo apt install -y openjdk-8-jdk python3 python3-pip bc jq && pip3 install --user confluent_kafka

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
checkpoint_frequencies=(1000 3000)
# Operators between source and sink
graph_depths=(1 2 3 )
# Degrees
parallelism_degrees=(1 2 )

index_to_kill=1

attempts=3

flink_rpc=0.0.0.0:31234
#Needs to be internal kafka loc. kubectl get svc | grep kafka | grep ClusterIP
kafka_bootstrap=$1
kafka_ext=145.100.58.225:31090
zk_loc=0.0.0.0:32715

c=1000
ss=1000000

input_topic=benchmark-input
output_topic=benchmark-output


echo "Beggining experiments"
#Start Benchmark
for p in ${parallelism_degrees[@]}
do
	mkdir -p $results/par-$p
	for g in ${graph_depths[@]}
	do
		mkdir -p $results/par-$p/dep-$g
		for a in $(seq 1 $attempts)
		do
			./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $input_topic --delete > /dev/null

			./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $output_topic --delete > /dev/null



			./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $input_topic --partitions $p --replication-factor 1 > /dev/null

			./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $output_topic --partitions $p --replication-factor 1 > /dev/null


			#echo "Filling kafka topic"
			#. ./distributed-producer.sh 60 5 1600000 $kafka_ext $input_topic 145.100.57.32 145.100.57.33 145.100.57.35
			#echo "Done Filling Kafka Topic"

			sleep 10
			
			echo "Pushing job to Flink"
			response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job-marios.jar" http://$flink_rpc/jars/upload)
			
			id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)
			echo "Jar id: $id"

			echo "Run experiment configuration:"
			echo "   Graph topology: Source - $g operators - Sink"
			echo "   Parallelism degree: $p"
			echo "   State: $ss bytes"
			echo "   Checkpoint frequency: $c milliseconds"
			echo "   Input rate: Pull rate"

			#Run job
			response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-checkpoint-interval-ms $c --experiment-depth $g --experiment-parallelism $p --sleep 0\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
			jobid=$(echo $response | jq '.jobid' |  tr -d '"')

			max=$(python3 ./throughput-measurer/Main.py  5 2 $kafka_ext $output_topic silent)
			sust=$(echo "$max * 0.9" | bc)
			

			#sleep 3
			echo "Starting sustainable distributed producer at $sust r/s. Max measured was: $max r/s"
			sleep 3000000


			echo "Starting measurer for results"
			python3 ./throughput-measurer/Main.py  60 5 $kafka_ext $output_topic verbose > $results/par-$p/dep-$g/attempt-$a-throughput &

			echo "Starting sustainable distributed producer at $sust r/s. Max measured was: $max r/s"
			. ./distributed-producer.sh 60 3 $sust $kafka_ext $input_topic 145.100.57.32 145.100.57.33 145.100.57.35 &
		#	python3 ./throughput-producer/Main.py 60 5 $sust $kafka_ext $input_topic &

			sleep 25

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
			
			echo "Recovery done! Wait 10 sec"
			sleep 10 

			echo "Saving logs"
			jobmanager=$(kubectl get pods | grep jobmanager | awk '{print $1}')
			new_host=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$index_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')

			kubectl logs $jobmanager > $results/par-$p/dep-$g/attempt-$a-JOBMANAGER-LOGS
			kubectl logs $new_host > $results/par-$p/dep-$g/attempt-$a-STANDBY-LOGS
			
			echo "Canceling the job with id $jobid"
			curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel"

			#Read topic to get latency info
			#num_msg_per_part=$(echo "96000000 / $p" | bc)
			#pminusone=$(echo "$p - 1" | bc)
			#for part in $(seq 0 1 $pminusone) ; do
			#	./kafka/bin/kafka-console-consumer.sh --bootstrap-server $kafka_ext --topic $output_topic --partition $part --offset $num_msg_per_part --timeout-ms 200 > $results/par-$p/dep-$g/attempt-$a-latency-part$part
			#done

			kubectl delete pod $(kubectl get pod | grep jobmanager | awk {'print $1'})
			sleep 3
			kubectl delete pod $(kubectl get pod | grep task | awk {'print $1'})

			#output to results file
			#"STATE_SIZE\t\tCHECKPOINT_INTERVAL\t\tDEPTH\t\tPARALLELISM\t\tINPUT_RATE\t\tRECOVERY_TIME"
			time_rec=$(echo "$time_rec_end - $time_rec_start" | bc)
			echo "Recovery time: $time_rec milliseconds"
			echo -e "$ss\t\t$c\t\t$g\t\t$p\t\tpull\t\t$time_rec" >> $results/data
			
			#Allow time for pods to reschedule
			echo "Sleeping before restarting to give time to schedule new pods"
			sleep 15
		done
	done
done
