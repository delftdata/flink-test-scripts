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
state_sizes=( 100 )
# Milliseconds
checkpoint_frequencies=(1000)
# Operators between source and sink
graph_depths=( 1 )
# Degrees
parallelism_degrees=( 20 )

index_to_kill=1

attempts=1

flink_rpc=0.0.0.0:31234
#Needs to be internal kafka loc. kubectl get svc | grep kafka | grep ClusterIP
kafka_bootstrap=$(kubectl get svc | grep ClusterIP | grep kafka | grep -v headless | awk {'print $3'}):9092
node_external_ip=$(ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)
kafka_ext=$node_external_ip:31090
zk_port=$(kubectl get svc | grep  zookeeper | grep NodePort | awk '{print $5}' | tr ":" "\n" | tr "/" "\n" | head -n 2 | tail -n 1)
zk_loc=0.0.0.0:$zk_port

benchmarkers=$@

frag_size=100

input_topic=benchmark-input
output_topic=benchmark-output


clear_make_topics() {
	echo "Trying to clear topic"

	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=1000 --entity-name $input_topic
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --add-config retention.ms=1000 --entity-name $output_topic

	sleep 5

	echo "Remove deletion mechanism"

	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --delete-config retention.ms --entity-name $input_topic
	./kafka/bin/kafka-configs.sh --zookeeper $zk_loc --alter --entity-type topics --delete-config retention.ms --entity-name $output_topic

	sleep 5
	echo "Now delete topic"

	./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $input_topic --delete 

	./kafka/bin/kafka-topics.sh --zookeeper $zk_loc --topic $output_topic --delete 

	sleep 10
	echo "Create topic"


	./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $input_topic --partitions $p --replication-factor 1

	./kafka/bin/kafka-topics.sh --create --zookeeper $zk_loc  --topic $output_topic --partitions $p --replication-factor 1 
	echo "Done creating"

	sleep 5

}

kill_taskmanager() {

	# Get taskmanagers used by job
	response=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$1")
	vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))
	echo "Vertexes used: ${vertex_ids[@]}"

	taskmanagers_used=($(for vid in ${vertex_ids[@]} ; do curl -sS -X GET "http://0.0.0.0:31234/jobs/$1/vertices/$vid/taskmanagers" | jq '.taskmanagers[] | .host'  |  tr -d '"' | tr ":" " " | awk {'print $1'}  ; done))
	echo "Taskmanagers used: ${taskmanagers_used[@]}"

	# Kill the zeroeth tm of subtask of index to kill
	taskmanager_to_kill=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$1/vertices/${vertex_ids[$index_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')

	kill_time=$(( $(date '+%s%N') / 1000000))
	echo $kill_time > $2/killtime
	echo "Killing taskmanager $taskmanager_to_kill at $kill_time"
	kubectl delete pod $taskmanager_to_kill

}


echo "Beggining experiments"
#Start Benchmark
for cf in ${checkpoint_frequencies[@]} ; do
    mkdir -p $results/cf-$cf
    for ss in ${state_sizes[@]} ; do
        mkdir -p $results/cf-$cf/ss-$ss
		for p in ${parallelism_degrees[@]} ; do
		    mkdir -p $results/cf-$cf/ss-$ss/par-$p
			for g in ${graph_depths[@]} ; do
		        mkdir -p $results/cf-$cf/ss-$ss/par-$p/dep-$g
				for a in $(seq 1 $attempts) ; do
		            mkdir -p $results/cf-$cf/ss-$ss/par-$p/dep-$g/attempt-$a
				    path=$results/cf-$cf/ss-$ss/par-$p/dep-$g/attempt-$a
					clear_make_topics

					echo "Run experiment configuration:"
					echo "   Graph topology: Source - $g operators - Sink"
					echo "   Parallelism degree: $p"
					echo "   State: $ss bytes"
					echo "   Checkpoint frequency: $c milliseconds"
					echo "   Input rate: Pull rate"


					echo "Pushing job to Flink"
					response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job-marios.jar" http://$flink_rpc/jars/upload)
					echo "Job submit: $response"
					id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)
					sleep 1

					########## Find Sustainable
					response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-checkpoint-interval-ms $cf --experiment-state-fragment-size $frag_size --experiment-depth $g --experiment-parallelism $p --sleep 0\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
					echo "Job run: $response"
					 ./distributed-producer.sh 120 5 2000000 $kafka_ext $input_topic  $benchmarkers &
					sleep 50
					max1=$(python3 ./throughput-measurer/Main.py  20 2 $kafka_ext $output_topic silent)
					echo "Max1 at $max1 r/s"
					max2=$(python3 ./throughput-measurer/Main.py  20 2 $kafka_ext $output_topic silent)
					echo "Max2 at $max2 r/s"
					max3=$(python3 ./throughput-measurer/Main.py  20 2 $kafka_ext $output_topic silent)
					echo "Max3 at $max3 r/s"
					max=$(echo -e "$max1\n$max2\n$max3" | sort -n -r | head -n1)
					echo "Max at $max r/s"

					jobid=$(curl -sS -X GET "http://0.0.0.0:31234/jobs" |  jq  '.jobs[0].id' | tr -d '"')
					echo "Canceling the job with id $jobid"
					curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel"

					kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})
					sleep 15


					########## Run throughput experiment
					clear_make_topics

					echo "Pushing job to Flink"
					response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job-marios.jar" http://$flink_rpc/jars/upload)
					echo "Job submit: $response"
					id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)
					sleep 1

					setting=0.9
					sustainable=$(echo "$setting * $max" | bc)
					echo "Setting throughput at $sustainable r/s"
					mkdir -p $path/$setting
					echo $sustainable > $path/$setting/sustainable

					response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-state-fragment-size $frag_size --experiment-checkpoint-interval-ms $cf --experiment-depth $g --experiment-parallelism $p --sleep 0\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
					echo "Job run: $response"
					sleep 20 #Make sure even large jobs get scheduled
					jobid=$(curl -sS -X GET "http://0.0.0.0:31234/jobs" |  jq  '.jobs[0].id' | tr -d '"')

					. ./distributed-producer.sh 200 5 $sustainable $kafka_ext $input_topic  $benchmarkers &
					sleep 45
					python3 ./throughput-measurer/Main.py  150 3 $kafka_ext $output_topic verbose > $path/$setting/throughput &
				    	. ./latmeasurer.sh $jobid 0.1 1500 $g >> $path/$setting/latmeasured &

					sleep 50
					kill_taskmanager $jobid $path/$setting
					sleep 115

					response=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid")
					vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))

					echo "Saving logs"
					jobmanager=$(kubectl get pods | grep jobmanager | awk '{print $1}')
					new_host=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$index_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')
					src_pod=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[0]}/subtasks/0" | jq '.host' |  tr -d '"')
					sink_pod=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$(( $g + 1 ))]}/subtasks/0" | jq '.host' |  tr -d '"')

					echo "Canceling the job with id $jobid"
					curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel"

					kubectl logs $jobmanager > $path/$setting/JOBMANAGER-LOGS
					kubectl logs $new_host > $path/$setting/STANDBY-LOGS
					kubectl logs $src_pod > $path/$setting/SRC-LOGS
					kubectl logs $sink_pod > $path/$setting/SINK-LOGS

					mkdir -p $path/$setting/logs
					for pod in $(kubectl get pod | grep flink | awk {'print $1'}) ; do kubectl logs $pod > $path/$setting/logs/$pod ; done

					kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})
					


					########## Run latency experiment
					clear_make_topics

					echo "Pushing job to Flink"
					response=$(curl -sS -X POST -H "Expect:" -F "jarfile=@job-marios.jar" http://$flink_rpc/jars/upload)
					echo "Job submit: $response"
					id=$(echo $response | jq '.filename' |  tr -d '"' | tr "/" "\n" | tail -n1)
					sleep 1

					setting=0.5
					sustainable=$(echo "$setting * $max" | bc)
					echo "Setting throughput at $sustainable r/s"
					mkdir -p $path/$setting
					echo $sustainable > $path/$setting/sustainable

					response=$(curl -sS  -X POST --header "Content-Type: application/json;charset=UTF-8" --data "{\"programArgs\":\"--bootstrap.servers $kafka_bootstrap --experiment-state-size $ss --experiment-state-fragment-size $frag_size --experiment-checkpoint-interval-ms $cf --experiment-depth $g --experiment-parallelism $p --sleep 0\"}" "http://$flink_rpc/jars/$id/run?allowNonRestoredState=false")
					echo "Job run: $response"
					sleep 20 #Make sure even large jobs get scheduled
					jobid=$(curl -sS -X GET "http://0.0.0.0:31234/jobs" |  jq  '.jobs[0].id' | tr -d '"')

					. ./distributed-producer.sh 200 5 $sustainable $kafka_ext $input_topic  $benchmarkers &
					sleep 45
					python3 ./throughput-measurer/Main.py  150 3 $kafka_ext $output_topic verbose > $path/$setting/throughput &
				    	. ./latmeasurer.sh $jobid 0.1 1500 $g >> $path/$setting/latmeasured &

					sleep 50
					kill_taskmanager $jobid $path/$setting
					sleep 115

					response=$(curl -sS -X GET "http://0.0.0.0:31234/jobs/$jobid")
					vertex_ids=($(echo $response | jq '.vertices[] | .id'  |  tr -d '"'))

					echo "Saving logs"
					jobmanager=$(kubectl get pods | grep jobmanager | awk '{print $1}')
					new_host=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$index_to_kill]}/subtasks/0" | jq '.host' |  tr -d '"')
					src_pod=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[0]}/subtasks/0" | jq '.host' |  tr -d '"')
					sink_pod=$(curl -sS -X GET "http://$flink_rpc/jobs/$jobid/vertices/${vertex_ids[$(( $g + 1 ))]}/subtasks/0" | jq '.host' |  tr -d '"')

					echo "Canceling the job with id $jobid"
					curl -sS -X PATCH "http://$flink_rpc/jobs/$jobid?mode=cancel"

					kubectl logs $jobmanager > $path/$setting/JOBMANAGER-LOGS
					kubectl logs $new_host > $path/$setting/STANDBY-LOGS
					kubectl logs $src_pod > $path/$setting/SRC-LOGS
					kubectl logs $sink_pod > $path/$setting/SINK-LOGS

					mkdir -p $path/$setting/logs
					for pod in $(kubectl get pod | grep flink | awk {'print $1'}) ; do kubectl logs $pod > $path/$setting/logs/$pod ; done
					kubectl delete pod $(kubectl get pod | grep flink | awk {'print $1'})

					
					#Allow time for pods to reschedule
					echo "Sleeping before restarting to give time to schedule new pods"
					sleep 5

				done
			done
		done
	done
done
