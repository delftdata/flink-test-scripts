#!/bin/bash

# Experiment dimensions
declare -a state_sizes
declare -a checkpoint_frequencies
declare -a graph_depths
declare -a parallelism_degrees
declare -a input_rates

# MB
state_sizes=(10 100 1000)
# Seconds
checkpoint_frequencies=(0.5 1 3)
# Operators between source and sink
graph_depths=(1 2 5)
# Degrees
parallelism_degrees=(2 5 10)
# MB/second
input_rates=(50 100 200)

for g in ${graph_depths[@]}
do
	for p in ${parallelism_degrees[@]}
	do
		echo "Run experiment configuration:"
		echo "   Graph topology: Source - $g operators - Sink"
		echo "   Parallelism degree: $p"
		echo "   State: 100MB"
		echo "   Checkpoint frequency: 1 second"
		echo "   Input rate: 50 MBs/second"

		# for pod in `kubectl get pods | grep taskmanager | awk ' {print $1}'`
		# do
			## Post jar 
			# kubectl exec $pod command
		# done

		## Let the job run for some seconds
		# sleep X
		## Kill task
		## Monitor recovery
		## Stop the clock when recovery is complete
		## Cancel job
	done
done
