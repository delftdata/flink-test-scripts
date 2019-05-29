#!/bin/bash

i=1

parentdir=..

while true
do
	echo "== Auto test run no $i. =="
	echo ""

	./cluster.sh start 4
	sleep 2

	echo ""
	#echo "Starting job WindowJoin parallel=2."
	#echo "Starting job StateMachine parallel=2."
	echo "Starting job TopSpeedWindowing parallel=2."
	#echo "Starting job IncrementalLearning parallel=2."

	# WindowJoin
	#nohup $dir/flink/build-target/bin/flink run -p 2 $dir/flink/build-target/examples/streaming/WindowJoin.jar --windowSize 10000 --rate 100 >$dir/flink-job.out 2>$dir/flink-job.err & 
	# StateMachine
	#nohup $dir/flink/build-target/bin/flink run -p 2 $dir/flink/build-target/examples/streaming/StateMachineExample.jar --error-rate 0.2 --sleep 100 >$dir/flink-job.out 2>$dir/flink-job.err &
	# TopSpeedWindowing
	nohup $parentdir/flink/build-target/bin/flink run -p 2 $parentdir/flink/build-target/examples/streaming/TopSpeedWindowing.jar >$parentdir/flink-job.out 2>$parentdir/flink-job.err &
	# IncrementalLearning
	#nohup $dir/flink/build-target/bin/flink run -p 2 $dir/flink/build-target/examples/streaming/IncrementalLearning.jar >$dir/flink-job.out 2>$dir/flink-job.err &

	sleep 2
	jobid=`$parentdir/flink/build-target/bin/flink list | grep -E -o ": ([a-z0-9]+) :" | grep -E -o "([a-z0-9]+)"`
	echo "Job's job id is $jobid."

	echo ""
	echo "=========="
	echo "Find and kill taskmanager producing output."
	./find-kill-taskmanager.sh
	exitstatus=$?
	echo "Find and kill taskmanager command returned $exitstatus."

	echo ""
	echo "=========="
	echo "Present output of alive taskmanagers."
	./present-output-taskmanager.sh

	echo ""
	echo "=========="
	echo "Present failed tasks."
	./present-failed-tasks.sh

	echo ""
	echo "=========="
	$parentdir/flink/build-target/bin/flink cancel $jobid
	./cluster.sh stop 4

	echo ""
	if [ "$exitstatus" -eq "1" ]
	then
		echo "Flink job with $jobid crashed after introduced failure. Exit."
		exit 0
	fi

	((i++))
	sleep 3
done
