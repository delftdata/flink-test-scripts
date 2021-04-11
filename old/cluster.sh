#!/bin/bash

TASKMANAGERS=$2

parentdir=..

if [ $# -ne 2 ]; then
	echo "Wrong syntax. Expected: cluster-tm.sh (start | stop) <number-of-taskmanagers>."
	exit 1
fi

if [[ "$1" == "start" ]]; then
	$parentdir/flink/build-target/bin/start-cluster.sh
	for ((i=1; i<TASKMANAGERS; i++)); do
		$parentdir/flink/build-target/bin/taskmanager.sh start
	done
else
	$parentdir/flink/build-target/bin/stop-cluster.sh
	for ((i=1; i<TASKMANAGERS; i++)); do
		$parentdir/flink/build-target/bin/taskmanager.sh stop
	done
fi
