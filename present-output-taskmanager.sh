#!/bin/bash


parentdir=..

jps |
grep -E -o "([0-9]+) TaskManagerRunner" |
grep -E -o "^[0-9]+" |
# Get the pid of each taskmanager.
while read -r pid; do
	logfilename=`ps aux | \
		grep $pid | \
		grep -E -o 'taskexecutor.*\.log'`
	filename=`basename $logfilename .log`
	outfilename=${filename}.out
	wc=`wc -l $parentdir/flink/build-target/log/flink-flink-${outfilename} | \
			grep -E -o "^([0-9]+)"`

	if [ "$wc" -gt "31" ]
	then
		echo ""
		echo "First five lines of ${outfilename} are:"
		head -n 36 $parentdir/flink/build-target/log/flink-flink-${outfilename} | \
			tail -n 5
	fi
done
