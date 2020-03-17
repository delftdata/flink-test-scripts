#!/bin/bash


parentdir=..

jps |
grep -E -o "([0-9]+) StandaloneSessionClusterEntrypoint" |
grep -E -o "^[0-9]+" |
# Get the pid of jobmanager.
while read -r pid; do
	logfilename=`ps aux | \
		grep $pid | \
		grep -E -o 'standalonesession.*\.log'`
	grep -E -o "\- .*RUNNING to FAILED\.$" $parentdir/flink/build-target/log/flink-flink-${logfilename}
done
