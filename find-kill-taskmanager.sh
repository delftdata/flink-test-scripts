#!/bin/bash


parentdir=..

# Check the job's state after killing the taskmanager. Works only for one job.
function check_state() {
	stateprevious=""
	for i in {1..10}
	do
		state=`$parentdir/flink/build-target/bin/flink list | \
			grep -E -o "\([A-Z]+\)$" | \
			grep -E -o "[A-Z]+"`
		if [ ! -z "$state" ] && [ "$state" != "RUNNING" ]
		then
			echo "Job has state $state (other than RUNNING). Exit."
			exit 1
		elif [ -z "$state" ] && [ "$stateprevious" == "RUNNING" ]
		then
			echo "Job is not running anymore."
			exit 1
		elif [ -z "$state" ]
		then
			echo "No job is running. Probably crashed."
			exit 1
		else
			echo "Job is running."
		fi

		stateprevious=$state

		sleep 1
	done
}

jps |
grep -E -o "([0-9]+) TaskManagerRunner" |
grep -E -o "^[0-9]+" |
# Get the pid of each taskmanager.
while read -r pid; do
	logfilename=`ps aux | \
		grep $pid | \
		grep -E -o 'taskexecutor.*\.log'`
	wc=0
	# Check if the taskmanager's output file grows.
	for i in {1..10}
	do
		wcprevious=$wc
		filename=`basename $logfilename .log`
		outfilename=${filename}.out
		wc=`wc -l $parentdir/flink/build-target/log/flink-flink-${outfilename} | \
			grep -E -o "^([0-9]+)"`
		echo "Length of ${outfilename} is $wc lines (was $wcprevious)."

		if [ "$wcprevious" -ne "0" ] && [ "$wc" -gt "$wcprevious" ]
		then
			echo "$outfilename with pid $pid produces output."
			triggerfailure="true"
			# Kill taskmanager if its output file grows, i.e. it's producing output.
			echo "Send terminate signal to pid $pid."
			kill -s 15 $pid
			break
		fi

		sleep 1
	done

	echo ""
	if [ "$triggerfailure" == "true" ]
	then
		check_state
		break
	fi
done
