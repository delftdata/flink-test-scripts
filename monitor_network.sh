#!/bin/bash

function poll() {
	local sleep=$1
	shift 1
	local pods=("$@")

	while true; do
		ts=$(( $(date '+%s%N') / 1000000))
		for pod in "${pods[@]}" ; do
			echo -e "$ts \t $(kubectl exec $pod cat /proc/net/dev | grep eth0 | awk {'print $2 "\t" $10'})" 
		done
		sleep "$sleep"s
	done
}

duration_sec=$1
shift 1
pods=("$@")

sleep=$(echo "scale=2 ; 1 - 0.3 * ${#pods[@]}" | bc)




for pod in "${pods[@]}" ; do 
	echo -e "TIME \t RX \t TX"  
done 

export -f poll
timeout "$duration_sec"s bash -c "poll $sleep $(echo $@)"
