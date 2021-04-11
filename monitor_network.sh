#!/bin/bash

function poll() {
	local sleep=$1
	local vertex=$2
	local local_experiment=$3

	while true; do
		ts=$(( $(date '+%s%N') / 1000000))
		if [ "$local_experiment" = "false" ]; then
		  echo -e "$ts\t$(kubectl exec $pod cat /proc/net/dev | grep eth0 | awk {'print $2 "\t" $10'})"
		else
		  echo -e "$ts\t$(docker exec $pod cat /proc/net/dev | grep eth0 | awk {'print $2 "\t" $10'})"
		fi
		sleep "$sleep"s
	done
}

duration_sec=$1
vertex=$2
local_experiment=$3
sleep="0.5"

echo -e "TIME\tRX\tTX"

export -f poll
timeout "$duration_sec"s bash -c "poll $sleep $vertex $local_experiment"
