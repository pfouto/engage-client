#!/bin/bash

if [[ -z "$1" ]]; then
  echo "Usage: setup_delay.sh [nServers]"
  exit
fi

RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

n_Servers=$1
all_nodes=$(./nodes.sh)

declare -a server_nodes

idx=0
for i in $all_nodes; do
  if [ $idx -eq "$n_Servers" ]; then
    break
  fi
  server_nodes+=($i)
  idx=$((idx + 1))
done

if [ $idx -lt "$n_Servers" ]; then
  echo -e "${RED}Not enough nodes: Got $idx, required ${n_Servers}$NC"
  exit
fi

echo -e "${GREEN}Servers: $NC${server_nodes[*]}"

declare -A latency_map

latency_map[0, 0]=-1
latency_map[0, 1]=46
latency_map[0, 2]=63
latency_map[0, 3]=102
latency_map[0, 4]=93
latency_map[1, 0]=44
latency_map[1, 1]=-1
latency_map[1, 2]=105
latency_map[1, 3]=144
latency_map[1, 4]=139
latency_map[2, 0]=61
latency_map[2, 1]=103
latency_map[2, 2]=-1
latency_map[2, 3]=169
latency_map[2, 4]=179
latency_map[3, 0]=105
latency_map[3, 1]=146
latency_map[3, 2]=162
latency_map[3, 3]=-1
latency_map[3, 4]=80
latency_map[4, 0]=94
latency_map[4, 1]=143
latency_map[4, 2]=154
latency_map[4, 3]=78
latency_map[4, 4]=-1

echo Setting up TC...

cmd1="sudo-g5k sudo ip link set eno2 down"

for ((i = 0; i < $(($n_Servers)); i++)); do
  echo "${server_nodes[$i]} -- $cmd1"
  oarsh "${server_nodes[$i]}" "$cmd1"
done

wait
sleep 5

for ((i = 0; i < $(($n_Servers)); i++)); do
  cmd="sudo-g5k tc qdisc del dev br0 root"
  echo "${server_nodes[$i]} -- $cmd"
  oarsh "${server_nodes[$i]}" "$cmd" &
done
wait

for ((i = 0; i < $(($n_Servers)); i++)); do
  cmd="sudo-g5k tc qdisc add dev br0 root handle 1: htb"
  echo "---------------- ${server_nodes[$i]} -- $cmd"
  oarsh "${server_nodes[$i]}" "$cmd"
  for ((j = 0; j < $n_Servers; j++)); do
    if [ $i -eq $j ]; then
      continue
    fi

    targetip=$(getent hosts ${server_nodes[$j]} | awk '{print $1}')
    #echo "latency from ${server_nodes[$i]} to ${server_nodes[$j]} ($targetip) is ${latency_map[$i, $j]}"
    echo "latency from ${server_nodes[$i]} to ${server_nodes[$j]} ($targetip) is 50"
    cmd="sudo-g5k tc class add dev br0 parent 1: classid 1:$(($j + 1))1 htb rate 1000mbit"
    echo "-- ${server_nodes[$i]} -- $cmd"
    oarsh "${server_nodes[$i]}" "$cmd"

    #cmd="sudo-g5k tc qdisc add dev br0 parent 1:$(($j + 1))1 handle $(($j + 1))10: netem delay ${latency_map[$i, $j]}ms $((${latency_map[$i, $j]} * 3 / 100))ms distribution normal"
    cmd="sudo-g5k tc qdisc add dev br0 parent 1:$(($j + 1))1 handle $(($j + 1))10: netem delay 50ms $((50 * 3 / 100))ms distribution normal"
    echo "-- ${server_nodes[$i]} -- $cmd"
    oarsh "${server_nodes[$i]}" "$cmd"

    cmd="sudo-g5k tc filter add dev br0 protocol ip parent 1:0 prio 1 u32 match ip dst $targetip flowid 1:$(($j + 1))1"
    echo "-- ${server_nodes[$i]} -- $cmd"
    oarsh "${server_nodes[$i]}" "$cmd"
  done
done
