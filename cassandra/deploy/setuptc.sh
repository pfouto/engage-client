#!/bin/bash

if [[ -z "$2" ]]; then
  echo "Usage: setup_delay.sh [nServers] [latency_file]"
  exit
fi

RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

n_Servers=$1
latency_file=$2
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

echo Setting up TC...

cmd1="sudo-g5k sudo ip link set eno2 down"
for ((i = 0; i < $(($n_Servers)); i++)); do
  echo "${server_nodes[$i]} -- $cmd1"
  oarsh "${server_nodes[$i]}" "$cmd1" &
done
wait

for ((i = 0; i < $(($n_Servers)); i++)); do
  cmd="sudo-g5k tc qdisc del dev br0 root"
  echo "${server_nodes[$i]} -- $cmd"
  oarsh "${server_nodes[$i]}" "$cmd" &
done
wait

i=0

while read -r line; do
  echo -e "${RED}${server_nodes[$i]}${NC}"
  cmd="sudo-g5k tc qdisc add dev br0 root handle 1: htb"
  echo "---------------- ${server_nodes[$i]} -- $cmd"
  oarsh -n "${server_nodes[$i]}" "$cmd"
  j=0
  for n in $line; do
    if [ $i -eq $j ]; then
      j=$((j + 1))
      continue
    fi

    target_ip=$(getent hosts ${server_nodes[$j]} | awk '{print $1}')
    echo -e "latency from ${GREEN}${server_nodes[$i]}${NC} to ${BLUE}${server_nodes[$j]}${NC} ($target_ip) is ${RED}${n}${NC}"

    cmd1="sudo-g5k tc class add dev br0 parent 1: classid 1:$(($j + 1))1 htb rate 1000mbit"
    cmd2="sudo-g5k tc qdisc add dev br0 parent 1:$(($j + 1))1 handle $(($j + 1))10: netem delay ${n}ms $((n * 3 / 100))ms distribution normal"
    cmd3="sudo-g5k tc filter add dev br0 protocol ip parent 1:0 prio 1 u32 match ip dst $target_ip flowid 1:$(($j + 1))1"

    echo "-- ${server_nodes[$i]} -- $cmd1"
    echo "-- ${server_nodes[$i]} -- $cmd2"
    echo "-- ${server_nodes[$i]} -- $cmd3"

    oarsh -n "${server_nodes[$i]}" "$cmd1 && $cmd2 && $cmd3"

    j=$((j + 1))
  done
  i=$((i + 1))
done <"$latency_file"
