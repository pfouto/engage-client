#!/bin/bash

if [[ -z "$1" ]]; then
  echo "Usage: config_nodes.sh [nServers]"
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

for (( i=0; i<$n_Servers; i++ ))
do
  oarsh "${server_nodes[$i]}" "sudo-g5k apt -y install libjemalloc-dev" &
done
wait
echo "DONE installing libjemalloc"

for (( i=0; i<$n_Servers; i++ ))
do
  oarsh "${server_nodes[$i]}" "sudo-g5k swapoff -a && sudo-g5k sysctl -w vm.max_map_count=1048575" &
done
wait
echo "DONE disabling swap && setting max_map count"
