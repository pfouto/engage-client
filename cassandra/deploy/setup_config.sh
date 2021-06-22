#!/bin/bash

if [[ -z "$2" ]]; then
  echo "Usage: setup_config.sh [nServers] [conf_file]"
  exit
fi

RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

n_Servers=$1
conf_file=$2

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
  sed -i "s/\"node-$((i+1))\"/\"${server_nodes[$i]}\"/g" "$conf_file"
done
