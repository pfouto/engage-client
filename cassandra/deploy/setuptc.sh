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

print_and_exec() {
  node=$1
  cmd=$2
  echo "-- node -- $cmd"
  oarsh -n "$node" "$cmd"
}

for ((i = 0; i < $(($n_Servers)); i++)); do
  print_and_exec "${server_nodes[$i]}" "sudo-g5k tc qdisc del dev br0 root" &
done
wait

i=0
while read -r line; do
  echo -e "${RED}${server_nodes[$i]}${NC}"

  print_and_exec "${server_nodes[$i]}" "sudo-g5k tc qdisc add dev br0 root handle 1: htb"

  j=0
  for n in $line; do
    if [ $i -ne $j ]; then

      target_ip=$(getent hosts "${server_nodes[$j]}" | awk '{print $1}')
      echo -e "latency from ${GREEN}${server_nodes[$i]}${NC} to ${BLUE}${server_nodes[$j]}${NC} ($target_ip) is ${RED}${n}${NC}"

      cmd="sudo-g5k tc class add dev br0 parent 1: classid 1:$(($j + 1))1 htb rate 1000mbit && \
          sudo-g5k tc qdisc add dev br0 parent 1:$(($j + 1))1 handle $(($j + 1))10: netem delay ${n}ms $((n/20))ms distribution normal && \
          sudo-g5k tc filter add dev br0 protocol ip parent 1:0 prio 1 u32 match ip dst $target_ip flowid 1:$(($j + 1))1"
      print_and_exec "${server_nodes[$i]}" "$cmd"

    fi
    j=$((j + 1))
  done
  i=$((i + 1))
done <"$latency_file"

#sudo tc qdisc del dev enp1s0 root
#sudo tc qdisc add dev enp1s0 root handle 1: htb
#sudo tc class add dev enp1s0 parent 1: classid 1:1 htb rate 5000mbit
#sudo tc class add dev enp1s0 parent 1:1 classid 1:10 htb ceil 5000mbit rate 1mbit
#sudo tc class add dev enp1s0 parent 1:1 classid 1:11 htb ceil 5000mbit rate 1mbit
#sudo tc class add dev enp1s0 parent 1:1 classid 1:12 htb ceil 5000mbit rate 1mbit
#sudo tc filter add dev enp1s0 parent 1:0 prio 1 u32 match ip dst 192.168.122.77 flowid 1:10
#sudo tc filter add dev enp1s0 parent 1:0 prio 1 u32 match ip dst 192.168.122.114 flowid 1:11
#sudo tc filter add dev enp1s0 parent 1:0 prio 1 u32 match ip dst 192.168.122.71 flowid 1:12
#sudo tc qdisc add dev enp1s0 parent 1:10 netem delay 30ms 3ms distribution normal
#sudo tc qdisc add dev enp1s0 parent 1:11 netem delay 50ms 5ms distribution normal
#sudo tc qdisc add dev enp1s0 parent 1:12 netem delay 80ms 8ms distribution normal
