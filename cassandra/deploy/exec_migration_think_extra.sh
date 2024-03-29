#!/usr/bin/env bash

# ----------------------------------- CONSTANTS -------------------------------
RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# ----------------------------------- PARSE PARAMS ----------------------------

start_run=1

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
  --exp_name)
    exp_name="$2"
    shift # past argument
    shift # past value
    ;;
  --tree_file)
    tree_file="$2"
    shift # past argument
    shift # past value
    ;;
  --think_times)
    think_times_arg="$2"
    shift # past argument
    shift # past value
    ;;
  --n_clients)
    n_clients="$2"
    shift # past argument
    shift # past value
    ;;
  --n_servers)
    n_servers="$2"
    shift # past argument
    shift # past value
    ;;
  --n_runs)
    n_runs="$2"
    shift # past argument
    shift # past value
    ;;
  --start_run)
    start_run="$2"
    shift # past argument
    shift # past value
    ;;
  --algs)
    algs_arg="$2"
    shift # past argument
    shift # past value
    ;;
  --guarantees)
    guarantees_arg="$2"
    shift # past argument
    shift # past value
    ;;
  --timer)
      timer_arg="$2"
      shift # past argument
      shift # past value
      ;;
  *) # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift              # past argument
    ;;
  esac
done

set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ -z "${exp_name}" ]]; then
  echo "exp_name not set"
  exit
fi
if [[ -z "${n_clients}" ]]; then
  echo "n_clients not set"
  exit
fi
if [[ -z "${n_runs}" ]]; then
  echo "n_runs not set"
  exit
fi
if [[ -z "${n_servers}" ]]; then
  echo "n_servers not set"
  exit
fi
if [[ -z "${algs_arg}" ]]; then
  echo "algs not set"
  exit
fi
if [[ -z "${guarantees_arg}" ]]; then
  echo "guarantees not set"
  exit
fi
if [[ -z "${tree_file}" ]]; then
  echo "tree_file not set"
  exit
fi
if [[ -z "${think_times_arg}" ]]; then
  echo "think_times not set"
  exit
fi
if [[ -z "${timer_arg}" ]]; then
  echo "timer not set"
  exit
fi


all_nodes=$(./nodes.sh)
start_date=$(date +"%H:%M:%S")
n_nodes=$(wc -l <<<"$all_nodes")

if [ $((n_servers + n_clients)) -gt "${n_nodes}" ]; then
  echo -e "${RED}Not enough nodes: Got ${n_nodes}, required $((n_servers + n_clients))$NC"
  exit
fi

mapfile -t client_nodes < <(tail -n "$n_clients" <<<"$all_nodes")
mapfile -t server_nodes < <(head -n "$n_servers" <<<"$all_nodes")

IFS=', ' read -r -a thinkslist <<<"$think_times_arg"
IFS=', ' read -r -a algslist <<<"$algs_arg"
IFS=', ' read -r -a guarantees_list <<<"$guarantees_arg"

total_runs=$((n_runs * ${#algslist[@]} * ${#thinkslist[@]} * ${#guarantees_list[@]}))

echo -e "$GREEN -- Rsync $NC"
for server_node in "${server_nodes[@]}"; do
  oarsh "$server_node" "mkdir -p /tmp/cass && rsync -arzq /home/pfouto/engage/cass/ /tmp/cass/${OAR_JOB_ID}" &
done
wait
echo -e "$GREEN -- Compiling $NC"
for server_node in "${server_nodes[@]}"; do
  oarsh "$server_node" "cd /tmp/cass/${OAR_JOB_ID} && CASSANDRA_USE_JDK11=true ant -S -q" &
done
wait
echo -e "$GREEN -- Compilation finished $NC"

for server_node in "${server_nodes[@]}"; do
  oarsh "$server_node" "sudo-g5k apt-get -q -y install libjemalloc-dev" &
done
wait
echo -e "$GREEN -- Done installing libjemalloc $NC"

for server_node in "${server_nodes[@]}"; do
  oarsh "$server_node" "sudo-g5k swapoff -a && sudo-g5k sysctl -w vm.max_map_count=1048575" &
done
wait
echo -e "$GREEN -- Done disabling swap && setting max_map count $NC"

records=10000
timer=$timer_arg
reads_per=50

# ----------------------------------- LOG PARAMS ------------------------------
echo -e "$BLUE \n ---- CONFIG ---- $NC"
echo -e "$GREEN exp_name: $NC ${exp_name}"
echo -e "$GREEN reads: $NC ${reads_per}"
# shellcheck disable=SC2086
echo -e "$GREEN servers (${n_servers}): $NC" ${server_nodes[*]}
# shellcheck disable=SC2086
echo -e "$GREEN clients (${n_clients}): $NC" ${client_nodes[*]}
echo -e "$GREEN n_runs: $NC ${n_runs}"
echo -e "$GREEN start_run: $NC ${start_run}"
echo -e "$GREEN think_times: $NC ${thinkslist[*]}"
echo -e "$GREEN algs: $NC ${algslist[*]}"
echo -e "$GREEN guarantees: $NC ${guarantees_list[*]}"
echo -e "$GREEN timer: $NC ${timer}"
echo -e "$GREEN ---------- $NC"
echo -e "$GREEN number of runs: $NC${total_runs}"
echo -e "$BLUE ---- END CONFIG ---- \n $NC"

current_run=0
sleep 3

echo -e "$GREEN -- Setup tree file $tree_file -> tree_${OAR_JOB_ID}.json $NC"
if [ ! -f "$HOME/engage/config/$tree_file" ]; then
  echo "File not found!"
  exit
fi
cp "$HOME/engage/config/$tree_file" "$HOME/engage/tree_${OAR_JOB_ID}.json"
for ((i = 0; i < n_servers; i++)); do
  sed -i "s/\"node-$((i + 1))\"/\"${server_nodes[$i]}\"/g" "$HOME/engage/tree_${OAR_JOB_ID}.json"
done
sleep 2

# ----------------------------------- START EXP -------------------------------

echo -e "$BLUE Setting log visibility to false in cassandra.yaml $NC"
for server_node in "${server_nodes[@]}"; do
  oarsh "$server_node" "sed -i \"s/^\(\s*log_visibility\s*:\s*\).*/\1'false'/\"" /tmp/cass/"${OAR_JOB_ID}"/conf/cassandra.yaml
done

#rm -rf /tmp/cass

for alg in "${algslist[@]}"; do # ----------------------------------- ALG
  echo -e "$GREEN -- -- -- -- -- -- STARTING ALG $NC$alg"

  mkdir -p ~/engage/logs/migration_think/metadata/"${exp_name}"/"${alg}"
  mkdir -p ~/engage/logs/migration_think/server/"${exp_name}"/"${alg}"

  if [ "$alg" == "saturn" ]; then
    mf_enabled="false"
  else
    mf_enabled="true"
  fi

  echo -e "$BLUE Setting alg in cassandra.yaml to ${alg} $NC"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "sed -i \"s/^\(\s*protocol\s*:\s*\).*/\1'${alg}'/\"" /tmp/cass/"${OAR_JOB_ID}"/conf/cassandra.yaml
  done

  echo -e "$BLUE Deleting cassandra data $NC"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "rm -rf /tmp/cass/${OAR_JOB_ID}/data/"
  done

  echo -e "$BLUE Starting metadata and sleeping 4 $NC"
  unset meta_pids
  meta_pids=()
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "cd engage && java -Dlog4j.configurationFile=config/log4j2.xml \
											-DlogFilename=/home/pfouto/engage/logs/migration_think/metadata/${exp_name}/${alg}/${server_node}_metadata_load \
											-jar metadata-1.0-SNAPSHOT.jar mf_enabled=${mf_enabled} \
											tree_file=tree_${OAR_JOB_ID}.json" 2>&1 | sed "s/^/[m-$server_node] /" &
    meta_pids+=($!)
  done
  sleep 4

  echo -e "$BLUE Launching cassandra and sleeping 70 $NC"
  unset cass_pids
  cass_pids=()
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "cd /tmp/cass/${OAR_JOB_ID} && mkdir -p results && bin/cassandra \
    -DlogFilename=/tmp/cass/${OAR_JOB_ID}/results/${exp_name}/${alg}/load_${server_node} -f >/dev/null" 2>&1 | sed "s/^/[s-$server_node] /" &
    cass_pids+=($!)
  done
  sleep 70

  echo -e "$BLUE Loading data $NC"
  unset client_pids
  client_pids=()
  i=0
  for client_node in "${client_nodes[@]}"; do
    server_node=${server_nodes[i]}
    oarsh "$client_node" "cd engage && java -Dlog4j.configurationFile=log4j2_client.xml -cp engage-client.jar \
          site.ycsb.Client -load -P workload -p localdc=$server_node -p engage.protocol=$alg -p measurementtype=timeseries \
          -p engage.ksmanager=visibility -p engage.tree_file=tree_${OAR_JOB_ID}.json -p recordcount=$records \
          -threads 500 > /dev/null" 2>&1 | sed "s/^/[c-$client_node] /" &
    client_pids+=($!)
    i=$((i + 1))
  done
  for pid in "${client_pids[@]}"; do
    wait "$pid"
  done
  echo -e "$BLUE All clients finished $NC"

  sleep 60

  echo -e "$BLUE Killing cassandra $NC"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "kill \$(ps aux | grep -v 'grep' | grep 'CassandraDaemon' | awk '{print \$2}')" &
  done
  for pid in "${cass_pids[@]}"; do
    wait "$pid"
  done
  echo -e "$BLUE Servers Killed $NC"

  echo -e "$BLUE Killing metadata $NC"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "pkill --full metadata-1.0" &
  done
  for pid in "${meta_pids[@]}"; do
    wait "$pid"
    echo -n "${pid} "
  done
  echo -e "$BLUE Metadata killed $NC"
  sleep 1

  echo -e "$BLUE Backing up cassandra data $NC"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "rm -r /tmp/cass/${OAR_JOB_ID}/data_$alg"
  done
  unset backup_pids
  backup_pids=()
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "cp -r /tmp/cass/${OAR_JOB_ID}/data /tmp/cass/${OAR_JOB_ID}/data_$alg" &
    backup_pids+=($!)
  done
  for pid in "${backup_pids[@]}"; do
    wait "$pid"
  done

  # ---------- RUN
  for run in $(seq "$start_run" $((n_runs + start_run - 1))); do
    echo -e "$GREEN -- STARTING RUN $NC$run"

    for think_time in "${thinkslist[@]}"; do # ---------------------------  READS_PER
      echo -e "$GREEN -- -- -- STARTING THINK  $NC$think_time"

      writes_per="$((100 - reads_per))"

      exp_path="${exp_name}/${alg}/${think_time}/${run}"

      mkdir -p ~/engage/logs/migration_think/client/"${exp_path}"
      mkdir -p ~/engage/logs/migration_think/server/"${exp_path}"
      mkdir -p ~/engage/logs/migration_think/metadata/"${exp_path}"

      for guarantee in "${guarantees_list[@]}"; do # -------------------- GUARANTEE
        echo -e "$GREEN -- -- -- -- -- -- -- -- STARTING GUARANTEE $NC$guarantee"
        echo -e "$GREEN -- -- -- -- -- -- -- -- - $NC$exp_path/$guarantee"

        rm -r ~/engage/logs/migration_think/client/"${exp_path}"/"${guarantee}"_*
        rm -r ~/engage/logs/migration_think/server/"${exp_path}"/"${guarantee}"_*
        rm -r ~/engage/logs/migration_think/metadata/"${exp_path}"/"${guarantee}"_*

        ((current_run = current_run + 1))
        echo -e "$GREEN RUN ${current_run}/${total_runs} - ($(((current_run - 1) * 100 / total_runs))%) ($start_date) $NC"
        sleep 2

        unset backup_pids
        backup_pids=()
        echo -e "$BLUE Restoring backup data $NC"
        for server_node in "${server_nodes[@]}"; do
          oarsh "$server_node" "rm -r /tmp/cass/${OAR_JOB_ID}/data && cp -r /tmp/cass/${OAR_JOB_ID}/data_$alg /tmp/cass/${OAR_JOB_ID}/data" &
          backup_pids+=($!)
        done
        for pid in "${backup_pids[@]}"; do
          wait "$pid"
        done

        echo -e "$BLUE Starting metadata and sleeping 4 $NC"
        unset meta_pids
        meta_pids=()
        for server_node in "${server_nodes[@]}"; do
          oarsh "$server_node" "cd engage && java -Dlog4j.configurationFile=config/log4j2.xml \
											-DlogFilename=/home/pfouto/engage/logs/migration_think/metadata/${exp_path}/${guarantee}_${server_node} \
											-jar metadata-1.0-SNAPSHOT.jar mf_enabled=${mf_enabled} setup_cass=false \
											bayou.stab_ms=$timer mf_timeout_ms=$timer \
											tree_file=tree_${OAR_JOB_ID}.json" 2>&1 | sed "s/^/[m-$server_node] /" &
          meta_pids+=($!)
        done
        sleep 4

        echo -e "$BLUE Starting cassandra and sleeping 15 $NC"
        unset cass_pids
        cass_pids=()
        for server_node in "${server_nodes[@]}"; do
          oarsh "$server_node" "cd /tmp/cass/${OAR_JOB_ID} && bin/cassandra \
          -DlogFilename=/tmp/cass/${OAR_JOB_ID}/results/${exp_path}/${guarantee}_${server_node} \
          -f > /dev/null" 2>&1 | sed "s/^/[s-$server_node] /" &
          cass_pids+=($!)
        done
        sleep 15

        echo -e "$BLUE Starting clients and sleeping 90 $NC"
        unset client_pids
        client_pids=()
        i=0
        for client_node in "${client_nodes[@]}"; do
          server_node=${server_nodes[i]}
          oarsh "$client_node" "cd engage && java -Dlog4j.configurationFile=log4j2_client.xml -cp engage-client.jar \
          site.ycsb.Client -P workload -p localdc=$server_node -p engage.protocol=$alg -p readproportion=${reads_per} \
          -p updateproportion=${writes_per} -threads 25 -p engage.tree_file=tree_${OAR_JOB_ID}.json \
          -p engage.migration_enabled=false \
          -p measurementtype=hdrhistogram \
          -p engage.session_guarantees=$guarantee \
          -p engage.ops_local=10 -p engage.ops_remote=10 \
          -p engage.ksmanager=regular -p recordcount=$records -thinktime ${think_time} \
          > /home/pfouto/engage/logs/migration_think/client/${exp_path}/${guarantee}_${client_node}" 2>&1 | sed "s/^/[c-$client_node] /" &
          client_pids+=($!)
          oarsh "$client_node" "cd engage && java -Dlog4j.configurationFile=log4j2_client.xml -cp engage-client.jar \
          site.ycsb.Client -P workload -p localdc=$server_node -p engage.protocol=$alg -p readproportion=${reads_per} \
          -p updateproportion=${writes_per} -threads 50 -p engage.tree_file=tree_${OAR_JOB_ID}.json \
          -p engage.migration_enabled=false \
          -p measurementtype=hdrhistogram \
          -p engage.session_guarantees=$guarantee \
          -p engage.ops_local=10 -p engage.ops_remote=10 \
          -p engage.ksmanager=visibility -p recordcount=$records -target 1000 \
          > /dev/null" 2>&1 | sed "s/^/[c-$client_node] /" &
          client_pids+=($!)
          i=$((i + 1))
        done
        sleep 90

        echo -e "$BLUE Killing clients $NC"
        for client_node in "${client_nodes[@]}"; do
          oarsh "$client_node" "pkill java" &
        done
        for pid in "${client_pids[@]}"; do
          wait "$pid"
        done
        echo -e "$BLUE Clients killed - Waiting 25 $NC"
        sleep 25

        echo -e "$BLUE Killing servers $NC"
        for server_node in "${server_nodes[@]}"; do
          oarsh "$server_node" "kill \$(ps aux | grep -v 'grep' | grep 'CassandraDaemon' | awk '{print \$2}')" &
        done

        for pid in "${cass_pids[@]}"; do
          wait "$pid"
        done
        echo -e "$BLUE Servers killed $NC"

        echo -e "$BLUE Killing metadata $NC"
        for server_node in "${server_nodes[@]}"; do
          oarsh "$server_node" "pkill --full metadata-1.0" &
        done
        for pid in "${meta_pids[@]}"; do
          wait "$pid"
          echo -n "${pid} "
        done
        echo -e "$BLUE Metadata killed $NC"
        sleep 1

      done #guarantee
    done #think
  done #run
done #alg
echo -e "$BLUE -- -- -- -- -- -- -- -- All tests completed $NC"
echo "Deleting tree file"
rm "$HOME/engage/tree_${OAR_JOB_ID}.json"
echo "Getting logs"
for server_node in "${server_nodes[@]}"; do
  oarsh "$server_node" "cp -r /tmp/cass/${OAR_JOB_ID}/results/* /home/pfouto/engage/logs/migration_think/server/" &
done
wait
exit