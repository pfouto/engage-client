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
  --reads_per)
    reads_arg="$2"
    shift # past argument
    shift # past value
    ;;
  --threads)
    threads_arg="$2"
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
if [[ -z "${reads_arg}" ]]; then
  echo "reads_per not set"
  exit
fi
if [[ -z "${threads_arg}" ]]; then
  echo "threads not set"
  exit
fi

all_nodes=$(./nodes.sh)
start_date=$(date +"%H:%M:%S")
n_nodes=$(wc -l <<<"$all_nodes")

if [ $((n_servers + n_clients)) -gt "${n_nodes}" ]; then
  echo -e "${RED}Not enough nodes: Got ${n_nodes}, required $((n_servers + n_clients))$NC"
  exit
fi

#serverswithoutport=""
#for snode in $server_nodes; do
#serverswithoutport=${serverswithoutport}${snode}","
#done
#serverswithoutport=${serverswithoutport::-1}

mapfile -t client_nodes < <(tail -n "$n_clients" <<<"$all_nodes")
mapfile -t server_nodes < <(head -n "$n_servers" <<<"$all_nodes")

IFS=', ' read -r -a algslist <<<"$algs_arg"
IFS=', ' read -r -a readslist <<<"$reads_arg"
IFS=', ' read -r -a threadslist <<<"$threads_arg"

total_runs=$((n_runs * ${#algslist[@]} * ${#readslist[@]} * ${#threadslist[@]}))

# ----------------------------------- LOG PARAMS ------------------------------
echo -e "$BLUE \n ---- CONFIG ---- $NC"
echo -e "$GREEN exp_name: $NC ${exp_name}"
# shellcheck disable=SC2086
echo -e "$GREEN servers (${n_servers}): $NC" ${server_nodes[*]}
# shellcheck disable=SC2086
echo -e "$GREEN clients (${n_clients}): $NC" ${client_nodes[*]}
echo -e "$GREEN n_runs: $NC ${n_runs}"
echo -e "$GREEN start_run: $NC ${start_run}"
echo -e "$GREEN reads percent: $NC ${readslist[*]}"
echo -e "$GREEN algs: $NC ${algslist[*]}"
echo -e "$GREEN n threads: $NC ${threadslist[*]}"
echo -e "$GREEN ---------- $NC"
echo -e "$GREEN number of runs: $NC${total_runs}"
echo -e "$BLUE ---- END CONFIG ---- \n $NC"

current_run=0
sleep 3

# ----------------------------------- START EXP -------------------------------

#rm -rf /tmp/cass

echo -e "$GREEN -- Waiting for compilation $NC"
for server_node in "${server_nodes[@]}"; do
  oarsh "$server_node" "mkdir -p /tmp/cass && rsync -arzP /home/pfouto/engage/cass/ /tmp/cass/${OAR_JOB_ID} \
                                && cd /tmp/cass/${OAR_JOB_ID} && CASSANDRA_USE_JDK11=true ant > /dev/null" &
done
wait
echo -e "$GREEN -- Compilation finished $NC"

for alg in "${algslist[@]}"; do # ----------------------------------- ALG
  echo -e "$GREEN -- -- -- -- -- -- STARTING ALG $NC$alg"

  mkdir -p ~/engage/logs/metadata/"${exp_name}"/"${alg}"

  if [ "$alg" == "saturn" ]; then
    mf_enabled="false"
  else
    mf_enabled="true"
  fi

  echo -e "$BLUE Setting alg in cassandra.yaml to ${alg} $NC"

  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "sed -i \"s/^\(\s*protocol\s*:\s*\).*/\1'${alg}'/\"" /tmp/cass/"${OAR_JOB_ID}"/conf/cassandra.yaml
  done

  unset meta_pids
  meta_pids=()
  echo -e "$BLUE Starting metadata and sleeping 5 $NC"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "cd engage && java -Dlog4j.configurationFile=config/log4j2.xml \
											-DlogFilename=/home/pfouto/engage/logs/metadata/${exp_name}/${alg}/${server_node}_metadata \
											-jar metadata-1.0-SNAPSHOT.jar mf_enabled=${mf_enabled}" 2>&1 | sed "s/^/[m-$server_node] /" &
    meta_pids+=($!)
  done
  sleep 5

  echo -e "$BLUE Deleting cassandra data $NC"
  rm -rf /tmp/cass/"${OAR_JOB_ID}"

  echo -e "$BLUE Launching cassandra and sleeping 60 $NC"
  unset cass_pids
  cass_pids=()
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "cd /tmp/cass/${OAR_JOB_ID} && bin/cassandra -f >/dev/null" 2>&1 | sed "s/^/[s-$server_node] /" &
    cass_pids+=($!)
  done

  sleep 60

  echo -e "$BLUE Loading data $NC"
  unset client_pids
  client_pids=()
  i=0
  for client_node in "${client_nodes[@]}"; do
    server_node=${server_nodes[i]}
    oarsh "$client_node" "cd engage && java -Dlog4j.configurationFile=log4j2_client.xml -cp engage-client.jar \
          site.ycsb.Client -load -P workload -p localdc=$server_node -p engage.protocol=$alg -threads 1 \
          > /dev/null" 2>&1 | sed "s/^/[c-$client_node] /" &
    client_pids+=($!)
    i=$((i + 1))
  done
  for pid in "${client_pids[@]}"; do
    wait "$pid"
    echo -n "${pid} "
  done
  echo -e "$BLUE All clients finished, waiting 20 more seconds $NC"

  sleep 20

  echo -e "$BLUE Killing cassandra $NC"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "kill \$(ps aux | grep -v 'grep' | grep 'CassandraDaemon' | awk '{print \$2}')" &
  done

  for pid in "${cass_pids[@]}"; do
    echo -n "wait ${pid} "
    wait $pid
  done
  echo "Servers Killed"

  # ---------- RUN
  for run in $(seq "$start_run" $((n_runs + start_run - 1))); do
    echo -e "$GREEN -- STARTING RUN $NC$run"

    for reads_per in "${readslist[@]}"; do # ---------------------------  READS_PER
      echo -e "$GREEN -- -- -- STARTING READS PERCENTAGE $NC$reads_per"

      writes_per="$((100 - reads_per))"
      echo -e "$GREEN -- -- -- - ${NC}r:${reads_per} w:${writes_per}"

      exp_path="${exp_name}/${alg}/${reads_per}/${run}"

      mkdir -p ~/engage/logs/client/"${exp_path}"
      mkdir -p ~/engage/logs/server/"${exp_path}"

      for nthreads in "${threadslist[@]}"; do # -------------------- N_THREADS
        echo -e "$GREEN -- -- -- -- -- -- -- -- STARTING THREADS $NC$nthreads"
        echo -e "$GREEN -- -- -- -- -- -- -- -- - $NC$exp_path/$nthreads"

        rm -r ~/engage/logs/client/"${exp_path}"/"${nthreads}"_*
        rm -r ~/engage/logs/server/"${exp_path}"/"${nthreads}"_*

        ((current_run = current_run + 1))
        echo -e "$GREEN RUN ${current_run}/${total_runs} - ($(((current_run - 1) * 100 / total_runs))%) ($start_date) $NC"
        sleep 6

        echo -e "$BLUE Starting cassandra and sleeping 15 $NC"
        unset cass_pids
        cass_pids=()
        for server_node in "${server_nodes[@]}"; do
          oarsh "$server_node" "cd /tmp/cass/${OAR_JOB_ID} && bin/cassandra -f >/dev/null" 2>&1 | sed "s/^/[s-$server_node] /" &
          cass_pids+=($!)
        done
        sleep 15

        echo "Starting clients and sleeping 70"
        unset client_pids
        client_pids=()
        i=0
        for client_node in "${client_nodes[@]}"; do
          server_node=${server_nodes[i]}
          oarsh "$client_node" "cd engage && java -Dlog4j.configurationFile=log4j2_client.xml -cp engage-client.jar \
          site.ycsb.Client -P workload -p localdc=$server_node -p engage.protocol=$alg \
          -p readproportion=${reads_per} -p updateproportion=${writes_per} -threads 1 \
          > /home/pfouto/engage/logs/client/${exp_path}/${nthreads}_${client_node}" 2>&1 | sed "s/^/[c-$client_node] /" &
          client_pids+=($!)
          i=$((i + 1))
        done
        sleep 70

        echo "Killing clients"
        for client_node in "${client_nodes[@]}"; do
          oarsh "$client_node" "pkill java" &
        done
        wait
        for pid in ${client_pids[@]}; do
          wait $pid
          echo -n "${pid} "
        done
        echo "Clients Killed"
        sleep 20

        echo "Killing servers"
        for server_node in "${server_nodes[@]}"; do
          oarsh "$server_node" "kill \$(ps aux | grep -v 'grep' | grep 'CassandraDaemon' | awk '{print \$2}')" &
        done

        for pid in "${cass_pids[@]}"; do
          echo -n "wait ${pid} "
          wait $pid
        done
        echo "Servers Killed"
        sleep 1

      done #nthreads
    done #reads_per
  done #run

  echo "Killing metadata"
  for server_node in "${server_nodes[@]}"; do
    oarsh "$server_node" "pkill --full metadata-1.0" &
  done
  wait
  for pid in ${meta_pids[@]}; do
    wait $pid
    echo -n "${pid} "
  done
  echo "Metadata Killed"
  sleep 1
done #alg
echo -e "$BLUE -- -- -- -- -- -- -- -- All tests completed $NC"
exit
