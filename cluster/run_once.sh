#!/bin/bash
BASE_DIR=`realpath $(dirname $0)`
ROOT_DIR=`realpath $BASE_DIR/..`

HELPER_SCRIPT=$ROOT_DIR/scripts/exp_helper

MANAGER_HOST=`$HELPER_SCRIPT get-docker-manager-host --base-dir=$BASE_DIR`
CLIENT_HOST=`$HELPER_SCRIPT get-client-host --base-dir=$BASE_DIR`
MIDDLE_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=middleshim`
ALL_HOSTS=`$HELPER_SCRIPT get-all-server-hosts --base-dir=$BASE_DIR`

ALL_ENGINE_HOSTS=`$HELPER_SCRIPT get-machine-with-label --base-dir=$BASE_DIR --machine-label=engine_node`
$HELPER_SCRIPT generate-docker-compose --base-dir=$BASE_DIR
scp -q $BASE_DIR/docker-compose.yml $MANAGER_HOST:~
scp -q $BASE_DIR/docker-compose-placement.yml $MANAGER_HOST:~

ssh -q $MANAGER_HOST -- docker stack rm tfaas-experiments
sleep 20

for host in $ALL_HOSTS; do
    scp -q $BASE_DIR/func_config.json  $host:/tmp/func_config.json
done

for HOST in $ALL_ENGINE_HOSTS; do
    scp -q $BASE_DIR/run_launcher $HOST:/tmp/run_launcher
    ssh -q $HOST -- sudo rm -rf /mnt/inmem/tfaas
    ssh -q $HOST -- sudo rm -rf /mnt/inmem/requests
    ssh -q $HOST -- sudo mkdir -p /mnt/inmem/tfaas
    ssh -q $HOST -- sudo mkdir -p /mnt/inmem/requests
    ssh -q $HOST -- sudo mkdir -p /mnt/inmem/tfaas/output /mnt/inmem/tfaas/ipc
    ssh -q $HOST -- sudo cp /tmp/run_launcher /mnt/inmem/tfaas/run_launcher
    ssh -q $HOST -- sudo cp /tmp/func_config.json /mnt/inmem/tfaas/func_config.json
done

ssh -q $MANAGER_HOST -- docker stack deploy \
    -c ~/docker-compose.yml -c ~/docker-compose-placement.yml tfaas-experiments
sleep 30

scp -q -r $ROOT_DIR/benchmark $CLIENT_HOST:/home/ubuntu/benchmark

