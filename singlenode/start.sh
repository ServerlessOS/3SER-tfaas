#!/bin/bash

BASE_DIR=`realpath $(dirname $0)/..`
function up {
    sudo rm -rf /mnt/inmem/tfaas
    sudo rm -rf /mnt/inmem/requests
    sudo mkdir -p /mnt/inmem/tfaas
    sudo mkdir -p /mnt/inmem/requests
    sudo mkdir -p /mnt/inmem/tfaas/output /mnt/inmem/tfaas/ipc
    sudo cp $BASE_DIR/singlenode/run_launcher /mnt/inmem/tfaas/run_launcher
    sudo cp $BASE_DIR/singlenode/func_config.json /mnt/inmem/tfaas/func_config.json
    sudo docker-compose -f $BASE_DIR/singlenode/docker-compose.yml up -d
}

function down {
    sudo docker-compose -f $BASE_DIR/singlenode/docker-compose.yml down
}

case "$1" in
up)
    up
    ;;
down)
    down
    ;;
esac