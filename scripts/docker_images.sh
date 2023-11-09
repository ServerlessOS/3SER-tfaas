#!/bin/bash

ROOT_DIR=`realpath $(dirname $0)/..`
version=v2.50.1
# Use BuildKit as docker builder
export DOCKER_BUILDKIT=1
sh $ROOT_DIR/scripts/sync_lib.sh

function build_cacheserver {
    docker build -t lechou/cacheserver:$version \
        -f $ROOT_DIR/dockerfiles/Dockerfile.cacheserver \
        $ROOT_DIR/CacheServer
}

function build_middleshim {
    docker build -t lechou/middleshim:$version \
        -f $ROOT_DIR/dockerfiles/Dockerfile.middleshim \
        $ROOT_DIR/MiddleShim
}

function build_ef {
    docker build -t lechou/ef:$version \
        -f $ROOT_DIR/dockerfiles/Dockerfile.ef \
        $ROOT_DIR/workloads/ef
}

function build_of {
    docker build -t lechou/of:$version \
        -f $ROOT_DIR/dockerfiles/Dockerfile.of \
        $ROOT_DIR/workloads/of
}

function build {
    build_cacheserver
    build_middleshim
    build_ef
    build_of
}

function push {
    build_cacheserver
    build_middleshim
    build_ef
    build_of
    docker push lechou/cacheserver:$version
    docker push lechou/middleshim:$version
    docker push lechou/ef:$version
    docker push lechou/of:$version
}

case "$1" in
build)
    build
    ;;
push)
    push
    ;;
esac