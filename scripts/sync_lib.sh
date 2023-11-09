#!/bin/bash

BASE_DIR=`realpath $(dirname $0)/..`

rm -rf $BASE_DIR/CacheClient/common
mkdir $BASE_DIR/CacheClient/common
cp -r $BASE_DIR/common/* $BASE_DIR/CacheClient/common
rm -rf $BASE_DIR/CacheClient/common/test.go

rm -rf $BASE_DIR/CacheServer/common
mkdir $BASE_DIR/CacheServer/common
cp -r $BASE_DIR/common/* $BASE_DIR/CacheServer/common
rm -rf $BASE_DIR/CacheServer/common/test.go

rm -rf $BASE_DIR/MiddleShim/common
mkdir $BASE_DIR/MiddleShim/common
cp -r $BASE_DIR/common/* $BASE_DIR/MiddleShim/common
rm -rf $BASE_DIR/MiddleShim/common/test.go

for file in $BASE_DIR/workloads/*
do
    rm -rf $file/CacheClient
    mkdir $file/CacheClient
    cp -r $BASE_DIR/CacheClient/* $file/CacheClient
done
