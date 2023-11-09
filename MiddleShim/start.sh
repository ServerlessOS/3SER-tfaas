#!/bin/sh
###
 # @Author: lechoux lechoux@qq.com
 # @Date: 2022-08-17 21:51:26
 # @LastEditors: lechoux lechoux@qq.com
 # @LastEditTime: 2022-09-05 19:20:25
 # @Description: 
### 
cd proto
protoc --go_out=. *.proto
protoc --go-grpc_out=. *.proto
cd ..
go mod tidy
go run . 