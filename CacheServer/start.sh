#!/bin/sh
###
 # @Author: lechoux lechoux@qq.com
 # @Date: 2022-06-17 16:58:47
 # @LastEditors: lechoux lechoux@qq.com
 # @LastEditTime: 2022-08-16 16:36:16
 # @Description: 
### 
go mod tidy
go run . &>log.txt