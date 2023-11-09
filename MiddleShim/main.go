/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-08-17 22:55:50
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-18 14:52:12
 * @Description:
 */
package main

import (
	"log"
	"net"
	"time"

	pb "github.com/lechou-0/common/proto"
	"github.com/lechou-0/common/util"

	"google.golang.org/grpc"
)

var lisPort = ":" + util.MiddleshimPort

func main() {
	time.Sleep(5 * time.Second)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	client, err := util.NewEtcdClient()
	if err != nil {
		log.Fatal(err)
	}
	RPCserver := grpc.NewServer()
	middleServer := NewMiddleshimServer(client)
	pb.RegisterMiddleshimServer(RPCserver, middleServer)
	lis, err := net.Listen("tcp", lisPort)
	if err != nil {
		log.Fatalf("Could not listen on port %s: %v\n", lisPort, err)
	}

	ser, err := NewServiceRegister(client, util.Prefix+middleServer.IpAddress, middleServer.IpAddress+lisPort, 5)
	if err != nil {
		log.Fatalf("register service err: %v", err)
	}
	defer ser.Close()
	if err = RPCserver.Serve(lis); err != nil {
		log.Fatalf("Could not start server on port %s: %v\n", lisPort, err)
	}
	defer middleServer.CloseServer()
}
