/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-06-17 12:00:28
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-09-01 21:53:48
 * @Description: start cacheserver
 */

package main

import (
	"log"
	"time"
	"wsh/server/cacheserver"

	"github.com/lechou-0/common/util"
)

func main() {
	time.Sleep(5 * time.Second)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	client, err := util.NewEtcdClient()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Init server...")
	cacheServer := cacheserver.NewServer(client)
	defer cacheServer.Zctx.Term()
	log.Println("Running...")
	cacheServer.Run()

}
