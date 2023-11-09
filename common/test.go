package main

import (
	"encoding/json"
	"log"

	pb "github.com/lechou-0/common/proto"
	"github.com/lechou-0/common/storage"
	"google.golang.org/protobuf/proto"
)
func main() {
	dsm := storage.NewDynamoStorageManager("test")
	kv := &pb.KeyValuePair{}
	bkv, _ := dsm.Get("1000")
	proto.Unmarshal(bkv, kv)
	log.Println(kv)
	result := map[string]any{}
	json.Unmarshal(kv.Value, &result)
	log.Println(result)
}