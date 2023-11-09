/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-11-07 16:14:35
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-18 16:21:04
 * @Description:
 */
package storage

import (
	pb "github.com/lechou-0/common/proto"
)

const TransactionKey = "transactions/%s-%d"

type StorageManager interface {

	// As a part of the transaction owned by tid, insert a key-value pair into
	// the storage engine.
	Put(key string, serialized []byte) error

	// As a part of the transaction owned by tid, insert a key-value pair into
	// the storage engine.
	PutState(key string, serialized []byte) error

	// As a part of transaction owned by tid, insert a set of key-value pairs
	// into the storage engine.
	MultiPut([]*pb.KeyValuePair) error

	// Retrieve the given key as a part of the transaction tid.
	Get(key string) ([]byte, error)

	// Strong consistency read
	GetSC(key string) ([]byte, error)

	// Deletes the given key from the underlying storage engine.
	Delete(key string) error

	// Deletes the given key from the underlying storage engine.
	DeleteState(key string) error

	// Delete multiple keys at once.
	MultiDelete([]string) error

	// Create state table
	CreateTable() error

	CloseManager()
}
