/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-08-26 18:20:26
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-16 14:22:22
 * @Description:
 */
package storage

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/proto"

	pb "github.com/lechou-0/common/proto"
)

type DynamoStorageManager struct {
	dataTable    string
	dynamoClient *awsdynamo.Client
}

func NewDynamoStorageManager(dataTable string) *DynamoStorageManager {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-2"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("your_access_key", "your_secret", "")))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	dc := awsdynamo.NewFromConfig(cfg)
	dynanoClient := &DynamoStorageManager{
		dataTable:    dataTable,
		dynamoClient: dc,
	}
	exist, _ := dynanoClient.TableExists()
	if !exist {
		dynanoClient.CreateTable()
	}
	return dynanoClient
}

func (dynamo *DynamoStorageManager) CloseManager() {

}

func (dynamo *DynamoStorageManager) CreateTable() error {

	_, err := dynamo.dynamoClient.CreateTable(context.TODO(), &awsdynamo.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("DataKey"),
			AttributeType: types.ScalarAttributeTypeS,
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("DataKey"),
			KeyType:       types.KeyTypeHash,
		}},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   aws.String(dynamo.dataTable),
	})
	if err != nil {
		log.Println(err)
		return err
	} else {
		waiter := awsdynamo.NewTableExistsWaiter(dynamo.dynamoClient)
		err = waiter.Wait(context.TODO(), &awsdynamo.DescribeTableInput{
			TableName: aws.String(dynamo.dataTable)}, 1*time.Minute)
		if err != nil {
			log.Println(err)
			return err
		}
	}
	log.Printf("Table %v was created successfully\n", dynamo.dataTable)
	return err
}

func (dynamo *DynamoStorageManager) TableExists() (bool, error) {
	exists := true
	_, err := dynamo.dynamoClient.DescribeTable(
		context.TODO(), &awsdynamo.DescribeTableInput{TableName: aws.String(dynamo.dataTable)},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("Table %v does not exist.\n", dynamo.dataTable)
			err = nil
		} else {
			log.Printf("Couldn't determine existence of table %v. Here's why: %v\n", dynamo.dataTable, err)
		}
		exists = false
	}
	return exists, err
}

func (dynamo *DynamoStorageManager) Get(key string) ([]byte, error) {
	input := &awsdynamo.GetItemInput{
		Key:       constructKeyData(key),
		TableName: aws.String(dynamo.dataTable),
	}
	var result []byte
	item, err := dynamo.dynamoClient.GetItem(context.TODO(), input)
	if err != nil {
		log.Println(err)
		return result, err
	}

	for item == nil || item.Item == nil {
		item, err = dynamo.dynamoClient.GetItem(context.TODO(), input)
		if err != nil {
			log.Println(err)
			return result, err
		}
	}

	err = attributevalue.Unmarshal(item.Item["Value"], &result)
	if err != nil {
		log.Println(err)
		return result, err
	}

	return result, nil
}

func (dynamo *DynamoStorageManager) GetSC(key string) ([]byte, error) {
	input := &awsdynamo.GetItemInput{
		Key:            constructKeyData(key),
		TableName:      aws.String(dynamo.dataTable),
		ConsistentRead: aws.Bool(true),
	}
	var result []byte
	item, err := dynamo.dynamoClient.GetItem(context.TODO(), input)
	if err != nil {
		return result, err
	}

	if item == nil || item.Item == nil {
		return nil, nil
	}

	err = attributevalue.Unmarshal(item.Item["Value"], &result)
	if err != nil {
		return result, err
	}

	return result, nil
}

func (dynamo *DynamoStorageManager) Put(key string, serialized []byte) error {

	input := constructPutInput(key, dynamo.dataTable, serialized)
	_, err := dynamo.dynamoClient.PutItem(context.TODO(), &input)
	return err
}

func (dynamo *DynamoStorageManager) PutState(key string, serialized []byte) error {

	input := constructPutInput(key, dynamo.dataTable, serialized)
	_, err := dynamo.dynamoClient.PutItem(context.TODO(), &input)
	return err
}

func (dynamo *DynamoStorageManager) MultiPut(data []*pb.KeyValuePair) error {
	inputData := map[string][]types.WriteRequest{}
	inputData[dynamo.dataTable] = []types.WriteRequest{}

	numWrites := 0
	for _, val := range data {
		serialized, err := proto.Marshal(val)
		if err != nil {
			return err
		}

		keyData := map[string]types.AttributeValue{"DataKey": &types.AttributeValueMemberS{Value: val.GetKey()}, "Value": &types.AttributeValueMemberB{Value: serialized}}

		putReq := &types.PutRequest{Item: keyData}
		inputData[dynamo.dataTable] = append(inputData[dynamo.dataTable], types.WriteRequest{PutRequest: putReq})

		// DynamoDB's BatchWriteItem only supports 25 writes at a time, so if we
		// have more than that, we break it up.
		numWrites += 1
		if numWrites == 25 {
			_, err := dynamo.dynamoClient.BatchWriteItem(context.TODO(), &awsdynamo.BatchWriteItemInput{RequestItems: inputData})
			if err != nil {
				return err
			}

			inputData = map[string][]types.WriteRequest{}
			inputData[dynamo.dataTable] = []types.WriteRequest{}
			numWrites = 0
		}
	}

	if len(inputData[dynamo.dataTable]) > 0 {
		_, err := dynamo.dynamoClient.BatchWriteItem(context.TODO(), &awsdynamo.BatchWriteItemInput{RequestItems: inputData})
		return err
	}

	return nil
}

func (dynamo *DynamoStorageManager) Delete(key string) error {
	input := &awsdynamo.DeleteItemInput{
		Key:       constructKeyData(key),
		TableName: aws.String(dynamo.dataTable),
	}

	_, err := dynamo.dynamoClient.DeleteItem(context.TODO(), input)

	return err
}

func (dynamo *DynamoStorageManager) DeleteState(key string) error {
	input := &awsdynamo.DeleteItemInput{
		Key:       constructKeyData(key),
		TableName: aws.String(dynamo.dataTable),
	}

	_, err := dynamo.dynamoClient.DeleteItem(context.TODO(), input)

	return err
}

func (dynamo *DynamoStorageManager) MultiDelete(keys []string) error {
	inputData := map[string][]types.WriteRequest{}
	inputData[dynamo.dataTable] = []types.WriteRequest{}

	numDeletes := 0
	for _, key := range keys {
		keyData := map[string]types.AttributeValue{"DataKey": &types.AttributeValueMemberS{Value: key}}

		deleteReq := &types.DeleteRequest{Key: keyData}
		inputData[dynamo.dataTable] = append(inputData[dynamo.dataTable], types.WriteRequest{DeleteRequest: deleteReq})

		numDeletes += 1
		if numDeletes == 25 {
			_, err := dynamo.dynamoClient.BatchWriteItem(context.TODO(), &awsdynamo.BatchWriteItemInput{RequestItems: inputData})
			if err != nil {
				return err
			}

			inputData = map[string][]types.WriteRequest{}
			inputData[dynamo.dataTable] = []types.WriteRequest{}
			numDeletes = 0
		}
	}

	_, err := dynamo.dynamoClient.BatchWriteItem(context.TODO(), &awsdynamo.BatchWriteItemInput{RequestItems: inputData})
	return err
}

func constructKeyData(key string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"DataKey": &types.AttributeValueMemberS{Value: key}}
}

func constructPutInput(key string, table string, data []byte) awsdynamo.PutItemInput {
	return awsdynamo.PutItemInput{
		Item:      map[string]types.AttributeValue{"DataKey": &types.AttributeValueMemberS{Value: key}, "Value": &types.AttributeValueMemberB{Value: data}},
		TableName: aws.String(table),
	}
}
