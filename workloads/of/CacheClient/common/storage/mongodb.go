package storage

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/lechou-0/common/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"google.golang.org/protobuf/proto"
)

type MongoStorageManager struct {
	client1 *mongo.Client
	client2 *mongo.Client
	doc     *mongo.Collection
	scdoc   *mongo.Collection
}

func (mongoManager *MongoStorageManager) CloseManager() {
	mongoManager.client1.Disconnect(context.TODO())
	mongoManager.client2.Disconnect(context.TODO())
}

func NewMongoStorageManager(dataTable string) *MongoStorageManager {
	uri, exists := os.LookupEnv("MONGODB_URI")
	if !exists {
		log.Println("mongodb url is not found")
		return nil
	}
	opts := options.Client()
	opts.ApplyURI(uri)
	opts.SetAuth(options.Credential{
		Username:   "testUser",
		Password:   "1234",
		AuthSource: "data",
	})
	opts.SetReadConcern(readconcern.Majority())
	opts.SetReadPreference(readpref.SecondaryPreferred())
	newCtx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()
	if client, err := mongo.Connect(newCtx, opts); err != nil {
		log.Fatalf("[FATAL] Failed to connect to mongo %s: %v", uri, err)
		return nil
	} else {
		db := client.Database("data")
		collection := db.Collection(dataTable)
		opts.SetReadPreference(readpref.Primary())
		if scClient, err := mongo.Connect(newCtx, opts); err != nil {
			log.Fatalf("[FATAL] Failed to connect to mongo %s: %v", uri, err)
			return nil
		} else {
			db1 := scClient.Database("data")
			collection1 := db1.Collection(dataTable)
			return &MongoStorageManager{
				client1: client,
				client2: scClient,
				doc:     collection,
				scdoc:   collection1,
			}
		}
	}
}

func (mongoManager *MongoStorageManager) CreateTable() error {
	return nil
}

func (mongoManager *MongoStorageManager) Get(key string) ([]byte, error) {
	datakey := bson.D{{Key: "DataKey", Value: key}}
	var value bson.M
	mongoManager.doc.FindOne(context.TODO(), datakey).Decode(&value)

	if len(value) == 0 {
		log.Printf("%s not found", key)
		return mongoManager.Get(key)
	}
	return value["Value"].(primitive.Binary).Data, nil
}

func (mongoManager *MongoStorageManager) GetSC(key string) ([]byte, error) {
	datakey := bson.D{{Key: "DataKey", Value: key}}
	var value bson.M
	mongoManager.scdoc.FindOne(context.TODO(), datakey).Decode(&value)
	if len(value) == 0 {
		return nil ,nil
	}
	return value["Value"].(primitive.Binary).Data, nil
}

func (mongoManager *MongoStorageManager) Put(key string, serialized []byte) error {
	datakey := bson.D{{Key: "DataKey", Value: key}}
	if _, err := mongoManager.doc.UpdateOne(context.TODO(), datakey, bson.D{{Key: "$set", Value: bson.D{{Key: "DataKey", Value: key}, {Key: "Value", Value: serialized}}}}, options.Update().SetUpsert(true)); err != nil {
		return err
	}
	return nil
}

func (mongoManager *MongoStorageManager) PutState(key string, serialized []byte) error {
	state := bson.D{{Key: "DataKey", Value: key}, {Key: "Value", Value: serialized}}
	if _, err := mongoManager.doc.InsertOne(context.TODO(), state); err != nil {
		return err
	}
	return nil
}

func (mongoManager *MongoStorageManager) MultiPut(data []*pb.KeyValuePair) error {
	var models []mongo.WriteModel
	for _, val := range data {
		key := val.GetKey()
		serialized, err := proto.Marshal(val)
		if err != nil {
			return err
		}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(bson.D{{Key: "DataKey", Value: key}}).SetUpdate(bson.D{{Key: "$set", Value: bson.D{{Key: "DataKey", Value: key}, {Key: "Value", Value: serialized}}}}).SetUpsert(true))

	}

	opts := options.BulkWrite().SetOrdered(false)
	_, err := mongoManager.doc.BulkWrite(context.TODO(), models, opts)

	if err != nil {
		return err
	}
	return nil
}

func (mongoManager *MongoStorageManager) Delete(key string) error {
	datakey := bson.D{{Key: "DataKey", Value: key}}
	if _, err := mongoManager.doc.DeleteOne(context.TODO(), datakey); err != nil {
		return err
	}
	return nil
}

func (mongoManager *MongoStorageManager) DeleteState(key string) error {
	datakey := bson.D{{Key: "DataKey", Value: key}}
	if _, err := mongoManager.doc.DeleteMany(context.TODO(), datakey); err != nil {
		return err
	}
	return nil
}

func (mongoManager *MongoStorageManager) MultiDelete(keys []string) error {
	var models []mongo.WriteModel
	for _, key := range keys {

		models = append(models, mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "DataKey", Value: key}}))

	}

	opts := options.BulkWrite().SetOrdered(false)
	_, err := mongoManager.doc.BulkWrite(context.TODO(), models, opts)

	if err != nil {
		return err
	}
	return nil
}
