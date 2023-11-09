/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-08-22 16:20:53
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-18 16:21:08
 * @Description:
 */
package storage

import (
	"bytes"
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/proto"

	pb "github.com/lechou-0/common/proto"
)

type S3StorageManager struct {
	bucket   string
	s3Client *awss3.Client
}

func NewS3StorageManager(bucket string) *S3StorageManager {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKIAYOK3WZGEVHTUQ5TF", "2Q50P43NwkqPgzDMCLHRZzp5ELpDz6jylAlZ5PUv", "")))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	s3c := awss3.NewFromConfig(cfg)
	return &S3StorageManager{bucket: bucket, s3Client: s3c}
}

func (s3 *S3StorageManager) Get(key string) (*pb.KeyValuePair, error) {
	input := &awss3.GetObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	result := &pb.KeyValuePair{}

	getObjectOutput, err := s3.s3Client.GetObject(context.TODO(), input)
	if err != nil {
		return result, err
	}

	body := new(bytes.Buffer)
	_, err = body.ReadFrom(getObjectOutput.Body)
	if err != nil {
		return result, err
	}
	err = proto.Unmarshal(body.Bytes(), result)

	return result, err
}

func (s3 *S3StorageManager) Put(key string, val *pb.KeyValuePair) error {
	serialized, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(serialized),
	}

	_, err = s3.s3Client.PutObject(context.TODO(), input)

	return err
}

func (s3 *S3StorageManager) MultiPut(data *map[string]*pb.KeyValuePair) error {
	for key, val := range *data {
		err := s3.Put(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s3 *S3StorageManager) Delete(key string) error {
	input := &awss3.DeleteObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	_, err := s3.s3Client.DeleteObject(context.TODO(), input)
	return err
}

func (s3 *S3StorageManager) MultiDelete(keys *[]string) error {
	for _, key := range *keys {
		err := s3.Delete(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s3 *S3StorageManager) List(prefix string) ([]string, error) {
	input := &awss3.ListObjectsV2Input{
		Bucket: &s3.bucket,
		Prefix: &prefix,
	}

	additionalKeys := true
	returnValue := []string{}

	for additionalKeys {
		result, err := s3.s3Client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			return nil, err
		}

		for _, val := range result.Contents {
			returnValue = append(returnValue, *val.Key)
		}

		if result.IsTruncated {
			input.ContinuationToken = result.NextContinuationToken
		} else {
			additionalKeys = false
		}
	}

	return returnValue, nil
}
