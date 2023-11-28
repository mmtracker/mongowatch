package db

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// RecordPreImages enables pre/post images for a collection for MongoDB < 6
func RecordPreImages(mongoInstance *mongo.Database, colName string) error {
	cmd := bson.D{
		{Key: "collMod", Value: colName},
		{Key: "recordPreImages", Value: true},
	}
	result := bson.M{}
	err := mongoInstance.RunCommand(context.Background(), cmd).Decode(&result)
	if err != nil {
		return fmt.Errorf("failed to enable recording of pre and post images: %w", err)
	}

	log.Printf("recording of pre and post images enabled: %+v", result)
	return nil
}

// EnablePrePostImages enables pre/post images for a collection for MongoDB >= 6
func EnablePrePostImages(mongoInstance *mongo.Database, colName string) error {
	cmd := bson.D{
		{Key: "collMod", Value: colName},
		{Key: "changeStreamPreAndPostImages",
			Value: bson.D{{Key: "enabled", Value: true}}},
	}
	result := bson.M{}
	err := mongoInstance.RunCommand(context.Background(), cmd).Decode(&result)
	if err != nil {
		return fmt.Errorf("failed to enable change stream pre and post images: %w", err)
	}

	log.Printf("change stream pre and post images enabled: %+v", result)
	return nil
}
