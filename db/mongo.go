// Description: helper to connect and setup  mongo DB

/*
 * Copyright (c) 2023. Monimoto Authors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package db

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func mongoClientOptions(uri string) *options.ClientOptions {
	return options.Client().SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true, ObjectIDAsHexString: true}).ApplyURI(uri)
}

// ConnectToMongo helper to connect and setup  mongo DB
func ConnectToMongo(dbName string, connectURL string) *mongo.Database {
	log.Info("connecting to MongoDB")

	// Create MongoDB client
	opts := mongoClientOptions(connectURL)
	opts.SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(opts)
	if err != nil {
		log.Fatalf("failed to create new MongoDB client: %#v", err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatalf("failed to ping MongoDB: %#v", err)
	}

	log.Info("mongo connection established")

	// Get collection from database
	return client.Database(dbName)
}

// Truncate collection records and indexes
func Truncate(col *mongo.Collection, dropIndexes bool) error {
	_, err := col.DeleteMany(context.Background(), bson.M{})
	if err != nil {
		return fmt.Errorf("truncate failed to delete all docs from %s: %w", col.Name(), err)
	}

	if dropIndexes {
		err = col.Indexes().DropAll(context.Background())
		if err != nil {
			return fmt.Errorf("truncate failed to drop indexes on %s: %w", col.Name(), err)
		}
	}

	return nil
}
