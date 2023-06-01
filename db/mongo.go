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
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ConnectToMongo helper to connect and setup  mongo DB
func ConnectToMongo(dbName string, connectURL string) *mongo.Database {
	log.Printf("connecting to MongoDB: %s", connectURL)

	// Create MongoDB client
	opts := options.Client().ApplyURI(connectURL)
	opts.SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.NewClient(opts)
	if err != nil {
		log.Fatalf("failed to create new MongoDB client: %#v", err)
	}

	// Connect client
	if err = client.Connect(context.Background()); err != nil {
		log.Fatalf("failed to connect to MongoDB: %#v", err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatalf("failed to ping MongoDB: %#v", err)
	}

	log.Info("mongo connection established")

	// Get collection from database
	return client.Database(dbName)
}
