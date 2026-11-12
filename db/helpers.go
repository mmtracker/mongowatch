/*
 * Copyright (c) 2026. Monimoto Authors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package db

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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
