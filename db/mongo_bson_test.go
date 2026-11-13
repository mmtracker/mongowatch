/*
 * Copyright (c) 2026. Monimoto Authors.
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
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

// TestMongoClientObjectIDStringDecoding checks the ordinary client independently of the stream's custom decoder.
func TestMongoClientObjectIDStringDecoding(t *testing.T) {
	id := bson.NewObjectID()
	for _, key := range []struct {
		name string
		id   any
		want string
	}{{"ObjectID", id, id.Hex()}, {"string", "existing-key", "existing-key"}} {
		for _, operation := range []string{"FindOne", "CursorAll"} {
			t.Run(key.name+"/"+operation, func(t *testing.T) {
				opts := mongoClientOptions("mongodb://127.0.0.1:1")
				deployment := drivertest.NewMockDeployment(bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{
					{Key: "id", Value: int64(0)}, {Key: "ns", Value: "compat.records"}, {Key: "firstBatch", Value: bson.A{
						bson.D{{Key: "_id", Value: key.id}, {Key: "native", Value: id}, {Key: "values", Value: bson.D{{Key: "_id", Value: id}}}},
					}},
				}}})
				opts.Deployment = deployment
				client, err := mongo.Connect(opts)
				if err != nil {
					t.Fatalf("construct offline client: %v", err)
				}
				t.Cleanup(func() {
					if err := client.Disconnect(context.Background()); err != nil {
						t.Errorf("disconnect offline client: %v", err)
					}
				})
				type record struct {
					ID     string        `bson:"_id"`
					Native bson.ObjectID `bson:"native"`
					Values any           `bson:"values"`
				}
				var decoded record
				collection := client.Database("compat").Collection("records")
				if operation == "FindOne" {
					err = collection.FindOne(context.Background(), bson.D{}).Decode(&decoded)
				} else {
					cursor, findErr := collection.Find(context.Background(), bson.D{})
					if findErr != nil {
						t.Fatalf("find offline documents: %v", findErr)
					}
					defer cursor.Close(context.Background())
					var rows []record
					err = cursor.All(context.Background(), &rows)
					if err == nil && len(rows) == 1 {
						decoded = rows[0]
					}
				}
				if err != nil {
					t.Fatalf("decode ID fixture: %v", err)
				}
				values, ok := decoded.Values.(bson.M)
				if decoded.ID != key.want || decoded.Native != id || !ok || values["_id"] != id {
					t.Fatal("string ID compatibility or native nested BSON types changed")
				}
			})
		}
	}
}
