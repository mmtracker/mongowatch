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

package stream

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var mongoTestsDB = &mongo.Database{}

// TestMain always runs unit tests. Integration tests opt in only through an
// authenticated loopback mm_compat sandbox, never through application settings.
func TestMain(m *testing.M) {
	uri := os.Getenv("MONGOWATCH_TEST_URI")
	if uri == "" {
		os.Exit(m.Run())
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme != "mongodb" || parsed.Hostname() != "127.0.0.1" || parsed.User == nil || parsed.Query().Get("replicaSet") != "mm_compat" {
		fmt.Fprintln(os.Stderr, "integration tests require an authenticated loopback mm_compat sandbox")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true, ObjectIDAsHexString: true}))
	if err != nil {
		cancel()
		os.Exit(1)
	}
	var hello struct {
		SetName string `bson:"setName"`
	}
	err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
	cancel()
	if err != nil || hello.SetName != "mm_compat" {
		os.Exit(1)
	}
	mongoTestsDB = client.Database(fmt.Sprintf("mm_compat_mongowatch_%d", time.Now().UnixNano()))
	code := m.Run()
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := mongoTestsDB.Drop(cleanupCtx); err != nil {
		code = 1
	}
	if err := client.Disconnect(cleanupCtx); err != nil {
		code = 1
	}
	cleanupCancel()
	os.Exit(code)
}

func requireMongoIntegration(t *testing.T) {
	t.Helper()
	if mongoTestsDB.Client() == nil {
		t.Skip("requires explicitly configured MONGOWATCH_TEST_URI sandbox")
	}
}
