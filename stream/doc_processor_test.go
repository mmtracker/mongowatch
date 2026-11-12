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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/mmtracker/mongowatch"
)

func Test_DocumentProcessor_Start(t *testing.T) {
	requireMongoIntegration(t)
	t.Run("data processing on the same connection with mock", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		const records = 5
		const resumeSuffix = "_resume"
		colName := "processor_" + bson.NewObjectID().Hex()
		col := NewCollection(colName, mongoTestsDB)
		resumeCol := NewCollection(colName+resumeSuffix, mongoTestsDB)
		if err := mongoTestsDB.CreateCollection(ctx, colName,
			options.CreateCollection().SetChangeStreamPreAndPostImages(bson.M{"enabled": true})); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			if err := col.Drop(cleanupCtx); err != nil {
				t.Error(err)
			}
			if err := resumeCol.Drop(cleanupCtx); err != nil {
				t.Error(err)
			}
		})

		processor := NewDataProcessor(mongoTestsDB, colName, resumeSuffix, mongoTestsDB)
		// Seed a timestamp so fixture writes cannot race the opening of the cursor.
		var hello struct {
			OperationTime bson.Timestamp `bson:"operationTime"`
		}
		if err := mongoTestsDB.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
			t.Fatal(err)
		}
		if hello.OperationTime.T == 0 {
			t.Fatal("sandbox did not return an operation timestamp")
		}
		if err := processor.resumeRepo.SaveResumePoint(ctx, mongowatch.ChangeStreamResumePoint{
			ID: mongowatch.ResumeToken{TokenData: "fixture-start"}, Timestamp: hello.OperationTime,
		}); err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{})
		processor.manager.watcher = &documentProcessorReadyWatcher{processor.manager.watcher, ready}
		callbacks := &documentProcessorCallbacks{events: make(chan documentProcessorEvent, records*3)}
		done := make(chan struct{})
		var startErr error
		go func() {
			startErr = processor.Start(callbacks, options.UpdateLookup)
			close(done)
		}()
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			// Joining the worker precedes collection cleanup and test completion.
			select {
			case <-ready:
				processor.Stop()
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("processor did not initialize or exit")
				return
			}
			select {
			case <-done:
				if startErr != nil {
					t.Errorf("processor exited: %v", startErr)
				}
			case <-time.After(5 * time.Second):
				t.Error("processor did not stop")
			}
		}
		t.Cleanup(stop)

		counts := map[string]int{}
		expect := func(operation string, id bson.ObjectID, value string) {
			t.Helper()
			select {
			case event := <-callbacks.events:
				var document struct {
					ID    string `json:"_id"`
					Value string `json:"test"`
				}
				if err := json.Unmarshal(event.document, &document); err != nil {
					t.Fatal(err)
				}
				if event.operation != operation || document.ID != id.Hex() || document.Value != value {
					t.Fatalf("unexpected %s callback: operation=%s document=%s", operation, event.operation, event.document)
				}
				counts[operation]++
			case <-done:
				t.Fatalf("processor exited before %s callback: %v", operation, startErr)
			case <-ctx.Done():
				t.Fatalf("wait for %s callback: %v", operation, ctx.Err())
			}
		}
		for range records {
			id := bson.NewObjectID()
			if _, err := col.InsertOne(ctx, bson.M{"_id": id, "test": "inserted"}); err != nil {
				t.Fatal(err)
			}
			expect("insert", id, "inserted")
			if _, err := col.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"test": "updated"}}); err != nil {
				t.Fatal(err)
			}
			expect("update", id, "updated")
			if _, err := col.DeleteOne(ctx, bson.M{"_id": id}); err != nil {
				t.Fatal(err)
			}
			expect("delete", id, "updated") // The deleted document must come from its pre-image.
		}
		stop()
		for _, operation := range []string{"insert", "update", "delete"} {
			if counts[operation] != records {
				t.Errorf("%s callbacks: got %d, want %d", operation, counts[operation], records)
			}
		}
		select {
		case event := <-callbacks.events:
			t.Errorf("unexpected extra %s callback", event.operation)
		default:
		}
	})
}

type documentProcessorEvent struct {
	operation string
	document  []byte
}

type documentProcessorCallbacks struct {
	events chan documentProcessorEvent
}

// Insert records a fixture insert callback.
func (c *documentProcessorCallbacks) Insert(ctx context.Context, document []byte) error {
	return c.record(ctx, "insert", document)
}

// Update records a fixture update callback.
func (c *documentProcessorCallbacks) Update(ctx context.Context, document []byte) error {
	return c.record(ctx, "update", document)
}

// Delete records a fixture delete callback.
func (c *documentProcessorCallbacks) Delete(ctx context.Context, document []byte) error {
	return c.record(ctx, "delete", document)
}

func (c *documentProcessorCallbacks) record(ctx context.Context, operation string, document []byte) error {
	select {
	case c.events <- documentProcessorEvent{operation, document}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("record %s callback: %w", operation, ctx.Err())
	}
}

type documentProcessorReadyWatcher struct {
	mongowatch.ChangeStreamWatcher
	ready chan struct{}
}

// Start signals that the manager has initialized cancellation before delegating to the real watcher.
func (w *documentProcessorReadyWatcher) Start(ctx context.Context, mode options.FullDocument,
	point *mongowatch.ChangeStreamResumePoint, save, remove mongowatch.ChangeEventDispatcherFunc,
	callbacks ...mongowatch.ChangeEventDispatcherFunc,
) error {
	close(w.ready)
	return w.ChangeStreamWatcher.Start(ctx, mode, point, save, remove, callbacks...)
}
