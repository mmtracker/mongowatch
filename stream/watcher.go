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

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/mmtracker/mongowatch"
)

// NewCollection returns a new collection
func NewCollection(col string, mongoInstance *mongo.Database) *mongo.Collection {
	collection := mongoInstance.Collection(col,
		options.Collection().SetWriteConcern(writeconcern.New(writeconcern.W(1), writeconcern.J(false))),
	)
	return collection
}

// ChangeStreamWatcher watches a mongo change stream for change events and reacts to those events.
type ChangeStreamWatcher struct {
	col *mongo.Collection
}

// NewChangeStreamWatcher builds a new mongo watcher instance
func NewChangeStreamWatcher(col *mongo.Collection) *ChangeStreamWatcher {
	return &ChangeStreamWatcher{col: col}
}

var _ mongowatch.ChangeStreamWatcher = (*ChangeStreamWatcher)(nil)

// Start starts watching mongo's change stream for the collection and
// if a valid timestamp is provided, the stream starts from that point
// it processes events synchronously
func (csw *ChangeStreamWatcher) Start(ctx context.Context, fullDocumentMode options.FullDocument, timestamp *primitive.Timestamp, saveFunc, deleteFunc mongowatch.ChangeEventDispatcherFunc, dispatchFuncs ...mongowatch.ChangeEventDispatcherFunc) error {
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if timestamp != nil {
		log.Tracef("starting watcher from timestamp: %d in mode: %s", timestamp.T, fullDocumentMode)
		opts.SetStartAtOperationTime(timestamp)
		opts.SetFullDocumentBeforeChange(fullDocumentMode)
	} else {
		log.Tracef("starting watcher without timestamp")
	}

	watchCursor, err := csw.col.Watch(ctx, buildPipeline(), opts)
	if err != nil {
		return fmt.Errorf("failed to watch collection: %w", err)
	}

	return csw.watchChangeStream(ctx, timestamp != nil, saveFunc, deleteFunc, watchCursor, dispatchFuncs)
}

func (csw *ChangeStreamWatcher) watchChangeStream(
	ctx context.Context,
	resuming bool,
	saveFunc mongowatch.ChangeEventDispatcherFunc,
	deleteFunc mongowatch.ChangeEventDispatcherFunc,
	watchCursor *mongo.ChangeStream,
	dispatchFuncs []mongowatch.ChangeEventDispatcherFunc,
) error {
	defer watchCursor.Close(ctx)

	log.Trace("mongo stream watcher launched, waiting for change events...")

	var previousEvent *mongowatch.ChangeStreamEvent
	// wait for the next change stream data to become available
	for watchCursor.Next(ctx) {
		event, err := csw.extractChangeEvent(watchCursor.Current)
		if err != nil {
			return fmt.Errorf("failed to extract change event: %w", err)
		}
		// when we resume we already have the last event stored
		// so all we need to do is process
		// we will leave the deletion to the next event, so we have a point to resume from
		if previousEvent == nil && resuming {
			for _, dispatchFunc := range dispatchFuncs {
				// we pass the previous error to the next handler
				// this way the last handler can do a cleanup
				err = dispatchFunc(ctx, event, err)
			}
			if err != nil {
				return fmt.Errorf("failed to process first event: %w", err)
			}

			// consider this event processed
			previousEvent = &event
			continue
		}

		// save event
		err = saveFunc(ctx, event, nil)
		if err != nil {
			return fmt.Errorf("failed to save event: %w", err)
		}

		// the very first run (before we have events stored) will have previousEvent nil
		if previousEvent != nil {
			// we saved the current event and keep it for resumption
			// we delete the previous event since we don't have to return to it
			err = deleteFunc(ctx, *previousEvent, nil)
			if err != nil {
				return fmt.Errorf("failed to delete event: %w", err)
			}
		}

		// once the current event is stored and the previous event is deleted
		// we can continue processing the current event since even if it fails we can resume from here
		for _, dispatchFunc := range dispatchFuncs {
			// we pass the previous error to the next handler
			// this way the last handler can do a cleanup
			err = dispatchFunc(ctx, event, err)
		}
		if err != nil {
			return fmt.Errorf("failed to process event: %w", err)
		}

		previousEvent = &event
	}

	return nil
}

// extractChangeEvent transforms the raw data received from the MongoDB change stream to the ChangeStreamEvent type.
func (csw *ChangeStreamWatcher) extractChangeEvent(rawChange bson.Raw) (mongowatch.ChangeStreamEvent, error) {
	log.Tracef("received change event: %s", rawChange)
	var ce mongowatch.ChangeStreamEvent
	err := bson.Unmarshal(rawChange, &ce)
	if err != nil {
		return ce, fmt.Errorf("failed to unmarshal change event: %w", err)
	}
	log.Tracef("unmarshalled change event: %+v", ce)

	return ce, nil
}

// buildPipeline builds a MongoDB aggregation pipeline to reshape the change stream data received from MongoDB in
// the format of our change events. See mongowatch.ChangeStreamEvent.
func buildPipeline() mongo.Pipeline {
	pipeline := mongo.Pipeline{
		bson.D{
			{
				Key: "$match",
				Value: bson.D{
					{
						Key: "$or",
						Value: bson.A{
							// TODO: as far as I can tell these are ignored for some reason
							bson.D{{Key: "operationType", Value: "insert"}},
							bson.D{{Key: "operationType", Value: "update"}},
							bson.D{{Key: "operationType", Value: "delete"}},
							// invalidate is received when the watched collection is dropped or renamed
							// https://www.mongodb.com/docs/manual/reference/change-events/#invalidate-event
							// we should probably restart the watcher on it
							bson.D{{Key: "operationType", Value: "invalidate"}},
						},
					},
				},
			},
		},
		bson.D{
			{
				Key: "$addFields", Value: bson.D{
					{Key: "timestamp", Value: "$clusterTime"},
					{Key: "database", Value: "$ns.db"},
					{Key: "collection", Value: "$ns.coll"},
					{Key: "documentKey", Value: "$documentKey._id"},
				},
			},
		},
		bson.D{
			{
				Key: "$project", Value: bson.D{
					{Key: "timestamp", Value: 1},
					{Key: "operationType", Value: 1},
					{Key: "database", Value: 1},
					{Key: "collection", Value: 1},
					{Key: "documentKey", Value: 1},
					{Key: "fullDocument", Value: 1},
					{Key: "updateDescription", Value: 1},
				},
			},
		},
	}

	return pipeline
}
