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
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
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
func (csw *ChangeStreamWatcher) Start(
	ctx context.Context,
	fullDocumentMode options.FullDocument,
	resumePoint *mongowatch.ChangeStreamResumePoint,
	saveFunc, deleteFunc mongowatch.ChangeEventDispatcherFunc,
	dispatchFuncs ...mongowatch.ChangeEventDispatcherFunc,
) error {
	return csw.startWatcher(ctx, fullDocumentMode, resumePoint, saveFunc, deleteFunc, dispatchFuncs)
}

func (csw *ChangeStreamWatcher) startWatcher(ctx context.Context, fullDocumentMode options.FullDocument, resumePoint *mongowatch.ChangeStreamResumePoint, saveFunc mongowatch.ChangeEventDispatcherFunc, deleteFunc mongowatch.ChangeEventDispatcherFunc, dispatchFuncs []mongowatch.ChangeEventDispatcherFunc) error {
	// we start a loop here to be able to restart the watcher on invalidate events
	watchCursor, err := csw.getWatchCursor(ctx, fullDocumentMode, resumePoint)
	if err != nil {
		return err
	}
	err = csw.watchChangeStream(
		ctx,
		resumePoint,
		saveFunc,
		deleteFunc,
		watchCursor,
		dispatchFuncs,
	)
	if err != nil {
		if errors.Is(err, ErrInvalidate) {
			log.Tracef("received 'invalidate' event, restarting watcher")
			// time.Sleep(10000 * time.Millisecond)
			// continue
		}
		return fmt.Errorf("failed to watch change stream: %w", err)
	}

	return nil
}

func (csw *ChangeStreamWatcher) getWatchCursor(ctx context.Context, fullDocumentMode options.FullDocument, resumePoint *mongowatch.ChangeStreamResumePoint) (*mongo.ChangeStream, error) {
	opts := options.ChangeStream()
	opts.SetFullDocument(options.UpdateLookup)
	opts.SetFullDocumentBeforeChange(options.Required)

	// since we don't store the resume point if it's the invalidate event
	// we have to start from the next event
	// but this fails, because the next event is the invalidate event
	if resumePoint != nil {
		log.Tracef("starting watcher from resume point for op: %s", resumePoint.OperationType)
		if resumePoint.OperationType == mongowatch.OperationTypeInvalidate {
			log.Tracef("starting watcher after resume point because of invalidate event: %s", resumePoint.ID)
			opts.SetStartAfter(resumePoint.ID)
		} else {
			log.Tracef("starting watcher from timestamp: %d in mode: %s", resumePoint.Timestamp, fullDocumentMode)
			opts.SetStartAtOperationTime(&resumePoint.Timestamp)
		}
	} else {
		log.Tracef("starting watcher without timestamp")
	}

	watchCursor, err := csw.col.Watch(ctx, buildPipeline(), opts)
	if err != nil {
		if strings.Contains(err.Error(), "NoMatchingDocument") {
			log.Errorf("NoMatchingDocument, falling back to fullDocumentMode options.Off: %s", err.Error())
			opts.SetFullDocumentBeforeChange(options.Off)
			watchCursor, err = csw.col.Watch(ctx, buildPipeline(), opts)
			if err != nil {
				return nil, fmt.Errorf("failed to watch collection: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to watch collection: %w", err)
		}
	}

	log.Tracef("getWatchCursor: watch cursor: %+v", watchCursor.ResumeToken())

	return watchCursor, nil
}

var ErrInvalidate = fmt.Errorf("received 'invalidate' event")

func (csw *ChangeStreamWatcher) watchChangeStream(ctx context.Context, resumeToken *mongowatch.ChangeStreamResumePoint, saveFunc mongowatch.ChangeEventDispatcherFunc, deleteFunc mongowatch.ChangeEventDispatcherFunc, watchCursor *mongo.ChangeStream, dispatchFuncs []mongowatch.ChangeEventDispatcherFunc) error {
	defer watchCursor.Close(ctx)

	log.Trace("mongo stream watcher launched, waiting for change events...")

	var previousEvent *mongowatch.ChangeStreamEvent
	// wait for the next change stream data to become available
	for watchCursor.Next(ctx) {
		// log.Tracef("received change event: %+v", watchCursor.Current)
		changeEvent, err := csw.extractChangeEvent(watchCursor.Current)
		if err != nil {
			return fmt.Errorf("failed to extract change event: %w", err)
		}
		// log.Tracef("extracted change event: %+v", changeEvent)

		// attempting to do the following here will fail
		// if changeEvent.OperationType == mongowatch.OperationTypeInvalidate return ErrInvalidate
		// the error will put the watcher into an infinite restart loop
		// after the first restart we should continue and wait for the watchCursor.Next(ctx) to return
		// but that's more difficult to implement

		// when we resume we already have the last event stored
		// so all we need to do is process
		// we will leave the deletion to the next event, so we have a point to resume from
		if previousEvent == nil && resumeToken != nil {
			log.Tracef("resuming watcher with no previous event: %+v", changeEvent)
			for _, dispatchFunc := range dispatchFuncs {
				// we pass the previous error to the next handler
				// this way the last handler can do a cleanup
				err = dispatchFunc(ctx, changeEvent, err)
			}
			if err != nil {
				return fmt.Errorf("failed to process first event: %w", err)
			}
			log.Tracef("resumed watcher from no event: %s", changeEvent.ID)

			// watchCursor was started with an invalidate event
			// we need to return the error to restart the watcher
			if changeEvent.OperationType == mongowatch.OperationTypeInvalidate {
				log.Tracef("received 'invalidate' event for: %s", changeEvent.Collection)
				log.Tracef("returning error to restart the watcher and resume the next event from: %s", changeEvent.ID)

				return ErrInvalidate
			}

			// consider this event processed
			previousEvent = &changeEvent
			continue
		}

		// save event
		err = saveFunc(ctx, changeEvent, nil)
		if err != nil {
			return fmt.Errorf("failed to save event: %w", err)
		}

		log.Tracef("saved event: %s", changeEvent.ID)

		// the very first run (before we have events stored) will have previousEvent nil
		if previousEvent != nil {
			// we saved the current event and keep it for resumption
			// we delete the previous event since we don't have to return to it
			err = deleteFunc(ctx, *previousEvent, nil)
			if err != nil {
				return fmt.Errorf("failed to delete event: %w", err)
			}
			log.Tracef("deleted event: %s", previousEvent.ID)
		}

		// once the current event is stored and the previous event is deleted
		// we can continue processing the current event since even if it fails we can resume from here
		for _, dispatchFunc := range dispatchFuncs {
			// we pass the previous error to the next handler
			// this way the last handler can do a cleanup
			err = dispatchFunc(ctx, changeEvent, err)
		}
		if err != nil {
			return fmt.Errorf("failed to process event: %w", err)
		}

		log.Tracef("processed event: %s", changeEvent.ID)

		// 2nd case
		if changeEvent.OperationType == mongowatch.OperationTypeInvalidate {
			log.Tracef("received 'invalidate' event for: %s", changeEvent.Collection)
			log.Tracef("returning error to restart the watcher and resume the next event from: %s", changeEvent.ID)
			return ErrInvalidate
		}

		previousEvent = &changeEvent
	}

	return nil
}

// extractChangeEvent transforms the raw data received from the MongoDB change stream to the ChangeStreamEvent type.
func (csw *ChangeStreamWatcher) extractChangeEvent(rawChange bson.Raw) (mongowatch.ChangeStreamEvent, error) {
	// log.Tracef("received change event: %s", rawChange)
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
					{Key: "fullDocumentBeforeChange", Value: 1},
					{Key: "updateDescription", Value: 1},
				},
			},
		},
	}

	return pipeline
}
