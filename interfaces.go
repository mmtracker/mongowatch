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

package mongowatch

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// StreamResume stores relevant change stream events
// mongo's oplog has configurable expiration, but we don't need a large oplog
// instead we store the changes we actually need
type StreamResume interface {
	// GetResumeToken fetches the last stored resume point and extracts the token
	GetResumeToken() (*ResumeToken, error)
	// GetResumeTime fetches the last stored resume point and extracts the timestamp
	GetResumeTime() (*primitive.Timestamp, error)
	// DeleteResumePoint deletes a change stream resume point from the collection
	DeleteResumePoint(ctx context.Context, token ResumeToken) error
	// SaveResumePoint stores ChangeStreamResumePoint
	SaveResumePoint(ctx context.Context, ce ChangeStreamResumePoint) error
}

// ChangeEventDispatcherFunc change event callback function
// returning err will stop further ChangeEventDispatcherFunc processing and the change stream watcher
type ChangeEventDispatcherFunc func(ctx context.Context, ce ChangeStreamEvent, err error) error

// ChangeStreamWatcher watches a change stream and dispatches received changed events
type ChangeStreamWatcher interface {
	// Start resumes watching change events and
	// passes event data to the supplied dispatch function for handling
	Start(ctx context.Context, fullDocumentMode options.FullDocument, timestamp *primitive.Timestamp, saveFunc, deleteFunc ChangeEventDispatcherFunc, dispatchFuncs ...ChangeEventDispatcherFunc) error
}

// CollectionWatcher is an interface for processing document data from a change stream
type CollectionWatcher interface {
	Update(ctx context.Context, doc []byte) error
	Insert(ctx context.Context, doc []byte) error
	Delete(ctx context.Context, doc []byte) error
}

// DocumentProcessor is an interface for processing document data from a change stream
type DocumentProcessor interface {
	StartWithRetry(bo backoff.BackOff, actions CollectionWatcher, fullDocumentMode options.FullDocument) error
	Start(actions CollectionWatcher, fullDocumentMode options.FullDocument) error
	Stop()
}
