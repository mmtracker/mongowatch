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

package watchers

import (
	"context"
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
	mongoDriver "go.mongodb.org/mongo-driver/mongo"

	"github.com/mmtracker/mongowatch"
	"github.com/mmtracker/mongowatch/db/tx"
)

// CollectionStruct refers to structure stored in target collection
type CollectionStruct struct {
	SomePrimaryKey string `json:"some_primary_key,omitempty"`
}

// NewSomeCollectionWatcher creates a new any Collection watcher
func NewSomeCollectionWatcher(executor tx.Executor) *SomeCollectionWatcher {
	return &SomeCollectionWatcher{executor: executor}
}

// SomeCollectionWatcher is a watcher for SomeCollectionWatcher changes
type SomeCollectionWatcher struct {
	executor tx.Executor
}

var _ mongowatch.CollectionWatcher = (*SomeCollectionWatcher)(nil)

// CollectionStructWrapper is a wrapper for SomeCollectionWatcher on watcher side since mongo primary key is in "_id" column
type CollectionStructWrapper struct {
	CollectionStruct `json:",inline"`
	SomePrimaryKey   string `json:"_id"` // override SomePrimaryKey with _id json identifier to unmarshall mongo data properly
}

// Insert is called when a new document is inserted
func (s SomeCollectionWatcher) Insert(ctx context.Context, doc []byte) error {
	return s.Update(ctx, doc)
}

// Update is called when a document is updated
func (s SomeCollectionWatcher) Update(ctx context.Context, doc []byte) error {
	log.Tracef("processing collection change: %s", string(doc))

	collection := CollectionStructWrapper{}
	err := json.Unmarshal(doc, &collection)
	if err != nil {
		return fmt.Errorf("collection watcher update: failed to unmarshal collection: %w", err)
	}

	// TODO: use changed structure to update local DB state
	log.Infof("collection watcher changed entity: %s", collection.SomePrimaryKey)

	return err
}

// Delete is called when a document is deleted
func (s SomeCollectionWatcher) Delete(ctx context.Context, doc []byte) error {
	log.Tracef("processing collection delete: %s", string(doc))

	collection := CollectionStruct{}
	err := json.Unmarshal(doc, &collection)
	if err != nil {
		return fmt.Errorf("collection watcher delete: failed to unmarshal collection: %w", err)
	}

	err = s.executor.WithTransaction(func(sessCtx mongoDriver.SessionContext) (interface{}, error) {
		// TODO: delete some state using sessCtx from local DB
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("collection watcher delete: failed to delete device and reports: %w", err)
	}

	log.Infof("collection watcher deleted entity %s", collection.SomePrimaryKey)

	return nil
}
