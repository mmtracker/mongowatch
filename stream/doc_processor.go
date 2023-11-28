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
	"errors"
	"fmt"

	"github.com/cenkalti/backoff/v4"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/mmtracker/mongowatch"
)

// DocumentProcessor is a wrapper around the mongo change stream watcher
// simplifies the usage of the stream manager by marshaling the internal mongo structure to JSON
// also exposing two functions for handling document changes and deletions
// this way handlers can flexibly unmarshal docs into their own structs
type DocumentProcessor struct {
	manager    *Manager
	resumeRepo mongowatch.StreamResume
}

var _ mongowatch.DocumentProcessor = (*DocumentProcessor)(nil)

// NewDataProcessor creates a new DocumentProcessor
func NewDataProcessor(targetDB *mongo.Database, targetCollectionName string, resumeSuffix string, localDB *mongo.Database) *DocumentProcessor {
	resumeRepo := NewStreamResumeRepository(NewCollection(
		targetCollectionName+resumeSuffix,
		localDB,
	))

	return &DocumentProcessor{
		resumeRepo: resumeRepo,
		manager: NewManager(
			resumeRepo,
			NewChangeStreamWatcher(NewCollection(targetCollectionName, targetDB)),
			GetSaveResumePointFunc(resumeRepo),
			GetDeleteResumePointFunc(resumeRepo),
		),
	}
}

// StartWithRetry starts the doc processor with a retry mechanism
func (dp DocumentProcessor) StartWithRetry(bo backoff.BackOff, actions mongowatch.CollectionWatcher, fullDocumentMode options.FullDocument) error {
	op := func() error {
		err := dp.Start(actions, fullDocumentMode)
		if err != nil {
			if errors.Is(err, ErrInvalidate) {
				// gracefully stop the stream manager
				log.Tracef("stopping data processor due to invalidate event: %v", err)
				log.Trace("restarting...")
				dp.Stop()
			}
			log.Errorf("error while starting data processor: %v", err)
		}
		// TODO: increase error metrics to trigger notification to slack from victoria metrics via grafana
		return err
	}

	// use exponential backoff not to spam the logs, implement notify on slack if some key error occurs
	return backoff.Retry(op, bo)
}

// Start starts the doc processor
func (dp DocumentProcessor) Start(actions mongowatch.CollectionWatcher, fullDocumentMode options.FullDocument) error {
	resumePoint, err := dp.resumeRepo.GetResumePoint()
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("failed to fetch mongo watcher resume token: %w", err)
		}
	}

	// skip initial error
	// stream manager supports running multiple callbacks which can share errors
	// we don't need it here because 1 op = 1 callback
	var changeEventDispatcherFunc mongowatch.ChangeEventDispatcherFunc = func(ctx context.Context, ce mongowatch.ChangeStreamEvent, _ error) error {
		log.Tracef("processing event: %d: %s", ce.Timestamp.T, ce.OperationType)

		// TODO: maybe ce.FullDocument can be serialized into a struct directly
		// easiest way to remap the document to a struct is with JSON marshalling
		var docBytes []byte
		var err error
		if ce.OperationType == "insert" {
			docBytes, err = json.Marshal(ce.FullDocument)
			if err != nil {
				return fmt.Errorf("failed to marshal event stream document: %w", err)
			}
			return actions.Insert(ctx, docBytes)
		}
		if ce.OperationType == "update" {
			docBytes, err = json.Marshal(ce.FullDocument)
			if err != nil {
				return fmt.Errorf("failed to marshal event stream document: %w", err)
			}
			return actions.Update(ctx, docBytes)
		}
		if ce.OperationType == "delete" {
			if ce.FullDocumentBeforeChange != nil {
				docBytes, err = json.Marshal(ce.FullDocumentBeforeChange)
				if err != nil {
					return fmt.Errorf("failed to marshal event stream document before change: %w", err)
				}
			} else {
				docBytes, err = json.Marshal(ce.FullDocument)
				if err != nil {
					return fmt.Errorf("failed to marshal event stream document: %w", err)
				}
			}
			return actions.Delete(ctx, docBytes)
		}

		log.Tracef("skipping event: %d: %s", ce.Timestamp.T, ce.OperationType)

		return nil
	}

	// start watching the change stream
	return dp.manager.Watch(context.Background(), fullDocumentMode, resumePoint, changeEventDispatcherFunc)
}

// Stop stops the doc processor
func (dp DocumentProcessor) Stop() {
	dp.manager.Stop()
}
