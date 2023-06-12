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

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/mmtracker/mongowatch"
)

// Manager manages the change stream
type Manager struct {
	resumeRepo            mongowatch.StreamResume
	watcher               mongowatch.ChangeStreamWatcher
	changeEventSaveFunc   mongowatch.ChangeEventDispatcherFunc
	changeEventDeleteFunc mongowatch.ChangeEventDispatcherFunc

	cancel context.CancelFunc
}

// NewManager creates a new change stream manager
func NewManager(
	resumeRepo mongowatch.StreamResume,
	watcher mongowatch.ChangeStreamWatcher,
	changeEventSaveFunc mongowatch.ChangeEventDispatcherFunc,
	changeEventDeleteFunc mongowatch.ChangeEventDispatcherFunc,
) *Manager {
	return &Manager{resumeRepo: resumeRepo, watcher: watcher, changeEventSaveFunc: changeEventSaveFunc, changeEventDeleteFunc: changeEventDeleteFunc}
}

// Watch starts the change stream manager
func (m *Manager) Watch(ctx context.Context, fullDocumentMode options.FullDocument, tm *primitive.Timestamp, fn ...mongowatch.ChangeEventDispatcherFunc) error {
	log.Tracef("manager.Watch")
	ctx, m.cancel = context.WithCancel(ctx)
	var err error
	if tm == nil {
		tm, err = m.resumeRepo.GetResumeTime()
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("failed to fetch mongo watcher resume token: %w", err)
		}
	}

	err = m.watcher.Start(ctx, fullDocumentMode, tm, m.changeEventSaveFunc, m.changeEventDeleteFunc, fn...)
	if err != nil {
		return fmt.Errorf("failed to watch mongo stream: %w", err)
	}

	return nil
}

// Stop stops the change stream manager
func (m *Manager) Stop() {
	if m.cancel == nil {
		log.Errorf("change stream manager stop called with no cancel")
		return
	}

	log.Trace("change stream manager stop called")
	m.cancel()
}
