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
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/mmtracker/mongowatch"
)

// Mock is a mock implementation of SomeCollectionWatcher
type Mock struct {
	Wg    *sync.WaitGroup
	Limit int

	Inserted  int
	InsertErr error
	Updated   int
	UpdateErr error
	Deleted   int
	DeleteErr error
}

var _ mongowatch.CollectionWatcher = (*Mock)(nil)

// Insert mock
func (m *Mock) Insert(ctx context.Context, doc []byte) error {
	log.Tracef("processing mock insert: %d: %s", m.Inserted+1, string(doc))
	m.Inserted++
	m.Wg.Done()
	return m.InsertErr
}

// Update mock
func (m *Mock) Update(ctx context.Context, doc []byte) error {
	log.Tracef("processing mock update: %d: %s", m.Updated+1, string(doc))
	m.Updated++
	m.Wg.Done()
	return m.UpdateErr
}

// Delete mock
func (m *Mock) Delete(ctx context.Context, doc []byte) error {
	log.Tracef("processing mock delete: %d: %s", m.Deleted+1, string(doc))
	m.Deleted++
	m.Wg.Done()
	return m.DeleteErr
}
