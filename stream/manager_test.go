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
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/mmtracker/mongowatch"
	"github.com/mmtracker/mongowatch/db"
)

func Test_Manager_ProcessesAndDeletesMessages_ExceptLast(t *testing.T) {
	watchManager, streamResumeRepo, watchableCollection, cleanup := buildManager()
	defer cleanup()

	const eventCount = 5
	wg := &sync.WaitGroup{}
	wg.Add(eventCount)

	insertDocumentsAsync(t, watchableCollection, eventCount, 0)
	runWatchAsync(watchManager, nil, handlerFunc(wg))

	log.Tracef("waiting")
	wg.Wait()
	log.Tracef("done waiting")
	watchManager.Stop()

	events := printResumePoints(streamResumeRepo)
	cnt, err := streamResumeRepo.Count()
	assert.NoError(t, err)
	log.Tracef("points in collection: %d", cnt)
	// last event stays
	assert.Equal(t, 1, int(cnt))
	assert.Equal(t, fmt.Sprintf("test_%d", eventCount-1), events[0].FullDocument["name"])
}

func Test_Manager_FailsOnError(t *testing.T) {
	watchManager, streamResumeRepo, watchableCollection, cleanup := buildManager()
	defer cleanup()

	const eventCount = 5
	const stopOn = 3
	stopOnName := fmt.Sprintf("test_%d", stopOn)

	wg := sync.WaitGroup{}
	wg.Add(stopOn + 1)
	insertDocumentsAsync(t, watchableCollection, eventCount, 0)
	runWatchAsync(watchManager, nil, func(ctx context.Context, ce mongowatch.ChangeStreamEvent, err error) error {
		wg.Done()
		name := ce.FullDocument["name"]
		log.Tracef("processing event: %s", name)
		if name == stopOnName {
			return fmt.Errorf("intended processing error on %s", name)
		}
		return nil
	})

	wg.Wait()
	log.Tracef("done waiting")
	watchManager.Stop()

	events := printResumePoints(streamResumeRepo)
	cnt, err := streamResumeRepo.Count()
	assert.NoError(t, err)
	log.Tracef("points in collection: %d", cnt)
	// last event stays
	assert.Equal(t, 1, int(cnt))
	assert.Equal(t, stopOnName, events[0].FullDocument["name"])
}

func Test_Manager_Resumes(t *testing.T) {
	watchManager, streamResumeRepo, watchableCollection, cleanup := buildManager()
	defer cleanup()

	const eventCount = 5
	wg := &sync.WaitGroup{}
	wg.Add(eventCount)

	insertDocumentsAsync(t, watchableCollection, eventCount, 0)
	runWatchAsync(watchManager, nil, handlerFunc(wg))

	log.Tracef("waiting")
	wg.Wait()
	log.Tracef("done waiting")
	watchManager.Stop()

	cnt, err := streamResumeRepo.Count()
	assert.NoError(t, err)
	log.Tracef("points in collection: %d", cnt)
	assert.Equal(t, 1, int(cnt)) // check that last event stays

	events := printResumePoints(streamResumeRepo)
	assert.Equal(t, fmt.Sprintf("test_%d", eventCount-1), events[0].FullDocument["name"])

	// end of first part of this test
	// we have successfully processed all events, but kept the last one
	wg = &sync.WaitGroup{}
	// HACK: +1 because otherwise it finishes right before the last event
	wg.Add(eventCount + 1)

	insertDocumentsAsync(t, watchableCollection, eventCount, eventCount)
	runWatchAsync(watchManager, nil, handlerFunc(wg))

	log.Tracef("waiting again")
	wg.Wait()
	// time.Sleep(time.Second)
	log.Tracef("done waiting")
	watchManager.Stop()

	cnt, err = streamResumeRepo.Count()
	assert.NoError(t, err)
	log.Tracef("points in collection: %d", cnt)
	assert.Equal(t, 1, int(cnt)) // last event stays

	events = printResumePoints(streamResumeRepo)
	assert.Equal(t, fmt.Sprintf("test_%d", eventCount*2-1), events[0].FullDocument["name"])
}

func Test_Manager_ResumesWithTimestamp(t *testing.T) {
	watchManager, streamResumeRepo, watchableCollection, cleanup := buildManager()
	defer cleanup()

	const eventCount = 5
	wg := &sync.WaitGroup{}
	wg.Add(eventCount)

	insertDocumentsAsync(t, watchableCollection, eventCount, 0)
	runWatchAsync(watchManager, nil, handlerFunc(wg))

	log.Tracef("waiting")
	wg.Wait()
	log.Tracef("done waiting")
	watchManager.Stop()

	cnt, err := streamResumeRepo.Count()
	assert.NoError(t, err)
	log.Tracef("points in collection: %d", cnt)
	assert.Equal(t, 1, int(cnt)) // check that last event stays

	events := printResumePoints(streamResumeRepo)
	assert.Equal(t, fmt.Sprintf("test_%d", eventCount-1), events[0].FullDocument["name"])

	// end of first part of this test
	// we have successfully processed all events, but kept the last one
	wg = &sync.WaitGroup{}
	// HACK: +1 because otherwise it finishes right before the last event
	wg.Add(eventCount + 1)

	insertDocumentsAsync(t, watchableCollection, eventCount, eventCount)
	runWatchAsync(watchManager, events[0], handlerFunc(wg))

	log.Tracef("waiting again")
	wg.Wait()
	log.Tracef("done waiting")
	watchManager.Stop()

	cnt, err = streamResumeRepo.Count()
	assert.NoError(t, err)
	log.Tracef("points in collection: %d", cnt)
	assert.Equal(t, 1, int(cnt)) // last event stays

	events = printResumePoints(streamResumeRepo)
	assert.Equal(t, fmt.Sprintf("test_%d", eventCount*2-1), events[0].FullDocument["name"])
}

func handlerFunc(wg *sync.WaitGroup) func(ctx context.Context, ce mongowatch.ChangeStreamEvent, err error) error {
	return func(ctx context.Context, ce mongowatch.ChangeStreamEvent, err error) error {
		wg.Done()
		name := ce.FullDocument["name"]
		log.Tracef("processed event: %s", name)
		return nil
	}
}

func runWatchAsync(watchManager *Manager, rp *mongowatch.ChangeStreamResumePoint, dispatcherFunc mongowatch.ChangeEventDispatcherFunc) {
	go func() {
		log.Tracef("starting watch in a routine")
		err := watchManager.Watch(context.Background(), options.Off, rp, dispatcherFunc)
		if err != nil {
			log.Errorf("watcher error: %s", err.Error())
		}

		log.Tracef("watcher finished")
	}()
}

func insertDocumentsAsync(t *testing.T, targetCollection *mongo.Collection, eventCount int, from int) {
	go func() {
		<-time.After(1 * time.Second)
		insertTestDocuments(t, targetCollection, eventCount, from)
	}()
}

func insertTestDocuments(t *testing.T, col *mongo.Collection, records int, from int) {
	log.Tracef("starting to insert %d records", records)
	for i := from; i < from+records; i++ {
		log.Tracef("inserting: %s", fmt.Sprintf("test_%d", i))
		_, err := col.InsertOne(context.Background(), map[string]string{
			"name": fmt.Sprintf("test_%d", i),
		})
		assert.NoError(t, err)
	}
}

func printResumePoints(changeRepo *ResumeRepository) []*mongowatch.ChangeStreamResumePoint {
	events, _ := changeRepo.FetchAll()
	for _, event := range events {
		log.Tracef("resume point in db: %v", event.FullDocument)
	}
	return events
}

func buildManager() (*Manager, *ResumeRepository, *mongo.Collection, func()) {
	watchableCollection := NewCollection("collection_to_watch", mongoTestsDB)
	resumeCollection := NewCollection("resume_points", mongoTestsDB)
	cleanup := func() {
		log.Tracef("truncated with: %s", db.Truncate(watchableCollection, false))
		log.Tracef("truncated with: %s", db.Truncate(resumeCollection, false))
	}
	cleanup()

	streamResumeRepo := NewStreamResumeRepository(resumeCollection)
	mongoWatcher := NewChangeStreamWatcher(watchableCollection)

	watchManager := NewManager(
		streamResumeRepo,
		mongoWatcher,
		GetSaveResumePointFunc(streamResumeRepo),
		GetDeleteResumePointFunc(streamResumeRepo),
	)
	return watchManager, streamResumeRepo, watchableCollection, cleanup
}
