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
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/mmtracker/mongowatch/db"
	"github.com/mmtracker/mongowatch/examples/watchers"
)

func Test_DocumentProcessor_Start(t *testing.T) {
	tests := []struct {
		name    string
		want    interface{}
		wantErr error
	}{
		{
			name:    "data processing on the same connection with mock",
			wantErr: nil,
		},
	}

	const colName = "fake_sims"
	col := NewCollection(colName, mongoTestsDB)
	resumeCol := NewCollection(colName+"_resume_suffix_in_test", mongoTestsDB)
	db.Truncate(col, true)
	db.Truncate(resumeCol, true)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := 5
			actions := 3 // insert, update, delete
			wg := sync.WaitGroup{}
			wg.Add(records * actions)
			mock := watchers.Mock{Limit: records, Wg: &wg}
			dp := NewDataProcessor(mongoTestsDB, colName, "", mongoTestsDB)

			// start data processor in the bg
			go func() {
				err := dp.Start(&mock, "")
				if err != nil {
					log.Error(err)
				}
				assert.NoError(t, err)
			}()
			time.Sleep(time.Millisecond * 100)
			// insert records in the bg
			go func() {
				for i := 0; i < records; i++ {
					log.Tracef("inserting record: %d", i+1)
					res, err := col.InsertOne(context.Background(), map[string]interface{}{"test": "test"})
					assert.NoError(t, err)
					log.Tracef("inserted record: %s", res.InsertedID)
					filter := bson.D{{Key: "_id", Value: res.InsertedID}}

					log.Tracef("updating record: %d", i+1)
					update := bson.M{"$set": map[string]interface{}{"test": "testas"}}
					upRes, err := col.UpdateOne(context.Background(), filter, update)
					assert.NoError(t, err)
					log.Tracef("updated record: %#v", upRes)

					delRes, err := col.DeleteOne(context.Background(), filter)
					assert.NoError(t, err)
					log.Tracef("deleted record: %s: %#v", res.InsertedID, delRes)
				}

				log.Trace("inserted records")
			}()

			wg.Wait()

			assert.Equal(t, records, mock.Inserted)
			assert.Equal(t, records, mock.Updated)
			assert.Equal(t, records, mock.Deleted)
		})
	}
}
