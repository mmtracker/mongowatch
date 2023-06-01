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

package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/zolia/mongowatch/db"
	"github.com/zolia/mongowatch/db/tx"
	"github.com/zolia/mongowatch/examples/watchers"
	"github.com/zolia/mongowatch/stream"
)

func main() {
	// where your local data is stored including resume point of target db event log
	// tune connection string to your needs
	localDB := db.ConnectToMongo("some_collection", "mongodb://local_db:27017")
	// target DB to watch for changes
	// tune connection string to your needs
	targetDB := db.ConnectToMongo("target_db_to_watch", "mongodb://target_db:27017")
	processor := stream.NewDataProcessor(targetDB, "target_collection_to_watch", localDB)

	txExecutor := tx.NewMongoExecutor(localDB.Client())
	collectionWatcher := watchers.NewSomeCollectionWatcher(txExecutor)
	err := processor.Start(collectionWatcher)
	if err != nil {
		log.Fatalf("failed to start event stream processor: %v", err)
	}
}
