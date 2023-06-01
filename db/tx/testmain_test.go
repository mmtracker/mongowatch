/*
 * Copyright (c) 2020.  Monimoto, UAB. All Rights Reserved.
 *
 * This software contains the intellectual property of Monimoto, UAB. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and conditions of the License Agreement under
 * which it is provided by or on behalf of Monimoto, UAB.
 */

package tx

import (
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
)

var mongoTestsDB = &mongo.Database{}

// If developing locally you should probably run mongo containers using docker compose.
// This way it will not attempt to start containers each time integrity test is being run.
func TestMain(m *testing.M) {
	// TODO: start mongo container or connect to existing DB
	// test.SetupMongoTestMain(m, "../..", mongoTestsDB)
}
