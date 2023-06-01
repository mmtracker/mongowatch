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
