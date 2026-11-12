/*
 * Copyright (c) 2026. Monimoto Authors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package stream

import (
	"encoding/json"
	"testing"

	"github.com/mmtracker/mongowatch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestExtractChangeEventPreservesLegacyNestedMaps(t *testing.T) {
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "resume-token"}}},
		{Key: "timestamp", Value: bson.Timestamp{T: 100, I: 3}},
		{Key: "operationType", Value: "delete"}, {Key: "documentKey", Value: "device"},
		{Key: "fullDocumentBeforeChange", Value: bson.M{"nested": bson.M{"enabled": true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	watcher := &ChangeStreamWatcher{}
	event, err := watcher.extractChangeEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	nested, ok := event.FullDocumentBeforeChange["nested"].(bson.M)
	if !ok || nested["enabled"] != true {
		t.Fatalf("legacy nested map contract changed: %#v", event.FullDocumentBeforeChange)
	}
	if event.ID.TokenData != "resume-token" || event.Timestamp != (bson.Timestamp{T: 100, I: 3}) {
		t.Fatal("resume metadata changed")
	}
	encoded, err := json.Marshal(event.FullDocumentBeforeChange)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"nested":{"enabled":true}}` {
		t.Fatalf("watcher JSON shape changed: %s", encoded)
	}
}

func TestResumePointBSONContractIsUnchanged(t *testing.T) {
	point := mongowatch.ChangeStreamResumePoint{ID: mongowatch.ResumeToken{TokenData: "saved-token"},
		Timestamp: bson.Timestamp{T: 101, I: 7}, OperationType: "delete"}
	encoded, err := bson.Marshal(point)
	if err != nil {
		t.Fatal(err)
	}
	raw := bson.Raw(encoded)
	if raw.Lookup("_id").Document().Lookup("_data").StringValue() != "saved-token" {
		t.Fatal("stored token field changed")
	}
	seconds, increment := raw.Lookup("timestamp").Timestamp()
	if seconds != 101 || increment != 7 {
		t.Fatal("stored timestamp BSON type or value changed")
	}
	var restored mongowatch.ChangeStreamResumePoint
	if err := bson.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.ID.TokenData != point.ID.TokenData || restored.Timestamp != point.Timestamp || restored.OperationType != point.OperationType {
		t.Fatal("persisted resume point did not round-trip")
	}
}

func TestExtractChangeEventRejectsMalformedBSON(t *testing.T) {
	watcher := &ChangeStreamWatcher{}
	if _, err := watcher.extractChangeEvent(bson.Raw{1, 2, 3}); err == nil {
		t.Fatal("malformed event was silently accepted")
	}
}
