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

package mongowatch

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ChangeStreamResumePoint holds information needed to resume a change stream from a specific point
type ChangeStreamResumePoint struct {
	ID        ResumeToken         `bson:"_id" json:"_id"`
	Timestamp primitive.Timestamp `bson:"timestamp" json:"timestamp"`
	// need to keep this for tests
	FullDocument primitive.M `bson:"fullDocument" json:"fullDocument"`
	// important to know before resuming the stream
	// OperationType == 'invalidate' means that the resume point is no longer valid,
	// and we need to use startAfter to resume the stream
	OperationType string `bson:"operationType" json:"operationType"`
}

const OperationTypeInvalidate = "invalidate"

// ChangeStreamEvent is the customized representation of a MongoDB change stream event that is captured and processed by
// this application.
type ChangeStreamEvent struct {
	ID            ResumeToken         `bson:"_id" json:"_id"`
	User          string              `bson:"user" json:"user"`
	Timestamp     primitive.Timestamp `bson:"timestamp" json:"timestamp"`
	OperationType string              `bson:"operationType" json:"operationType"`
	Database      string              `bson:"database" json:"database"`
	Collection    string              `bson:"collection" json:"collection"`
	// DocumentKey is the unique identifier for the document that was changed
	// (e.g. the _id field for a document)
	// some of our collections use custom IDs therefore it doesn't fit into the primitive.ObjectID type
	DocumentKey              string      `bson:"documentKey" json:"documentKey"`
	FullDocument             primitive.M `bson:"fullDocument" json:"fullDocument"`
	FullDocumentBeforeChange primitive.M `bson:"fullDocumentBeforeChange" json:"fullDocumentBeforeChange"`
	// TODO: get previous field values e.g. paidUntil
	UpdateDescription struct {
		UpdatedFields map[string]interface{} `bson:"updatedFields" json:"updatedFields"`
		RemovedFields interface{}            `bson:"removedFields" json:"removedFields"`
	} `bson:"updateDescription" json:"updateDescription"`
}

// IsInvalidate checks if the change stream event is an invalidate event.
func (cse ChangeStreamEvent) IsInvalidate() bool {
	return cse.OperationType == OperationTypeInvalidate
}

// ResumeToken denotes the token associated with a MongoDB change stream event, which may be used to resume receiving change stream events from
// a point in the past.
type ResumeToken struct {
	TokenData interface{} `bson:"_data" json:"_data"`
}
