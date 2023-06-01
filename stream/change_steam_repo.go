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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/zolia/mongowatch"
)

// ResumeRepository stores metadata of mongo change events for resumption
type ResumeRepository struct {
	col *mongo.Collection
}

var _ mongowatch.StreamResume = (*ResumeRepository)(nil)

// NewStreamResumeRepository builds a new change stream repo instance
func NewStreamResumeRepository(col *mongo.Collection) *ResumeRepository {
	return &ResumeRepository{col: col}
}

// GetResumeTime returns the mongo stream timestamp for the last change stream event that was recorded
func (csr *ResumeRepository) GetResumeTime() (*primitive.Timestamp, error) {
	e, err := csr.GetLastResumePoint()
	if err != nil {
		return nil, err
	}

	return &e.Timestamp, nil
}

// GetResumeToken returns the mongo stream token for the last change stream event that was recorded
// This may be used to resume change events from the point of the last change event, meaning last event will be skipped.
func (csr *ResumeRepository) GetResumeToken() (*mongowatch.ResumeToken, error) {
	e, err := csr.GetLastResumePoint()
	if err != nil {
		return nil, err
	}

	return &e.ID, nil
}

// Count returns the total doc count
func (csr *ResumeRepository) Count() (int64, error) {
	cnt, err := csr.col.CountDocuments(context.Background(), bson.D{}, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to count resume points: %w", err)
	}

	return cnt, nil
}

// FetchAll returns all resume points
func (csr *ResumeRepository) FetchAll() ([]*mongowatch.ChangeStreamResumePoint, error) {
	cursor, err := csr.col.Find(context.Background(), bson.D{}, nil)
	if err != nil {
		return nil, err
	}

	var events []*mongowatch.ChangeStreamResumePoint
	if err = cursor.All(context.Background(), &events); err != nil {
		return nil, fmt.Errorf("failed cursor iteration for resumption points: %w", err)
	}

	return events, nil
}

// GetLastResumePoint returns the last resumption point
func (csr *ResumeRepository) GetLastResumePoint() (*mongowatch.ChangeStreamResumePoint, error) {
	var opts options.FindOneOptions
	opts.Sort = map[string]int{"timestamp": -1}
	ctx := context.Background()
	result := csr.col.FindOne(ctx, bson.D{}, &opts)

	var event *mongowatch.ChangeStreamResumePoint
	err := result.Decode(&event)
	if err != nil {
		return nil, fmt.Errorf("failed to find resume point: %w", err)
	}
	return event, nil
}

// DeleteResumePoint deletes a resumption point
func (csr *ResumeRepository) DeleteResumePoint(ctx context.Context, token mongowatch.ResumeToken) error {
	filter := bson.D{{Key: "_id", Value: token}}
	_, err := csr.col.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete resume point: %w", err)
	}
	return nil
}

// SaveResumePoint saves a resumption point
func (csr *ResumeRepository) SaveResumePoint(ctx context.Context, ce mongowatch.ChangeStreamResumePoint) error {
	filter := bson.D{{Key: "_id", Value: ce.ID}}
	update := bson.M{"$set": ce}
	_, err := csr.col.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("failed to save resume point: %w", err)
	}
	return nil
}
