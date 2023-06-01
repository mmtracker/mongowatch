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

package tx

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

// Executor database transaction executor
type Executor interface {
	WithTransaction(callback Callback) error
}

// MongoExecutor manages mongo transaction
type MongoExecutor struct {
	Client *mongo.Client
}

// NewMongoExecutor creates new MongoExecutor for transaction management
func NewMongoExecutor(client *mongo.Client) *MongoExecutor {
	return &MongoExecutor{Client: client}
}

// Callback describes callback accepted by session.WithTransaction
type Callback func(sessCtx mongo.SessionContext) (interface{}, error)

// WithTransaction execute callback within transaction
func (e *MongoExecutor) WithTransaction(callback Callback) error {
	// opts := options.Session().SetDefaultReadConcern(readconcern.Majority())
	session, err := e.Client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start mongo session: %w", err)
	}
	// TODO: tune according to your needs
	const duration = 10 * time.Second
	ctx, cancel := context.WithTimeout(context.TODO(), duration)
	defer cancel()

	defer session.EndSession(ctx)

	// txnOpts := options.Transaction().SetReadPreference(readpref.PrimaryPreferred())
	result, err := session.WithTransaction(ctx, callback)
	if err != nil {
		return fmt.Errorf("failed to execute transaction: %w", err)
	}

	log.Infof("tx successful with result: %v", result)
	return nil
}
