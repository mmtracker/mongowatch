/*
 * Copyright (c) 2022 Monimoto, UAB. All Rights Reserved.
 *
 *  This software contains the intellectual property of Monimoto, UAB. Use of
 *  this software and the intellectual property contained therein is expressly
 *  limited to the terms and conditions of the License Agreement under which
 *  it is provided by or on behalf of Monimoto, UAB.
 */

package tx

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestMongoExecutor_WithTransaction(t *testing.T) {
	e := &MongoExecutor{
		Client: mongoTestsDB.Client(),
	}

	callback := func(ctx mongo.SessionContext) (interface{}, error) {
		log.Info("db tx insert called")
		return nil, nil
	}

	err := e.WithTransaction(callback)
	if err != nil {
		log.Errorf("error: %v", err)
	}
}
