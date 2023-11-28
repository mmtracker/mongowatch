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

	log "github.com/sirupsen/logrus"

	"github.com/mmtracker/mongowatch"
)

// GetSaveResumePointFunc returns a function that saves a resume point to our collection
func GetSaveResumePointFunc(streamResumeRepo mongowatch.StreamResume) mongowatch.ChangeEventDispatcherFunc {
	return func(ctx context.Context, cse mongowatch.ChangeStreamEvent, err error) error {
		// log.Tracef(
		//     "resume point of type %s for collection %s for database %s for record %v\n",
		//     cse.OperationType,
		//     cse.Collection,
		//     cse.Database,
		//     cse.DocumentKey,
		// )
		log.Tracef("saving resume point: %d", cse.Timestamp.T)
		point := mongowatch.ChangeStreamResumePoint{
			ID:            cse.ID,
			Timestamp:     cse.Timestamp,
			OperationType: cse.OperationType,
			FullDocument:  cse.FullDocument,
		}
		savePtErr := streamResumeRepo.SaveResumePoint(ctx, point)
		if savePtErr != nil {
			return fmt.Errorf("failed to save resume point %v: %w", cse.FullDocument, savePtErr)
		}

		return nil
	}
}

// GetDeleteResumePointFunc returns a function that deletes a resume point
func GetDeleteResumePointFunc(resumeTokenRepo mongowatch.StreamResume) mongowatch.ChangeEventDispatcherFunc {
	return func(ctx context.Context, ce mongowatch.ChangeStreamEvent, err error) error {
		if err != nil {
			log.Errorf("failed to delete resume point: %s", err.Error())
			return err
		}

		err = resumeTokenRepo.DeleteResumePoint(ctx, ce.ID)
		if err != nil {
			log.Errorf("failed to delete resume point ID %v: %v\n", ce.ID.TokenData, err)
			return err
		}
		log.Tracef("deleted resume point: %d", ce.Timestamp.T)

		return nil
	}
}
