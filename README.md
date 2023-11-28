# mongowatch

## MongoDB Event Stream Watcher

Watches target mongo collection using mongo event log and executes certain handlers based on subscribed collection changes.

## Limitations

The watcher hangs when it receives an 'invalidate' event.
If the collection was renamed, dropped or recreated, the watcher will not be able to recover.

Once the collection is recreated, make sure to reapply the collMod command to the collection to re-enable the event log.

This package contains helper methods to do it (make sure you have the right permissions):

#### For Mongo >= 6.0
`collMod` `changeStreamPreAndPostImages`

`db.EnablePrePostImages(mongoInstance *mongo.Database, colName string) error`

#### For Mongo < 6.0
`collMod` `recordPreImages`

`db.RecordPreImages(mongoInstance *mongo.Database, colName string) error`

### Package testing
To be able to run tests in this repo you will need to have some local and remote mongo instances running on port 27017.
Configure parts with TODO comments.

Courtesy of [@ignasbernotas](https://github.com/ignasbernotas) and [@zolia](https://github.com/zolia)
