var mongo = require('mongojs');
var MongoClient = require('mongodb').MongoClient;
var job = require('./job');
var Queue = require('./queue');
var Worker = require('./worker');

module.exports = Connection;

/**
* @constructor
* @param {string} uri - MongoDB connection string
* @param {Object} options - connection options
*/
function Connection(uri, options) {
    this.db = mongo(uri, [], options);
    this.uri = uri;
    this.options = options || {};
    this._nativeClient = null;
    this._nativeDb = null;
}

/**
* Returns a new {@link Worker}
* @param {string[]|string} queues - list of queue names, a single queue name, or '*' for a universal worker
* @param {Object} options - an object with worker options
*/
Connection.prototype.worker = function (queues, options) {
    var self = this;

    options || (options = {});

    var collection = options.collection || 'jobs';

    if (queues === "*") {
        options.universal = true;

        queues = [self.queue('*', {
            universal: true,
            collection: collection
        })];
    } else {
        if (!Array.isArray(queues)) {
            queues = [queues];
        }

        var queues = queues.map(function (queue) {
            if (typeof queue === 'string') {
                queue = self.queue(queue, {
                    collection: collection
                });
            }

            return queue;
        });
    }

    return new Worker(queues, options);
};

Connection.prototype.queue = function (name, options) {
    return new Queue(this, name, options);
};

/**
* Gets or creates the native MongoDB client for change streams
* @param {Function} callback - Optional callback with (err, client, db). If omitted, returns a Promise
* @returns {Promise} Promise resolving to [client, db] if no callback provided
*/
Connection.prototype.getNativeClient = function (callback) {
    var self = this;

    // If we already have a client, reuse it
    // MongoDB client handles reconnection automatically, so we don't need to check connection state
    if (this._nativeClient && this._nativeDb) {
        var result = Promise.resolve([self._nativeClient, self._nativeDb]);
        if (callback) {
            result.then(function (res) {
                callback(null, res[0], res[1]);
            }).catch(callback);
            return;
        }
        return result;
    }

    // Create new client connection
    // MongoDB Driver v6.x: connect() returns a Promise, callback is no longer supported
    var promise = MongoClient.connect(this.uri, this.options)
        .then(function (client) {
            self._nativeClient = client;

            // Extract database name from connection string to ensure we use the correct database
            // MongoDB connection string formats:
            //   mongodb://[username:password@]host[:port][/database][?options]
            //   mongodb+srv://[username:password@]host[/database][?options]
            var dbName = null;
            try {
                // Handle both mongodb:// and mongodb+srv:// formats
                var uri = self.uri;
                var dbPart = null;

                // Check for mongodb+srv:// format
                if (uri.indexOf('mongodb+srv://') === 0) {
                    // mongodb+srv://user:pass@cluster.mongodb.net/database?options
                    // Find the database part after the last '/' before '?'
                    var srvMatch = uri.match(/mongodb\+srv:\/\/[^\/]+\/([^?]+)/);
                    if (srvMatch && srvMatch[1]) {
                        dbPart = srvMatch[1];
                    }
                } else if (uri.indexOf('mongodb://') === 0) {
                    // mongodb://user:pass@host:port/database?options
                    // More robust parsing: find database after the host part
                    // Format: mongodb://[user:pass@]host[:port]/database[?options]
                    var standardMatch = uri.match(/mongodb:\/\/[^\/]+\/([^?]+)/);
                    if (standardMatch && standardMatch[1]) {
                        dbPart = standardMatch[1];
                    }
                }

                // Validate database name (should not be empty and should be valid)
                if (dbPart && dbPart.length > 0 && dbPart.trim().length > 0) {
                    // Remove any trailing slashes or whitespace
                    dbName = dbPart.trim().replace(/\/+$/, '');
                }
            } catch (e) {
                // If parsing fails, fall back to default behavior
                // This handles edge cases like malformed URIs or special characters
            }

            // Use explicit database name if found, otherwise use default from connection
            if (dbName) {
                self._nativeDb = client.db(dbName);
            } else {
                // Fall back to default database (usually 'test' or from connection string)
                self._nativeDb = client.db();
            }

            return [self._nativeClient, self._nativeDb];
        });

    if (callback) {
        promise.then(function (result) {
            callback(null, result[0], result[1]);
        }).catch(callback);
        return;
    }

    return promise;
};

Connection.prototype.close = function () {
    var self = this;

    if (this._nativeClient) {
        this._nativeClient.close(function () {
            self.db.close();
        });
    } else {
        this.db.close();
    }
};
