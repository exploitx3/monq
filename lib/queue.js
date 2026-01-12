var mongo = require('mongojs');
var db = require('./db');
var Job = require('./job');

module.exports = Queue;

function Queue(connection, name, options) {
    if (typeof name === 'object' && options === undefined) {
        options = name;
        name = undefined;
    }

    options || (options = {});
    options.collection || (options.collection = 'jobs');
    options.universal || (options.universal = false);

    this.connection = connection;
    this.name = name || 'default';
    this.options = options;

    this.collection = this.connection.db.collection(this.options.collection);

    if (options.index !== false) {
        db.index(this.collection);
    }
}

Queue.prototype.job = function (data) {
    return new Job(this.collection, data);
};

Queue.prototype.get = function (id, callback) {
    var self = this;

    if (typeof id === 'string') {
        id = new mongo.ObjectID(id);
    }

    var query = { _id: id };
    if (!this.options.universal) {
        query.queue = this.name;
    }

    var promise = new Promise(function (resolve, reject) {
        self.collection.findOne(query, function (err, data) {
            if (err) return reject(err);

            var job = new Job(self.collection, data);
            resolve(job);
        });
    });

    if (callback) {
        promise.then(function (result) {
            callback(null, result);
        }).catch(callback);
        return;
    }

    return promise;
};

Queue.prototype.enqueue = function (name, params, options, callback) {
    if (!callback && typeof options === 'function') {
      callback = options;
      options = {};
    }

    var job = this.job({
        name: name,
        params: params,
        queue: this.name,
        attempts: parseAttempts(options.attempts),
        timeout: parseTimeout(options.timeout),
        delay: options.delay,
        priority: options.priority
    });

    return job.enqueue(callback);
};

/**
* Dequeues a job from the database by searching for matching jobs
* This is the original polling-based approach
* @param {Object} options - dequeue options (minPriority, callbacks)
* @param {Function} callback - callback with (err, job)
*/
Queue.prototype.dequeue = function (options, callback) {
    var self = this;

    // Handle different call signatures
    if (callback === undefined && typeof options === 'function') {
        callback = options;
        options = {};
    }

    // Ensure options is an object
    if (!options || typeof options !== 'object') {
        options = {};
    }

    var query = {
        status: Job.QUEUED,
        delay: { $lte: new Date() }
    };

    if (!this.options.universal) {
        query.queue = this.name;
    }

    if (options.minPriority !== undefined) {
        query.priority = { $gte: options.minPriority };
    }

    if (options.callbacks !== undefined) {
        var callback_names = Object.keys(options.callbacks);
        query.name = { $in: callback_names };
    }

    var sort = {
        'priority': -1,
        '_id': 1
    };

    var update = { $set: { status: Job.DEQUEUED, dequeued: new Date() }};

    var promise = new Promise(function (resolve, reject) {
        self.collection.findAndModify({
            query: query,
            sort: sort,
            update: update,
            new: true
        }, function (err, doc) {
            if (err) return reject(err);
            if (!doc) return resolve(null);

            resolve(self.job(doc));
        });
    });

    if (callback && typeof callback === 'function') {
        promise.then(function (result) {
            callback(null, result);
        }).catch(callback);
        return;
    }

    return promise;
};

/**
* Dequeues a specific job document from a change stream
* This function processes jobs directly from change stream events
* Atomically claims the job if it still matches criteria
* @param {Object} jobDocument - The job document from the change stream
* @param {Object} options - dequeue options (minPriority, callbacks)
* @param {Function} callback - callback with (err, job)
*/
Queue.prototype.dequeueFromChangeStream = function (jobDocument, options, callback) {
    var self = this;
    var Job = require('./job');

    // Validate the job document matches our criteria
    if (jobDocument.status !== Job.QUEUED) {
        var result = Promise.resolve(null);
        if (callback) {
            result.then(function (res) {
                callback(null, res);
            }).catch(callback);
        }
        return result;
    }

    // Check delay
    if (jobDocument.delay && new Date(jobDocument.delay) > new Date()) {
        var result = Promise.resolve(null);
        if (callback) {
            result.then(function (res) {
                callback(null, res);
            }).catch(callback);
        }
        return result;
    }

    // Check queue name (if not universal)
    if (!this.options.universal && jobDocument.queue !== this.name) {
        var result = Promise.resolve(null);
        if (callback) {
            result.then(function (res) {
                callback(null, res);
            }).catch(callback);
        }
        return result;
    }

    // Check minPriority
    if (options.minPriority !== undefined) {
        var priority = jobDocument.priority || 0;
        if (priority < options.minPriority) {
            var result = Promise.resolve(null);
            if (callback) {
                result.then(function (res) {
                    callback(null, res);
                }).catch(callback);
            }
            return result;
        }
    }

    // Check callbacks
    if (options.callbacks !== undefined) {
        var callback_names = Object.keys(options.callbacks);
        if (callback_names.indexOf(jobDocument.name) === -1) {
            var result = Promise.resolve(null);
            if (callback) {
                result.then(function (res) {
                    callback(null, res);
                }).catch(callback);
            }
            return result;
        }
    }

    // Build query to atomically claim this specific job
    var query = {
        _id: jobDocument._id,
        status: Job.QUEUED // Only claim if still queued (atomic check)
    };

    var update = { $set: { status: Job.DEQUEUED, dequeued: new Date() }};

    // Atomically claim the job
    var promise = new Promise(function (resolve, reject) {
        self.collection.findAndModify({
            query: query,
            update: update,
            new: true
        }, function (err, doc) {
            if (err) return reject(err);
            if (!doc) {
                // Job was already claimed by another worker or no longer queued
                return resolve(null);
            }

            resolve(self.job(doc));
        });
    });

    if (callback) {
        promise.then(function (result) {
            callback(null, result);
        }).catch(callback);
        return;
    }

    return promise;
};

// Helpers

function parseTimeout(timeout) {
    if (timeout === undefined) return undefined;
    return parseInt(timeout, 10);
}

function parseAttempts(attempts) {
    if (attempts === undefined) return undefined;

    if (typeof attempts !== 'object') {
        throw new Error('attempts must be an object');
    }

    var result = {
        count: parseInt(attempts.count, 10)
    };

    if (attempts.delay !== undefined) {
        result.delay = parseInt(attempts.delay, 10);
        result.strategy = attempts.strategy;
    }

    return result;
}
