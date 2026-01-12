var events = require('events');
var util = require('util');

module.exports = Job;

/**
* Job retry specification
* @typedef {Object} Job~Attempts
* @property {string} strategy - Name of {@link Worker~strategyCallback} to use on retry
* @property {number} count - total number of attempts so far
* @property {number} delay - a delay constant for use in determining a delay.  In default linear strategy, this will be the delay between attempts
*/

/**
* @constructor
* @param {string} collection - The collection to save the job to
* @param {Object} data - The Job data
*/
function Job(collection, data) {
    this.collection = collection;

    if (data) {
        // Convert plain object to JobData type
        data.__proto__ = JobData.prototype;
        this.data = data;
    } else {
        this.data = new JobData();
    }
}

util.inherits(Job, events.EventEmitter);

Job.QUEUED = 'queued';
Job.DEQUEUED = 'dequeued';
Job.COMPLETE = 'complete';
Job.FAILED = 'failed';
Job.CANCELLED = 'cancelled';

Job.prototype.save = function (callback) {
    var self = this;

    // Support both Promise and callback APIs
    var promise = new Promise(function (resolve, reject) {
        self.collection.save(self.data, function (err, doc) {
            if (err) return reject(err);

            if (doc && self.data._id === undefined) {
                self.data._id = doc._id;
            }

            resolve(self);
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

Job.prototype.cancel = function (callback) {
    var self = this;

    if (this.data.status !== Job.QUEUED) {
        var error = new Error('Only queued jobs may be cancelled');
        if (callback) {
            return callback(error);
        }
        return Promise.reject(error);
    }

    this.data.status = Job.CANCELLED;
    this.data.ended = new Date();

    return this.save(callback);
};

Job.prototype.complete = function (result, callback) {
    this.data.status = Job.COMPLETE;
    this.data.ended = new Date();
    this.data.result = result;

    return this.save(callback);
};

Job.prototype.fail = function (err, callback) {
    this.data.status = Job.FAILED;
    this.data.ended = new Date();
    this.data.error = err.message;
    this.data.stack = err.stack;

    return this.save(callback);
};

Job.prototype.enqueue = function (callback) {
    if (this.data.delay === undefined) {
        this.data.delay = new Date();
    }

    if (this.data.priority === undefined) {
        this.data.priority = 0;
    }

    this.data.status = Job.QUEUED;
    this.data.enqueued = new Date();

    return this.save(callback);
};

Job.prototype.delay = function (delay, callback) {
    this.data.delay = new Date(new Date().getTime() + delay);

    return this.enqueue(callback);
};

function JobData() {}

Object.defineProperty(JobData.prototype, 'id', {
    get: function () {
        return this._id && this._id.toString && this._id.toString();
    }
});
