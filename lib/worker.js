var events = require('events');
var util = require('util');
var uuid = require('uuid');
var Queue = require('./queue');

module.exports = Worker;

/**
* Options for a new worker
* @typedef {Object} Worker~Options
* @property {Number} interval - the polling interval for the worker. Note: The worker will process jobs, one at a time, as fast as possible while queues have waiting jobs
* @property {Worker~Strategies} strategies - {@link Worker~Strategies} for retrying jobs
* @property {Worker~Callbacks} callbacks - Map of {@link Worker~Callback} for processing jobs
* @property {Number} minPriority - The lowest job priority the worker will process
* @property {String} workerId - Unique identifier for this worker instance (auto-generated if not provided)
* @property {String} resumeTokenCollection - Collection name for storing resume tokens (default: 'monq_resume_tokens')
* @property {Number} delayedJobsPollInterval - Interval in ms for polling delayed jobs (default: 5000)
* @property {Number} changeStreamHealthCheckInterval - Interval in ms for change stream health checks (default: 60000)
* @property {Number} reconnectDelay - Delay in ms before reconnecting change stream (default: 5000)
* @property {Number} maxReconnectAttempts - Maximum change stream reconnection attempts (default: 10)
* @property {Number} maxPendingJobs - Maximum size of pending jobs queue (default: 1000)
* @property {String} fullDocument - Change stream fullDocument option: 'updateLookup' (default), 'whenAvailable', or 'required' (MongoDB 8.0+ compatible)
* @property {Number} changeStreamMaxAwaitTimeMS - Maximum time in ms to wait for change stream events (default: undefined)
*/

/**
* @constructor
* @param {string[]} queues - an array of queue names that this worker will listen for
* @param {Worker~Options} options - {@link Worker~Options} Options object 
*/
function Worker(queues, options) {
    options || (options = {});

    this.queueRotationIndex = 0; // Round-robin queue rotation counter (renamed from 'empty' for clarity)
    this.queues = queues || [];
    this.interval = options.interval || 5000;

    this.callbacks = options.callbacks || {};
    this.strategies = options.strategies || {};
    this.universal = options.universal || false;

    // Default retry strategies
    this.strategies.linear || (this.strategies.linear = linear);
    this.strategies.exponential || (this.strategies.exponential = exponential);

    // This worker will only process jobs of this priority or higher
    this.minPriority = options.minPriority;

    // Single change stream for all queues
    this.changeStream = null;
    this.processing = false; // Flag to prevent concurrent processing
    this.processingInitialJobs = false; // Flag to indicate we're processing initial jobs
    this.pendingJobs = []; // Queue for jobs that arrived while processing
    this.maxPendingJobs = options.maxPendingJobs || 1000; // Limit pending jobs queue size

    // Resume token support
    this.workerId = options.workerId || this.generateWorkerId();
    this.resumeTokenCollectionName = options.resumeTokenCollection || 'monq_resume_tokens';
    this.lastResumeToken = null;
    this.db = null; // Store db reference for saving resume tokens
    this.collectionName = null; // Store collection name for reconnection
    this.pendingResumeTokenSave = null; // Debounce resume token saves
    this.resumeTokenSaveTimeout = null;
    this.flushingResumeToken = false; // Flag to prevent concurrent flushResumeToken calls
    this.resumeTokenSaveQueue = []; // Queue for resume token saves when one is in progress
    this.resumeTokenSaveProcessingQueue = []; // Queue for resume token saves to ensure ordering
    this.maxResumeTokenSaveProcessingQueueSize = 500; // Maximum size for processing queue
    this.processingResumeTokenSaves = false; // Flag to prevent concurrent processing
    this.maxResumeTokenSaveQueueSize = 100; // Maximum size for resume token save queue
    this.dbConnectionLost = false; // Flag to track database connection state
    this.pendingResumeTokenSavesOnDisconnect = []; // Queue resume token saves when disconnected
    this.maxPendingResumeTokenSavesOnDisconnect = 1000; // Maximum size for pending saves during disconnect

    // Job deduplication and tracking
    // Use Map with timestamps for proper TTL support
    this.recentlyProcessedJobs = new Map(); // Track recently processed job IDs with timestamps: Map<jobId, timestamp>
    this.recentlyProcessedJobsMaxSize = 1000; // Maximum size of recently processed map
    this.recentlyProcessedJobsTTL = 60000; // 1 minute TTL for recently processed jobs
    this.recentlyProcessedJobsCleanupInterval = null;

    // Track pending job IDs to prevent duplicates (O(1) lookup)
    this.pendingJobIds = new Set(); // Set of job IDs currently in pending queue

    // Change stream backpressure
    this.changeStreamPaused = false; // Flag to track if change stream is paused

    // Change stream health monitoring
    this.lastChangeEventTime = null;
    this.changeStreamHealthCheckInterval = null;
    this.changeStreamHealthCheckIntervalMs = options.changeStreamHealthCheckInterval || 60000; // 1 minute default
    this.reconnectDelay = options.reconnectDelay || 5000; // 5 seconds default
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = options.maxReconnectAttempts || 10;
    this.healthWarningEmitted = false; // Flag to prevent warning spam
    this.lastResumeTokenUpdate = null; // Track when resume token was last updated
    this.pendingJobsWarningEmitted = false; // Flag to prevent pending jobs warning spam
    this.updatingResumeTokenTimestamp = false; // Flag to prevent concurrent timestamp updates

    // Periodic polling for delayed jobs
    this.delayedJobsPollInterval = null;
    this.delayedJobsPollIntervalMs = options.delayedJobsPollInterval || 5000; // 5 seconds default - check for delayed jobs

    // Change stream options
    this.fullDocumentOption = options.fullDocument || 'updateLookup'; // For MongoDB 8.0+ compatibility
    this.changeStreamMaxAwaitTimeMS = options.changeStreamMaxAwaitTimeMS; // Optional max await time

    // Job callback hang detection
    this.jobCallbackWatchdogTimeout = options.jobCallbackWatchdogTimeout || 300000; // 5 minutes default
    this.jobCallbackWatchdogTimer = null; // Timer to detect hung job callbacks

    // Connection monitoring
    this.connectionCheckInterval = null; // Interval to check connection health
    this.connectionCheckIntervalMs = options.connectionCheckInterval || 30000; // 30 seconds default
}

util.inherits(Worker, events.EventEmitter);

/**
* Generates a unique worker ID using GUID
* @returns {string} - Unique worker identifier (GUID)
*/
Worker.prototype.generateWorkerId = function () {
    // Generate a GUID (UUID v4)
    return uuid.v4();
};

/**
* Job handler functions should take this form
* @callback Worker~Callback
* @param {Job.params} params - {@link Job.params} object for the job to be processed 
* @param {Function} callback - NodeJS style callback to be invoked when job processing is finished
*/

/**
* Sets handlers to be invoked for each queue that the worker is listening to
* @param {Object} callbacks - map of {@link Worker~Callback} objects. Keys are the name of the queue, values are the handlers for those queues
*/
Worker.prototype.register = function (callbacks) {
    for (var name in callbacks) {
        this.callbacks[name] = callbacks[name];
    }
};

/**
* A map of {@link Worker~StrategyCallback}s
* @typedef {Object} Worker~Strategies
*/

/**
* @callback Worker~StrategyCallback
* @param {Job~Attempts} attempts - {@link Job~Attempts} object
* @returns {Number} delay time
*/

Worker.prototype.strategies = function (strategies) {
    for (var name in strategies) {
        this.strategies[name] = strategies[name];
    }
};


/**
* Starts the worker.  If no queues have been specified yet, this will loop
*/
Worker.prototype.start = function () {
    var self = this;

    if (this.queues.length === 0) {
        return setTimeout(this.start.bind(this), this.interval);
    }

    this.working = true;

    // Get connection from first queue
    var connection = this.queues[0].connection;
    var collectionName = this.queues[0].options.collection || 'jobs';

    // Get native MongoDB client for change streams
    connection.getNativeClient(function (err, client, db) {
        if (err) {
            self.emit('error', err);
            return;
        }

        // Store db reference and collection name for resume token operations and reconnection
        self.db = db;
        self.collectionName = collectionName;

        // Create indexes for resume token collection
        var dbIndex = require('./db');
        var resumeTokenCollection = db.collection(self.resumeTokenCollectionName);
        dbIndex.indexResumeTokens(resumeTokenCollection);

        // Load last resume token before processing
        self.loadLastResumeToken(db, function (err, resumeToken) {
            if (err) {
                self.emit('error', err);
                return;
            }

            self.lastResumeToken = resumeToken;
            self.lastResumeTokenUpdate = new Date(); // Initialize last update time

            // Set up change stream first to catch new jobs immediately
            self.setupChangeStreams(db, collectionName);

            // Start periodic polling for delayed jobs
            self.startDelayedJobsPolling();

            // Start cleanup for recently processed jobs tracking
            self.startRecentlyProcessedJobsCleanup();

            // Start connection health monitoring
            self.startConnectionHealthMonitoring();

            // Then process all existing queued jobs (jobs inserted during startup will be handled by change stream)
            self.processAllExistingJobs(function () {
                // All initial jobs processed, change stream is already running
            });
        });

    });
};

/**
* Stops the worker
*/
Worker.prototype.stop = function (callback) {
    var self = this;

    function done() {
        if (callback) callback();
    }

    if (!this.working) {
        return done();
    }

    this.working = false;

    // Clear health check interval
    if (this.changeStreamHealthCheckInterval) {
        clearInterval(this.changeStreamHealthCheckInterval);
        this.changeStreamHealthCheckInterval = null;
    }

    // Clear delayed jobs polling interval
    if (this.delayedJobsPollInterval) {
        clearInterval(this.delayedJobsPollInterval);
        this.delayedJobsPollInterval = null;
    }

    // Clear recently processed jobs cleanup interval
    if (this.recentlyProcessedJobsCleanupInterval) {
        clearInterval(this.recentlyProcessedJobsCleanupInterval);
        this.recentlyProcessedJobsCleanupInterval = null;
    }

    // Clear connection health check interval
    if (this.connectionCheckInterval) {
        clearInterval(this.connectionCheckInterval);
        this.connectionCheckInterval = null;
    }

    // Clear resume token save timeout
    if (this.resumeTokenSaveTimeout) {
        clearTimeout(this.resumeTokenSaveTimeout);
        this.resumeTokenSaveTimeout = null;
    }

    // Clear pending job IDs tracking
    if (this.pendingJobIds) {
        this.pendingJobIds.clear();
    }

    // Clear resume token save queue
    if (this.resumeTokenSaveQueue) {
        // Call callbacks for queued saves with error
        while (this.resumeTokenSaveQueue.length > 0) {
            var queued = this.resumeTokenSaveQueue.shift();
            if (queued && queued.callback) {
                queued.callback(new Error('Worker stopped - resume token save cancelled'));
            }
        }
    }

    // Clear resume token save processing queue
    if (this.resumeTokenSaveProcessingQueue) {
        // Call callbacks for queued saves with error
        while (this.resumeTokenSaveProcessingQueue.length > 0) {
            var queued = this.resumeTokenSaveProcessingQueue.shift();
            if (queued && queued.callback) {
                queued.callback(new Error('Worker stopped - resume token save cancelled'));
            }
        }
    }

    // Clear pending resume token saves on disconnect
    if (this.pendingResumeTokenSavesOnDisconnect) {
        // Call all pending callbacks with error
        while (this.pendingResumeTokenSavesOnDisconnect.length > 0) {
            var pending = this.pendingResumeTokenSavesOnDisconnect.shift();
            if (pending && pending.callback) {
                pending.callback(new Error('Worker stopped - resume token save cancelled'));
            }
        }
    }

    // Wait for resume token save processing to finish
    function waitForResumeTokenProcessing(callback) {
        if (!self.processingResumeTokenSaves) {
            return callback();
        }

        var maxWaitTime = 10000; // 10 seconds max wait for graceful shutdown
        var startTime = Date.now();
        var attempts = 0;
        var maxAttempts = 100; // 100 attempts * 100ms = 10 seconds

        function checkAndWait() {
            if (!self.processingResumeTokenSaves) {
                return callback();
            }

            attempts++;
            var elapsed = Date.now() - startTime;

            // Check if we've exceeded max wait time or attempts
            if (elapsed >= maxWaitTime || attempts >= maxAttempts) {
                self.emit('warning', 'Timeout waiting for resume token save processing to finish during stop');
                return callback();
            }

            setTimeout(checkAndWait, 100);
        }

        checkAndWait();
    }

    // Process any remaining pending jobs before stopping
    // Note: These jobs will remain in the database as QUEUED and will be picked up on restart
    if (this.pendingJobs.length > 0) {
        this.emit('info', 'Stopping worker with ' + this.pendingJobs.length + ' pending jobs - they will remain in database');
        // Clear pending jobs tracking
        this.pendingJobs = [];
        this.pendingJobIds.clear();
    }

    // Flush pending resume token save and wait for processing to finish
    this.flushResumeToken(function (err) {
        if (err) {
            self.emit('error', new Error('Failed to flush resume token during stop: ' + err.message));
        }

        // Wait for processing queue to finish
        waitForResumeTokenProcessing(function () {
            // Close the change stream
            if (!self.changeStream) {
                return done();
            }

            self.changeStream.close(function (err) {
                if (err) {
                    self.emit('error', err);
                }
                self.changeStream = null;
                self.emit('stopped');
                done();
            });
        });
    });
};

/**
* Adds a queue for the worker to listen on
* @param {string} queue - the name of the queue to add
*/
Worker.prototype.addQueue = function (queue) {
    if (!this.universal) {
        this.queues.push(queue);

        // If worker is running, refresh the change stream to apply the new filter
        // We call setupChangeStreams directly to avoid the backoff delay of reconnectChangeStream
        if (this.working && this.db && this.collectionName) {
            this.setupChangeStreams(this.db, this.collectionName);
        }
    }
};

/**
* Loads the last resume token for this worker from the database
* @param {Object} db - MongoDB database instance
* @param {Function} callback - callback with (err, resumeToken)
*/
Worker.prototype.loadLastResumeToken = function (db, callback) {
    var self = this;
    var resumeTokenCollection = db.collection(this.resumeTokenCollectionName);

    resumeTokenCollection.findOne(
        { workerId: this.workerId },
        function (err, doc) {
            if (err) {
                return callback(err);
            }

            // Return the resume token if found, otherwise null
            var resumeToken = doc && doc.resumeToken ? doc.resumeToken : null;
            callback(null, resumeToken);
        }
    );
};

/**
* Validates that a resume token is a valid BSON Timestamp object
* @param {Object} token - The resume token to validate
* @returns {Boolean} - True if token appears valid
*/
Worker.prototype.validateResumeToken = function (token) {
    if (!token) return false;

    // Check if it's a BSON Timestamp (has _bsontype or high/low properties)
    if (token._bsontype === 'Timestamp') {
        return true;
    }

    // Check for high/low properties (common in MongoDB driver)
    if (token.high !== undefined && token.low !== undefined) {
        return true;
    }

    // Check if it's an ObjectId-like structure (has toHexString method)
    if (typeof token.toHexString === 'function') {
        return true;
    }

    // If it has _id property, it might be a change stream event ID
    if (token._id !== undefined) {
        return true;
    }

    return false;
};

/**
* Compares two resume tokens to determine which is newer
* Resume tokens are BSON Timestamp objects that are monotonically increasing
* @param {Object} token1 - First resume token
* @param {Object} token2 - Second resume token
* @returns {Number} - Negative if token1 < token2, positive if token1 > token2, 0 if equal
*/
Worker.prototype.compareResumeTokens = function (token1, token2) {
    if (!token1 && !token2) return 0;
    if (!token1) return -1;
    if (!token2) return 1;

    // Validate tokens before comparison
    if (!this.validateResumeToken(token1) || !this.validateResumeToken(token2)) {
        // If tokens are invalid, log warning and use string comparison as last resort
        this.emit('warning', 'Invalid resume token structure detected, using fallback comparison');
        var str1 = token1.toString ? token1.toString() : String(token1);
        var str2 = token2.toString ? token2.toString() : String(token2);
        return str1.localeCompare(str2);
    }

    // Resume tokens are BSON Timestamp objects with high and low components
    // Compare high bits first, then low bits
    // Handle both direct properties and nested _id structure
    var t1High = token1.high !== undefined ? token1.high : (token1._id && token1._id.high !== undefined ? token1._id.high : undefined);
    var t1Low = token1.low !== undefined ? token1.low : (token1._id && token1._id.low !== undefined ? token1._id.low : undefined);
    var t2High = token2.high !== undefined ? token2.high : (token2._id && token2._id.high !== undefined ? token2._id.high : undefined);
    var t2Low = token2.low !== undefined ? token2.low : (token2._id && token2._id.low !== undefined ? token2._id.low : undefined);

    if (t1High !== undefined && t2High !== undefined) {
        if (t1High !== t2High) {
            return t1High - t2High;
        }
        if (t1Low !== undefined && t2Low !== undefined) {
            return t1Low - t2Low;
        }
    }

    // Fallback: compare as strings (less reliable but works)
    var str1 = token1.toString ? token1.toString() : String(token1);
    var str2 = token2.toString ? token2.toString() : String(token2);
    var comparison = str1.localeCompare(str2);

    // Log warning if we had to use string comparison
    if (comparison !== 0) {
        this.emit('warning', 'Resume token comparison using string fallback - may be unreliable');
    }

    return comparison;
};

/**
* Saves the resume token for this worker to the database (debounced)
* @param {Object} db - MongoDB database instance
* @param {Object} resumeToken - The resume token to save
* @param {Function} callback - Optional callback with (err) - if provided, saves immediately
*/
Worker.prototype.saveResumeToken = function (db, resumeToken, callback) {
    var self = this;

    if (!resumeToken) {
        if (callback) callback();
        return;
    }

    // Validate resume token structure
    if (!this.validateResumeToken(resumeToken)) {
        this.emit('error', new Error('Invalid resume token structure: ' + JSON.stringify(resumeToken)));
        if (callback) callback(new Error('Invalid resume token structure'));
        return;
    }

    // Validate that this token is newer than the last saved token
    // Only save if it's newer to prevent regression
    if (this.lastResumeToken) {
        var comparison = this.compareResumeTokens(resumeToken, this.lastResumeToken);
        if (comparison <= 0) {
            // Token is not newer, skip saving
            if (callback) callback();
            return;
        }
    }

    // Store the latest resume token to save (only if newer)
    this.pendingResumeTokenSave = resumeToken;

    // Clear existing timeout
    if (this.resumeTokenSaveTimeout) {
        clearTimeout(this.resumeTokenSaveTimeout);
        this.resumeTokenSaveTimeout = null;
    }

    // If callback provided, save immediately (e.g., on shutdown or after job completion)
    // Otherwise, debounce to batch saves
    if (callback && typeof callback === 'function') {
        // Queue the save to ensure ordering when multiple jobs complete simultaneously
        this.queueResumeTokenSave(resumeToken, callback);
    } else {
        // Debounced save - wait 1 second for more tokens before saving
        this.resumeTokenSaveTimeout = setTimeout(function () {
            self.flushResumeToken();
        }, 1000);
    }
};

/**
* Queues a resume token save to ensure ordering when multiple jobs complete simultaneously
* @param {Object} resumeToken - The resume token to save
* @param {Function} callback - Callback with (err)
*/
Worker.prototype.queueResumeTokenSave = function (resumeToken, callback) {
    var self = this;

    if (!resumeToken) {
        if (callback) callback();
        return;
    }

    // Validate resume token structure
    if (!this.validateResumeToken(resumeToken)) {
        this.emit('error', new Error('Invalid resume token structure: ' + JSON.stringify(resumeToken)));
        if (callback) callback(new Error('Invalid resume token structure'));
        return;
    }

    // Validate that this token is newer than the last saved token
    if (this.lastResumeToken) {
        var comparison = this.compareResumeTokens(resumeToken, this.lastResumeToken);
        if (comparison <= 0) {
            // Token is not newer, skip saving
            if (callback) callback();
            return;
        }
    }

    // Check queue size limit to prevent unbounded growth
    if (this.resumeTokenSaveProcessingQueue.length >= this.maxResumeTokenSaveProcessingQueueSize) {
        // Drop oldest entry to make room
        var dropped = this.resumeTokenSaveProcessingQueue.shift();
        if (dropped && dropped.callback) {
            dropped.callback(new Error('Resume token save processing queue overflow - request dropped'));
        }
        this.emit('warning', 'Resume token save processing queue overflow - dropping oldest request');
    }

    // Add to processing queue to ensure ordering
    this.resumeTokenSaveProcessingQueue.push({
        resumeToken: resumeToken,
        callback: callback || function () { }
    });

    // Start processing if not already processing
    if (!this.processingResumeTokenSaves) {
        this.processResumeTokenSaveProcessingQueue();
    }
};

/**
* Processes the resume token save processing queue to ensure ordering
*/
Worker.prototype.processResumeTokenSaveProcessingQueue = function () {
    var self = this;

    if (this.processingResumeTokenSaves || this.resumeTokenSaveProcessingQueue.length === 0 || !this.working) {
        return;
    }

    this.processingResumeTokenSaves = true;

    var consecutiveErrors = 0;
    var maxConsecutiveErrors = 10; // Stop processing if too many errors

    function processNext() {
        // Check if worker is still working
        if (!self.working) {
            self.processingResumeTokenSaves = false;
            return;
        }

        if (self.resumeTokenSaveProcessingQueue.length === 0) {
            self.processingResumeTokenSaves = false;
            return;
        }

        var save = self.resumeTokenSaveProcessingQueue.shift();

        // Re-validate resumeToken is not null
        if (!save.resumeToken) {
            save.callback();
            consecutiveErrors = 0; // Reset error count on successful skip
            processNext();
            return;
        }

        // Double-check token is still newer (in case another save completed)
        if (self.lastResumeToken) {
            var comparison = self.compareResumeTokens(save.resumeToken, self.lastResumeToken);
            if (comparison <= 0) {
                // Token is not newer, skip and process next
                save.callback();
                consecutiveErrors = 0; // Reset error count on successful skip
                processNext();
                return;
            }
        }

        // Set as pending and flush
        self.pendingResumeTokenSave = save.resumeToken;
        self.flushResumeToken(function (err) {
            if (err) {
                consecutiveErrors++;
                // If too many consecutive errors, stop processing to prevent queue growth
                if (consecutiveErrors >= maxConsecutiveErrors) {
                    self.processingResumeTokenSaves = false;
                    self.emit('error', new Error('Too many consecutive resume token save errors, stopping processing'));
                    return;
                }
            } else {
                consecutiveErrors = 0; // Reset error count on success
            }

            save.callback(err);

            // Process next save after a short delay to ensure ordering
            setTimeout(processNext, 10);
        });
    }

    processNext();
};

/**
* Updates the resume token's updatedAt timestamp without changing the token value
* This keeps the token alive for active workers even when no events occur
* @param {Function} callback - Optional callback with (err)
*/
Worker.prototype.updateResumeTokenTimestamp = function (callback) {
    var self = this;
    callback = callback || function () { };

    if (!this.lastResumeToken || !this.db) {
        return callback();
    }

    var resumeTokenCollection = this.db.collection(this.resumeTokenCollectionName);

    // Update only the updatedAt field to keep the token alive
    // If document doesn't exist, create it with the current resume token
    resumeTokenCollection.updateOne(
        { workerId: this.workerId },
        {
            $set: {
                workerId: this.workerId,
                resumeToken: this.lastResumeToken,
                updatedAt: new Date()
            }
        },
        { upsert: true },
        function (err, result) {
            if (err) {
                return callback(err);
            }

            // Verify the update succeeded
            if (result.matchedCount === 0 && result.upsertedCount === 0) {
                return callback(new Error('Failed to update resume token timestamp'));
            }

            callback();
        }
    );
};

/**
* Flushes the pending resume token to the database with retry logic and concurrency protection
* @param {Function} callback - Optional callback with (err)
* @param {Number} retries - Number of retry attempts remaining (internal use)
* @param {Number} retryDelay - Delay in ms before retry (for exponential backoff)
*/
Worker.prototype.flushResumeToken = function (callback, retries, retryDelay) {
    var self = this;
    callback = callback || function () { };
    retries = retries !== undefined ? retries : 3; // Default 3 retries
    retryDelay = retryDelay !== undefined ? retryDelay : 1000; // Start with 1 second

    // Prevent concurrent flushResumeToken calls
    if (this.flushingResumeToken) {
        // Check queue size limit to prevent unbounded growth
        if (this.resumeTokenSaveQueue.length >= this.maxResumeTokenSaveQueueSize) {
            // Drop oldest entry to make room
            var dropped = this.resumeTokenSaveQueue.shift();
            if (dropped && dropped.callback) {
                dropped.callback(new Error('Resume token save queue overflow - request dropped'));
            }
            this.emit('warning', 'Resume token save queue overflow - dropping oldest request');
        }
        // Queue this save request
        this.resumeTokenSaveQueue.push({ callback: callback, retries: retries, retryDelay: retryDelay });
        return;
    }

    if (!this.pendingResumeTokenSave || !this.db) {
        // Process queued saves if any
        this.processResumeTokenSaveQueue();
        return callback();
    }

    // Check if database connection is lost
    if (this.dbConnectionLost) {
        // Check queue size limit to prevent unbounded growth
        if (this.pendingResumeTokenSavesOnDisconnect.length >= this.maxPendingResumeTokenSavesOnDisconnect) {
            // Drop oldest entry to make room
            var dropped = this.pendingResumeTokenSavesOnDisconnect.shift();
            if (dropped && dropped.callback) {
                dropped.callback(new Error('Resume token save queue overflow during disconnect - request dropped'));
            }
            this.emit('warning', 'Resume token save queue overflow during disconnect - dropping oldest request');
        }
        // Queue save for when connection is restored
        this.pendingResumeTokenSavesOnDisconnect.push({
            resumeToken: this.pendingResumeTokenSave,
            callback: callback,
            retries: retries,
            retryDelay: retryDelay
        });
        // Process queued saves if any
        this.processResumeTokenSaveQueue();
        return;
    }

    // Mark as flushing to prevent concurrent calls
    this.flushingResumeToken = true;

    var resumeToken = this.pendingResumeTokenSave;
    // Don't clear pendingResumeTokenSave yet - only clear after successful save
    // This allows retry logic to work

    if (this.resumeTokenSaveTimeout) {
        clearTimeout(this.resumeTokenSaveTimeout);
        this.resumeTokenSaveTimeout = null;
    }

    var resumeTokenCollection = this.db.collection(this.resumeTokenCollectionName);

    // Upsert the resume token with write concern for reliability
    var options = {
        upsert: true,
        w: 'majority' // Ensure write is acknowledged by majority of replica set
    };

    resumeTokenCollection.updateOne(
        { workerId: this.workerId },
        {
            $set: {
                workerId: this.workerId,
                resumeToken: resumeToken,
                updatedAt: new Date()
            }
        },
        options,
        function (err) {
            if (err) {
                // Check if this is a connection error
                var isConnectionError = err.message && (
                    err.message.indexOf('connection') !== -1 ||
                    err.message.indexOf('network') !== -1 ||
                    err.message.indexOf('timeout') !== -1 ||
                    err.message.indexOf('not connected') !== -1 ||
                    err.code === 6 || // HostUnreachable
                    err.code === 7 || // HostNotFound
                    err.code === 50  // MaxTimeMSExpired
                );

                if (isConnectionError) {
                    // Mark connection as lost
                    self.dbConnectionLost = true;

                    // Check queue size limit to prevent unbounded growth
                    if (self.pendingResumeTokenSavesOnDisconnect.length >= self.maxPendingResumeTokenSavesOnDisconnect) {
                        // Drop oldest entry to make room
                        var dropped = self.pendingResumeTokenSavesOnDisconnect.shift();
                        if (dropped && dropped.callback) {
                            dropped.callback(new Error('Resume token save queue overflow during disconnect - request dropped'));
                        }
                        self.emit('warning', 'Resume token save queue overflow during disconnect - dropping oldest request');
                    }

                    // Queue save for when connection is restored
                    self.pendingResumeTokenSavesOnDisconnect.push({
                        resumeToken: resumeToken,
                        callback: callback,
                        retries: retries,
                        retryDelay: retryDelay
                    });

                    self.flushingResumeToken = false;
                    // Process queued saves if any
                    self.processResumeTokenSaveQueue();
                    // Don't call callback with error - will retry when connection restored
                    return;
                }

                // Retry on other transient failures
                if (retries > 0) {
                    // Exponential backoff: double the delay for each retry
                    var nextDelay = retryDelay * 2;
                    setTimeout(function () {
                        self.flushResumeToken(callback, retries - 1, nextDelay);
                    }, retryDelay);
                    return;
                }

                // Max retries reached or non-retryable error
                self.flushingResumeToken = false;
                // Process queued saves
                self.processResumeTokenSaveQueue();
                return callback(err);
            }

            // Success - mark connection as restored
            if (self.dbConnectionLost) {
                self.dbConnectionLost = false;
                // Process queued saves from disconnect period
                self.processPendingResumeTokenSavesOnReconnect();
            }

            // Success - now clear the pending save and update in-memory token
            self.pendingResumeTokenSave = null;

            // Update in-memory token only after successful save (prevents race conditions)
            self.lastResumeToken = resumeToken;
            self.lastResumeTokenUpdate = new Date();

            // Mark as not flushing
            self.flushingResumeToken = false;

            callback();

            // Process queued saves
            self.processResumeTokenSaveQueue();
        }
    );
};

/**
* Processes queued resume token save requests
*/
Worker.prototype.processResumeTokenSaveQueue = function () {
    if (this.resumeTokenSaveQueue.length === 0) {
        return;
    }

    // Process the next queued save
    var next = this.resumeTokenSaveQueue.shift();
    if (next) {
        this.flushResumeToken(next.callback, next.retries, next.retryDelay);
    }
};

/**
* Processes resume token saves that were queued during connection loss
*/
Worker.prototype.processPendingResumeTokenSavesOnReconnect = function () {
    var self = this;

    // Wait for processing queue to finish before processing disconnect saves
    // This ensures saves are processed in the correct order
    function waitForProcessingQueue(callback) {
        if (!self.processingResumeTokenSaves) {
            return callback();
        }

        // Add timeout to prevent waiting indefinitely
        var maxWaitTime = 30000; // 30 seconds max wait
        var startTime = Date.now();
        var attempts = 0;
        var maxAttempts = 300; // 300 attempts * 100ms = 30 seconds

        function checkAndWait() {
            if (!self.processingResumeTokenSaves) {
                return callback();
            }

            attempts++;
            var elapsed = Date.now() - startTime;

            // Check if we've exceeded max wait time or attempts
            if (elapsed >= maxWaitTime || attempts >= maxAttempts) {
                self.emit('warning', 'Timeout waiting for resume token save processing queue to finish');
                return callback();
            }

            // Check if worker stopped
            if (!self.working) {
                return callback();
            }

            // Wait a bit and check again
            setTimeout(checkAndWait, 100);
        }

        checkAndWait();
    }

    waitForProcessingQueue(function () {
        // Now process disconnect saves by adding them to the processing queue
        // This ensures they're processed in order with other saves
        while (self.pendingResumeTokenSavesOnDisconnect.length > 0) {
            var save = self.pendingResumeTokenSavesOnDisconnect.shift();

            // Validate token is newer than last saved token
            if (self.lastResumeToken) {
                var comparison = self.compareResumeTokens(save.resumeToken, self.lastResumeToken);
                if (comparison <= 0) {
                    // Token is not newer, skip
                    if (save.callback) {
                        save.callback();
                    }
                    continue;
                }
            }

            // Validate token structure
            if (!self.validateResumeToken(save.resumeToken)) {
                self.emit('error', new Error('Invalid resume token structure in reconnect queue'));
                if (save.callback) {
                    save.callback(new Error('Invalid resume token structure'));
                }
                continue;
            }

            // Add to processing queue to ensure ordering
            if (self.resumeTokenSaveProcessingQueue.length >= self.maxResumeTokenSaveProcessingQueueSize) {
                // Drop oldest entry to make room
                var dropped = self.resumeTokenSaveProcessingQueue.shift();
                if (dropped && dropped.callback) {
                    dropped.callback(new Error('Resume token save processing queue overflow - request dropped'));
                }
                self.emit('warning', 'Resume token save processing queue overflow - dropping oldest request');
            }

            self.resumeTokenSaveProcessingQueue.push({
                resumeToken: save.resumeToken,
                callback: save.callback || function () { }
            });
        }

        // Start processing if not already processing
        if (!self.processingResumeTokenSaves && self.resumeTokenSaveProcessingQueue.length > 0) {
            self.processResumeTokenSaveProcessingQueue();
        }
    });
};

/**
* Sets up a single change stream for all queues the worker processes
*/
Worker.prototype.setupChangeStreams = function (db, collectionName) {
    var self = this;
    var Job = require('./job');

    // Close existing change stream if any
    // Note: If we're resuming from a pause, we want to close the old stream
    // If we're reconnecting, we also want to close the old stream
    if (this.changeStream) {
        var oldStream = this.changeStream;
        this.changeStream = null; // Clear reference immediately

        // Remove all listeners to prevent 'close' event from triggering reconnect logic
        // This avoids a race condition where manual close triggers auto-reconnect
        oldStream.removeAllListeners();

        oldStream.close(function (err) {
            if (err && self.working) {
                // Determine if we should log this error
                // If message is "ChangeStream is closed", it's expected and can be ignored
                if (err.message !== 'ChangeStream is closed') {
                    self.emit('error', new Error('Error closing old change stream: ' + err.message));
                }
            }
        });
    }

    // Clear pause flag when recreating stream
    this.changeStreamPaused = false;

    // Clear existing health check
    if (this.changeStreamHealthCheckInterval) {
        clearInterval(this.changeStreamHealthCheckInterval);
        this.changeStreamHealthCheckInterval = null;
    }

    var collection = db.collection(collectionName);

    // Build the match filter - watch for queued job inserts and updates
    // MongoDB 8.0.17+ compatible: fullDocument.status filtering works with fullDocument: 'updateLookup'
    // Note: We can't filter delayed jobs in change stream pipeline (MongoDB limitation)
    // Delayed jobs will be filtered in dequeueFromChangeStream instead
    // Strategy: Watch ALL queues and filter in application code to handle dynamic queue changes
    var insertFilter = {
        'operationType': 'insert',
        'fullDocument.status': Job.QUEUED
    };

    // For updates: MongoDB 8.0.17+ supports checking fullDocument.status when fullDocument: 'updateLookup'
    // We watch all updates and check status in application code for reliability across versions
    var updateFilter = {
        'operationType': 'update'
    };

    // Don't filter by queue names at change stream level
    // Instead, watch all queues and filter in application code (findQueueForJob)
    // This allows dynamic queue addition/removal without recreating change stream
    // If universal worker, this is already the behavior we want

    // Strategies:
    // 1. Universal worker: Watch all queues (no filtering)
    // 2. Specific queues: Filter by queue names in change stream to reduce network traffic

    // Base filter for operations
    var operationFilter = {
        $or: [insertFilter, updateFilter]
    };

    // Combine into final match filter
    var matchFilter = {};

    if (!this.universal && this.queues.length > 0) {
        // Extract queue names from Queue objects for filtering
        var queueNames = this.queues.map(function(q) { return q.name; });
        // Filter by specific queues
        matchFilter = {
            $and: [
                operationFilter,
                { 'fullDocument.queue': { $in: queueNames } }
            ]
        };
    } else {
        // Universal worker or no specific queues - watch everything
        matchFilter = operationFilter;
    }

    // Build change stream options
    // MongoDB 8.0+ compatible: fullDocument option supports 'updateLookup', 'whenAvailable', 'required'
    var changeStreamOptions = {
        fullDocument: this.fullDocumentOption || 'updateLookup'
    };

    // Optional max await time for change stream events
    if (this.changeStreamMaxAwaitTimeMS) {
        changeStreamOptions.maxAwaitTimeMS = this.changeStreamMaxAwaitTimeMS;
    }

    // Use resume token if available
    // Note: Resume token validation happens in error handler (invalid token errors are caught)
    if (this.lastResumeToken) {
        // Validate resume token before using it
        if (this.validateResumeToken(this.lastResumeToken)) {
            changeStreamOptions.resumeAfter = this.lastResumeToken;
        } else {
            this.emit('warning', 'Invalid resume token structure, starting from beginning');
            this.lastResumeToken = null;
        }
    }

    // Create a single change stream filtered by queue names
    var changeStream = collection.watch([
        { $match: matchFilter }
    ], changeStreamOptions);

    // Reset reconnect attempts on successful change stream creation
    self.reconnectAttempts = 0;

    // Store the change stream reference
    self.changeStream = changeStream;

    changeStream.on('change', function (change) {
        // Check if worker is still working - ignore events after stop
        if (!self.working) {
            return;
        }

        // Note: Change stream pause is implemented by closing/recreating the stream
        // If changeStreamPaused is true, the stream should be closed, so we shouldn't receive events here
        // This check is a safety measure in case events arrive during the pause transition
        if (self.changeStreamPaused) {
            // This shouldn't happen if pause is implemented correctly, but log a warning
            self.emit('warning', 'Received change stream event while paused - this should not happen');
            return;
        }

        // Update last change event time for health monitoring
        self.lastChangeEventTime = new Date();
        self.reconnectAttempts = 0; // Reset reconnect attempts on successful event
        self.healthWarningEmitted = false; // Reset warning flag on new events

        // Only process actual job documents (not other document types)
        // Verify it's a job by checking for job-specific fields
        var isJobDocument = change.fullDocument &&
            (change.fullDocument.status !== undefined ||
                change.fullDocument.name !== undefined ||
                change.fullDocument.queue !== undefined);

        if (!isJobDocument || !change._id) {
            return; // Not a job document, skip
        }

        var resumeToken = change._id; // Store resume token for this specific change event

        // Handle insert operations
        if (change.operationType === 'insert' && change.fullDocument) {
            var jobDoc = change.fullDocument;

            // Verify it's a queued job (double-check)
            if (jobDoc.status === Job.QUEUED) {
                // Check if we've recently processed this job (deduplication)
                var jobId = jobDoc._id ? jobDoc._id.toString() : null;
                if (jobId && self.recentlyProcessedJobs.has(jobId)) {
                    // Check if entry is still within TTL
                    var timestamp = self.recentlyProcessedJobs.get(jobId);
                    var age = Date.now() - timestamp;
                    if (age < self.recentlyProcessedJobsTTL) {
                        return; // Skip duplicate event (still within TTL)
                    } else {
                        // Entry expired, remove it
                        self.recentlyProcessedJobs.delete(jobId);
                    }
                }

                // Filter by queue in application code (handles dynamic queue changes)
                var queue = self.findQueueForJob(jobDoc);
                if (queue) {
                    self.processJobFromChangeStream(queue, jobDoc, resumeToken);
                }
            }
        }

        // Handle update operations (retried jobs, status changes to queued)
        if (change.operationType === 'update') {
            // Handle case where fullDocument might not be available
            var jobDoc = change.fullDocument;

            // If fullDocument is not available, we need to fetch it
            // This can happen with certain MongoDB versions or configurations
            if (!jobDoc && change.documentKey && change.documentKey._id) {
                // Fetch the document to get current status
                var collection = self.db.collection(self.collectionName);
                // Add timeout to prevent hanging indefinitely
                var findOptions = {
                    maxTimeMS: 5000 // 5 second timeout
                };
                collection.findOne({ _id: change.documentKey._id }, findOptions, function (err, doc) {
                    // Check if worker is still working before processing
                    if (!self.working) {
                        return;
                    }

                    if (err) {
                        self.emit('error', err);
                        return;
                    }

                    if (doc && doc.status === Job.QUEUED) {
                        // Check if status was actually updated to QUEUED
                        var statusWasUpdated = false;
                        if (change.updateDescription && change.updateDescription.updatedFields) {
                            statusWasUpdated = change.updateDescription.updatedFields.status === Job.QUEUED;
                        } else {
                            // If we can't determine from updateDescription, assume it was updated
                            // (safer to process than to skip)
                            statusWasUpdated = true;
                        }

                        if (statusWasUpdated) {
                            // Process the job
                            var jobId = doc._id ? doc._id.toString() : null;
                            if (jobId && self.recentlyProcessedJobs.has(jobId)) {
                                var timestamp = self.recentlyProcessedJobs.get(jobId);
                                var age = Date.now() - timestamp;
                                if (age < self.recentlyProcessedJobsTTL) {
                                    return; // Skip duplicate
                                } else {
                                    self.recentlyProcessedJobs.delete(jobId);
                                }
                            }

                            var queue = self.findQueueForJob(doc);
                            if (queue) {
                                self.processJobFromChangeStream(queue, doc, resumeToken);
                            }
                        }
                    }
                });
                return; // Exit early, will process in callback
            }

            // If fullDocument is available, process normally
            if (jobDoc && jobDoc.status === Job.QUEUED) {
                // Check if status was actually updated (not just a lookup of existing queued job)
                var statusWasUpdated = false;
                if (change.updateDescription && change.updateDescription.updatedFields) {
                    statusWasUpdated = change.updateDescription.updatedFields.status === Job.QUEUED;
                } else {
                    // If updateDescription is not available, assume status was updated
                    // (safer to process than to skip)
                    statusWasUpdated = true;
                }

                // Process if status was updated or if we can't determine
                if (statusWasUpdated) {
                    // Check if we've recently processed this job (deduplication)
                    var jobId = jobDoc._id ? jobDoc._id.toString() : null;
                    if (jobId && self.recentlyProcessedJobs.has(jobId)) {
                        // Check if entry is still within TTL
                        var timestamp = self.recentlyProcessedJobs.get(jobId);
                        var age = Date.now() - timestamp;
                        if (age < self.recentlyProcessedJobsTTL) {
                            return; // Skip duplicate event (still within TTL)
                        } else {
                            // Entry expired, remove it
                            self.recentlyProcessedJobs.delete(jobId);
                        }
                    }

                    // Filter by queue in application code (handles dynamic queue changes)
                    var queue = self.findQueueForJob(jobDoc);
                    if (queue) {
                        self.processJobFromChangeStream(queue, jobDoc, resumeToken);
                    }
                }
            }
        }
    });

    changeStream.on('error', function (err) {
        self.emit('error', err);

        // Handle invalid resume token
        if (err.code === 280 || err.code === 286 || err.message && err.message.indexOf('resume token') !== -1) {
            // Invalid resume token - reset and reconnect
            self.lastResumeToken = null;
            self.reconnectChangeStream();
            return;
        }

        // Handle rate limiting errors (MongoDB may throttle change streams)
        if (err.code === 16500 || err.code === 16501 ||
            (err.message && (err.message.indexOf('rate limit') !== -1 ||
                err.message.indexOf('throttle') !== -1))) {
            // Rate limited - wait longer before reconnecting
            self.emit('warning', 'Change stream rate limited, waiting before reconnect');
            setTimeout(function () {
                if (self.working) {
                    self.reconnectChangeStream();
                }
            }, self.reconnectDelay * 2); // Wait 2x longer for rate limits
            return;
        }

        // Attempt to reconnect on other errors
        if (self.working) {
            self.reconnectChangeStream();
        }
    });

    changeStream.on('close', function () {
        if (self.working) {
            // Change stream closed unexpectedly - attempt to reconnect
            // Flush any pending resume token saves before reconnecting to ensure we don't lose progress
            function flushAllSaves(callback) {
                // First flush pending save
                self.flushResumeToken(function (err) {
                    if (err) {
                        self.emit('error', new Error('Failed to flush resume token before reconnect: ' + err.message));
                    }

                    // Wait for processing queue to finish
                    var maxWaitTime = 30000; // 30 seconds max wait
                    var startTime = Date.now();
                    var attempts = 0;
                    var maxAttempts = 300; // 300 attempts * 100ms = 30 seconds

                    function waitForProcessingQueue() {
                        if (!self.processingResumeTokenSaves || self.resumeTokenSaveProcessingQueue.length === 0) {
                            return callback();
                        }

                        attempts++;
                        var elapsed = Date.now() - startTime;

                        // Check if we've exceeded max wait time or attempts
                        if (elapsed >= maxWaitTime || attempts >= maxAttempts) {
                            self.emit('warning', 'Timeout waiting for resume token save processing queue to finish before reconnect');
                            return callback();
                        }

                        // Check if worker stopped
                        if (!self.working) {
                            return callback();
                        }

                        setTimeout(waitForProcessingQueue, 100);
                    }

                    waitForProcessingQueue();
                });
            }

            flushAllSaves(function () {
                // Reconnect even if flush failed
                self.reconnectChangeStream();
            });
        }
    });

    // Handle invalidate event (collection dropped/renamed)
    changeStream.on('invalidate', function () {
        self.emit('error', new Error('Change stream invalidated - collection may have been dropped or renamed'));
        if (self.working) {
            // Attempt to reconnect after a delay
            setTimeout(function () {
                self.reconnectChangeStream();
            }, self.reconnectDelay);
        }
    });

    // changeStream reference is already set above in setupChangeStreams (line 1079)
    // No need to set it again here

    // Start health monitoring
    self.startChangeStreamHealthMonitoring();
};

/**
* Reconnects the change stream after an error with exponential backoff
*/
Worker.prototype.reconnectChangeStream = function () {
    var self = this;

    if (!this.working || !this.db || !this.collectionName) {
        return;
    }

    // Limit reconnect attempts
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
        this.emit('error', new Error('Max reconnect attempts reached for change stream'));
        return;
    }

    this.reconnectAttempts++;

    // Exponential backoff: delay increases with each attempt
    // Formula: baseDelay * (2 ^ (attempts - 1))
    // Attempt 1: 5s, Attempt 2: 10s, Attempt 3: 20s, etc.
    var backoffDelay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
    // Cap at 5 minutes
    backoffDelay = Math.min(backoffDelay, 300000);

    // Wait before reconnecting
    setTimeout(function () {
        if (!self.working) {
            return;
        }

        // Flush resume token before reconnecting
        self.flushResumeToken(function (err) {
            if (err) {
                self.emit('error', err);
            }

            // Reconnect change stream
            self.setupChangeStreams(self.db, self.collectionName);
        });
    }, backoffDelay);
};

/**
* Starts health monitoring for the change stream
*/
Worker.prototype.startChangeStreamHealthMonitoring = function () {
    var self = this;

    // Clear existing interval
    if (this.changeStreamHealthCheckInterval) {
        clearInterval(this.changeStreamHealthCheckInterval);
    }

    // Initialize last change event time
    this.lastChangeEventTime = new Date();

    // Check health periodically
    this.changeStreamHealthCheckInterval = setInterval(function () {
        if (!self.working) {
            return;
        }

        // Double-check working flag to prevent operations after stop
        if (!self.working) {
            return;
        }

        var timeSinceLastEvent = Date.now() - (self.lastChangeEventTime ? self.lastChangeEventTime.getTime() : 0);

        // If no events for 5 minutes and we're working, something might be wrong
        // But don't error - just log a warning (change streams can be quiet if no jobs)
        // Only emit warning once to prevent spam
        if (timeSinceLastEvent > 300000 && !self.healthWarningEmitted) { // 5 minutes
            // This is just informational - change streams can be quiet
            self.healthWarningEmitted = true;
            self.emit('warning', 'No events for the last 5 minutes');
        } else if (timeSinceLastEvent <= 300000 && self.healthWarningEmitted) {
            // Reset flag if events resume
            self.healthWarningEmitted = false;
        }

        // Check if change stream is still open
        if (self.changeStream && self.changeStream.closed) {
            self.emit('error', new Error('Change stream closed unexpectedly'));
            self.reconnectChangeStream();
        }

        // Periodically update resume token's updatedAt to prevent TTL expiration
        // Update every 6 days to keep token alive (TTL is 7 days)
        // This ensures active workers don't lose their resume tokens
        var timeSinceLastTokenUpdate = self.lastResumeTokenUpdate ?
            (Date.now() - self.lastResumeTokenUpdate.getTime()) : Infinity;
        var sixDaysInMs = 6 * 24 * 60 * 60 * 1000; // 6 days in milliseconds

        // Prevent concurrent timestamp updates
        if (self.lastResumeToken && self.db && timeSinceLastTokenUpdate > sixDaysInMs && !self.updatingResumeTokenTimestamp) {
            self.updatingResumeTokenTimestamp = true;
            // Update the resume token's updatedAt field to keep it alive
            self.updateResumeTokenTimestamp(function (err) {
                self.updatingResumeTokenTimestamp = false;
                if (err) {
                    self.emit('error', err);
                } else {
                    self.lastResumeTokenUpdate = new Date();
                }
            });
        }
    }, this.changeStreamHealthCheckIntervalMs);
};

/**
* Starts periodic polling for delayed jobs that are now ready
* This ensures delayed jobs are processed when their delay expires
* Jobs are processed idempotently via atomic findAndModify operations
*/
Worker.prototype.startDelayedJobsPolling = function () {
    var self = this;

    // Clear existing interval
    if (this.delayedJobsPollInterval) {
        clearInterval(this.delayedJobsPollInterval);
    }

    // Poll periodically for delayed jobs that are now ready
    this.delayedJobsPollInterval = setInterval(function () {
        if (!self.working) {
            return;
        }

        // Atomic check-and-set to prevent race conditions
        // Use a single check to prevent change stream events from starting processing
        // between the check and the set
        if (self.processing) {
            return;
        }

        // Atomically set processing flag - if another operation set it between
        // the check and here, we'll detect it in dequeue callback
        self.processing = true;

        // Check for delayed jobs that are now ready
        // This uses the same dequeue() method which atomically claims jobs
        // Ensuring idempotency - only one worker can claim each job via findAndModify
        self.dequeue(function (err, job) {
            if (err) {
                self.processing = false;
                return self.emit('error', err);
            }

            if (job) {
                // Found a delayed job that's now ready
                self.emit('dequeued', job.data);
                self.work(job);
                // processing flag will be reset in work() completion handlers
            } else {
                // No job found, reset processing flag
                self.processing = false;
            }
            // If no job found, that's fine - will check again on next interval
        });
    }, this.delayedJobsPollIntervalMs);
};

/**
* Finds the appropriate queue for a job document
* @param {Object} jobDocument - The job document from the change stream
* @returns {Queue|null} - The matching queue or null if no match
*/
Worker.prototype.findQueueForJob = function (jobDocument) {
    var self = this;
    var jobQueueName = jobDocument.queue;

    // Validate queue field exists
    if (jobQueueName === undefined || jobQueueName === null) {
        // If no queue field and not universal, this is a malformed job
        if (!this.universal) {
            this.emit('error', new Error('Job document missing queue field: ' + JSON.stringify(jobDocument._id || jobDocument)));
            return null;
        }
        // Universal workers can handle jobs without queue field
        jobQueueName = '*';
    }

    // If this is a universal worker, return the first (and only) queue
    if (this.universal) {
        return this.queues.length > 0 ? this.queues[0] : null;
    }

    // Find the queue that matches the job's queue name
    for (var i = 0; i < this.queues.length; i++) {
        var queue = this.queues[i];

        // Check if this queue matches the job's queue name
        if (queue.name === jobQueueName) {
            return queue;
        }
    }

    // No matching queue found - job is for a queue this worker doesn't process
    // This is normal when watching all queues and filtering in application code
    return null;
};

/**
* Processes a specific job document from a change stream
* @param {Queue} queue - The queue for this job
* @param {Object} jobDocument - The job document from the change stream
* @param {Object} resumeToken - The resume token for this change event
*/
Worker.prototype.processJobFromChangeStream = function (queue, jobDocument, resumeToken) {
    var self = this;

    if (!this.working) {
        return; // Worker stopped, ignore this job
    }

    // NOTE: Pending jobs are stored in memory only. If the worker crashes,
    // pending jobs are lost. However, jobs remain QUEUED in the database
    // and will be picked up by polling or change stream on restart.

    // Check if worker is still working before processing
    if (!this.working) {
        return; // Worker stopped, ignore this job
    }

    // Check for duplicate job ID in pending queue (deduplication) - O(1) lookup
    var jobId = jobDocument._id ? jobDocument._id.toString() : null;
    if (jobId) {
        // Check if already in pending queue using Set for O(1) lookup
        if (this.pendingJobIds.has(jobId)) {
            return; // Already queued, skip
        }
    }

    // If already processing, queue this job for later
    if (this.processing) {
        // Check queue size limit to prevent memory leaks
        // Add small buffer (10%) to handle race conditions
        var maxWithBuffer = Math.floor(this.maxPendingJobs * 1.1);
        if (this.pendingJobs.length >= maxWithBuffer) {
            this.emit('error', new Error('Pending jobs queue overflow: ' + this.pendingJobs.length + ' jobs queued (max: ' + maxWithBuffer + ')'));
            // Pause change stream to prevent more events
            this.pauseChangeStream();
            return;
        }

        // Emit warning when queue crosses 80% threshold (backpressure mechanism)
        // Only emit once when crossing the threshold to prevent spam
        var queuePercent = (this.pendingJobs.length / this.maxPendingJobs) * 100;
        if (queuePercent >= 80 && !this.pendingJobsWarningEmitted) {
            this.pendingJobsWarningEmitted = true;
            this.emit('warning', 'Pending jobs queue is ' + Math.round(queuePercent) + '% full');
            // Pause change stream when queue is 80% full
            this.pauseChangeStream();
        } else if (queuePercent < 80 && this.pendingJobsWarningEmitted) {
            // Reset flag when queue drops below 80%
            this.pendingJobsWarningEmitted = false;
            // Resume change stream when queue drops below 80%
            this.resumeChangeStream();
        }

        // Store job with its resume token (one per job to prevent overwriting)
        this.pendingJobs.push({
            queue: queue,
            jobDocument: jobDocument,
            resumeToken: resumeToken // Store resume token per job
        });

        // Track job ID in Set for O(1) duplicate checking
        if (jobId) {
            this.pendingJobIds.add(jobId);
        }

        return;
    }

    this.processJobInternal(queue, jobDocument, resumeToken);
};

/**
* Internal method to process a job (called directly or from pending queue)
* @param {Queue} queue - The queue for this job
* @param {Object} jobDocument - The job document from the change stream
* @param {Object} resumeToken - The resume token for this change event (optional, for pending jobs)
*/
Worker.prototype.processJobInternal = function (queue, jobDocument, resumeToken) {
    var self = this;

    this.processing = true;

    // Dequeue the specific job document from the change stream
    queue.dequeueFromChangeStream(jobDocument, {
        minPriority: this.minPriority,
        callbacks: this.callbacks
    }, function (err, job) {
        if (err) {
            self.processing = false;
            self.emit('error', err);
            // Don't process next pending job on error - let error recovery handle it
            return;
        }

        if (job) {
            // Successfully claimed the job
            // Store resume token with the job - will save after job completes
            // This ensures we only advance the resume token for jobs we actually complete
            var jobId = job.data._id ? job.data._id.toString() : null;

            // Store resume token on job object for saving after completion
            job._resumeToken = resumeToken;

            self.emit('dequeued', job.data);

            // Don't add to recentlyProcessedJobs here - only add after successful completion
            // This allows immediate retries if job fails quickly
            // The job will be added to recentlyProcessedJobs in the work() completion handler

            // Note: processing flag remains true until job completes in work() done callback
            // The done callback will call processNextPendingJob() which handles the flag
            self.work(job);
        } else {
            // Job was already claimed by another worker or doesn't match criteria
            // This is normal in a multi-worker setup
            // Still save resume token to advance past this event (prevents reprocessing on restart)
            // The job itself won't be processed (already claimed), but we've seen this event
            // Note: lastResumeToken is updated in flushResumeToken after successful save
            if (resumeToken) {
                // Use queueResumeTokenSave to ensure it's processed in order with other saves
                // This prevents reprocessing on restart
                self.queueResumeTokenSave(resumeToken, function (err) {
                    if (err) {
                        self.emit('error', new Error('Failed to save resume token for failed claim: ' + err.message));
                        // Don't update lastResumeToken if save failed
                    }
                });
            }

            // Job was not claimed, so we can process next pending job immediately
            // Reset processing flag and process next
            self.processing = false;
            self.processNextPendingJob();
        }
    });
};

/**
* Processes the next job from the pending queue
* Uses atomic check-and-shift to prevent race conditions
*/
Worker.prototype.processNextPendingJob = function () {
    if (!this.working) {
        return;
    }

    // Atomic check-and-set: check processing flag and set it atomically
    // This prevents race conditions where multiple events try to process simultaneously
    if (this.processing) {
        return;
    }

    // Set processing flag BEFORE checking queue to prevent race conditions
    this.processing = true;

    // Atomic check-and-shift: check length and shift in one operation
    // This prevents race conditions where jobs are added between check and shift
    if (this.pendingJobs.length === 0) {
        this.processing = false;
        return;
    }

    var next = this.pendingJobs.shift();
    if (!next) {
        // Double-check in case array was modified between length check and shift
        this.processing = false;
        return;
    }

    // Remove job ID from tracking Set
    var nextJobId = next.jobDocument._id ? next.jobDocument._id.toString() : null;
    if (nextJobId) {
        this.pendingJobIds.delete(nextJobId);
    }

    // Resume change stream if queue is getting low
    var queuePercent = (this.pendingJobs.length / this.maxPendingJobs) * 100;
    if (queuePercent < 50 && this.changeStreamPaused) {
        this.resumeChangeStream();
    }

    // Process job with its associated resume token (sequential processing, not round-robin)
    // Note: processJobInternal will set processing = false when done
    this.processJobInternal(next.queue, next.jobDocument, next.resumeToken);
};

/**
* Processes all existing jobs in queues before setting up change streams
* @param {Function} callback - called when all existing jobs are processed
*/
Worker.prototype.processAllExistingJobs = function (callback) {
    var self = this;

    if (!this.working) {
        return callback();
    }

    // Set flag to prevent work() from calling processNextJob()
    this.processingInitialJobs = true;

    // Listen for job completion to continue processing
    function onJobDone() {
        // Remove listener after first use
        self.removeListener('done', onJobDone);
        self.removeListener('failed', onJobDone);
        self.removeListener('complete', onJobDone);

        // Reset processing flag and continue processing next job
        self.processing = false;
        setTimeout(processOne, 10);
    }

    // Process jobs one at a time until no more are found
    function processOne() {
        if (!self.working) {
            self.processingInitialJobs = false;
            return callback();
        }

        // Prevent concurrent processing
        if (self.processing) {
            // If already processing, wait a bit and try again
            return setTimeout(processOne, 100);
        }

        self.processing = true;

        self.dequeue(function (err, job) {
            if (err) {
                self.processing = false;
                self.processingInitialJobs = false;
                self.emit('error', err);
                return callback();
            }

            if (job) {
                self.emit('dequeued', job.data);

                // Set up listeners for job completion
                self.once('done', onJobDone);
                self.once('failed', onJobDone);
                self.once('complete', onJobDone);

                // Process the job - it will emit 'done', 'failed', or 'complete' when finished
                self.work(job);
            } else {
                // No more jobs found, we're done
                self.processing = false;
                self.processingInitialJobs = false;
                self.emit('empty');
                callback();
            }
        });
    }

    processOne();
};

/**
* Processes the next available job from queues
* This is used for processing jobs triggered by change streams
*/
Worker.prototype.processNextJob = function () {
    if (!this.working) {
        return this.emit('stopped');
    }

    // Prevent concurrent processing
    if (this.processing) {
        return;
    }

    var self = this;
    this.processing = true;

    this.dequeue(function (err, job) {
        self.processing = false;

        if (err) {
            return self.emit('error', err);
        }

        if (job) {
            self.emit('dequeued', job.data);
            self.work(job);
        } else {
            self.emit('empty');
            // No job found, will wait for change stream to notify of new jobs
        }
    });
};

Worker.prototype.dequeue = function (callback) {
    // Process jobs sequentially (not round-robin) - check queues in order
    // This is used for delayed job polling and initial job processing
    if (this.queues.length === 0) {
        return callback(null, null);
    }

    // Try each queue in order until we find a job
    // This ensures sequential processing rather than round-robin
    var self = this;
    var queueIndex = 0;

    function tryNextQueue() {
        if (queueIndex >= self.queues.length) {
            return callback(null, null); // No jobs found in any queue
        }

        var queue = self.queues[queueIndex];
        queue.dequeue({ minPriority: self.minPriority, callbacks: self.callbacks }, function (err, job) {
            if (err) {
                return callback(err);
            }

            if (job) {
                return callback(null, job);
            }

            // No job in this queue, try next
            queueIndex++;
            tryNextQueue();
        });
    }

    tryNextQueue();
};

Worker.prototype.work = function (job) {
    var self = this;
    var finished = false;
    var timer = null; // Declare timer at function scope
    var watchdogTimer = null; // Watchdog timer to detect hung callbacks

    if (job.data.timeout) {
        timer = setTimeout(function () {
            done(new Error('timeout'));
        }, job.data.timeout);
    }

    // Set up watchdog timer to detect hung callbacks (even without job timeout)
    // This prevents the processing flag from getting stuck forever
    // Use job timeout if available, otherwise use default watchdog timeout
    // Ensure watchdog is at least 10 seconds longer than job timeout, but not less than default
    var watchdogTimeout;
    if (job.data.timeout) {
        // Use job timeout + 10 seconds, but ensure it's at least the default watchdog timeout
        watchdogTimeout = Math.max(job.data.timeout + 10000, this.jobCallbackWatchdogTimeout);
    } else {
        watchdogTimeout = this.jobCallbackWatchdogTimeout;
    }
    watchdogTimer = setTimeout(function () {
        if (!finished) {
            self.emit('error', new Error('Job callback appears to be hung - no response after ' + watchdogTimeout + 'ms'));
            // Force completion to prevent worker from getting stuck
            done(new Error('Job callback watchdog timeout'));
        }
    }, watchdogTimeout);

    function done(err, result) {
        // It's possible that this could be called twice in the case that a job times out,
        // but the handler ends up finishing later on
        if (finished) {
            return;
        } else {
            finished = true;
        }

        if (timer) {
            clearTimeout(timer);
        }

        if (watchdogTimer) {
            clearTimeout(watchdogTimer);
        }

        self.emit('done', job.data);

        if (err) {
            self.error(job, err, function (err) {
                if (err) return self.emit('error', err);

                self.emit('failed', job.data);

                // Don't add to recentlyProcessedJobs on failure - allow immediate retry
                // Remove from recentlyProcessedJobs if it was added (shouldn't be, but just in case)
                var jobId = job.data._id ? job.data._id.toString() : null;
                if (jobId) {
                    self.recentlyProcessedJobs.delete(jobId);
                }

                // Save resume token after job completes (not after claim)
                // This ensures we only advance resume token for completed jobs
                // Save immediately (no debounce) to prevent loss on crash
                // Note: lastResumeToken is updated in flushResumeToken after successful save
                if (job._resumeToken) {
                    // Save immediately with callback to ensure it's persisted
                    // Don't update lastResumeToken here - let flushResumeToken do it after successful save
                    self.saveResumeToken(self.db, job._resumeToken, function (err) {
                        if (err) {
                            self.emit('error', new Error('Failed to save resume token: ' + err.message));
                            // Don't update lastResumeToken if save failed
                        }
                    });
                    job._resumeToken = null; // Clear
                }

                // Reset processing flag and process any pending jobs that arrived while we were processing
                self.processing = false;
                self.processNextPendingJob();
            });
        } else {
            job.complete(result, function (err) {
                if (err) return self.emit('error', err);

                self.emit('complete', job.data);

                // Add to recentlyProcessedJobs only on successful completion
                // This prevents skipping jobs that fail immediately after claim
                var jobId = job.data._id ? job.data._id.toString() : null;
                if (jobId) {
                    // Mark as recently processed to skip duplicate events (with timestamp for TTL)
                    self.recentlyProcessedJobs.set(jobId, Date.now());
                    // Clean up if map is too large
                    if (self.recentlyProcessedJobs.size > self.recentlyProcessedJobsMaxSize) {
                        // Cleanup will be handled by the cleanup interval
                    }
                }

                // Save resume token after job completes (not after claim)
                // This ensures we only advance resume token for completed jobs
                // Save immediately (no debounce) to prevent loss on crash
                // Note: lastResumeToken is updated in flushResumeToken after successful save
                if (job._resumeToken) {
                    // Save immediately with callback to ensure it's persisted
                    // Don't update lastResumeToken here - let flushResumeToken do it after successful save
                    self.saveResumeToken(self.db, job._resumeToken, function (err) {
                        if (err) {
                            self.emit('error', new Error('Failed to save resume token: ' + err.message));
                            // Don't update lastResumeToken if save failed
                        }
                    });
                    job._resumeToken = null; // Clear
                }

                // Reset processing flag and process any pending jobs that arrived while we were processing
                self.processing = false;
                self.processNextPendingJob();
            });
        }
    };

    this.process(job.data, done);
};

Worker.prototype.process = function (data, callback) {
    var func = this.callbacks[data.name];

    if (!func) {
        return callback(new Error('No callback registered for `' + data.name + '`'));
    }

    // Wrap callback invocation in try-catch to handle synchronous errors
    // and prevent processing flag from getting stuck
    try {
        var called = false;
        var wrappedCallback = function (err, result) {
            // Prevent callback from being called multiple times
            if (called) {
                return;
            }
            called = true;
            callback(err, result);
        };

        // Invoke the job callback
        func(data.params, wrappedCallback);

        // Note: In Node.js, callbacks are typically asynchronous, but if a callback
        // is synchronous and doesn't call wrappedCallback, we can't detect it here.
        // The timeout mechanism in work() will handle hung callbacks if timeout is set.
    } catch (err) {
        // Handle synchronous errors thrown by the callback function
        callback(err);
    }
};

Worker.prototype.error = function (job, err, callback) {
    var attempts = job.data.attempts;
    var remaining = 0;

    if (attempts) {
        remaining = attempts.remaining = (attempts.remaining || attempts.count) - 1;
    }

    if (remaining > 0) {
        var strategy = this.strategies[attempts.strategy || 'linear'];
        if (!strategy) {
            strategy = linear;

            console.error('No such retry strategy: `' + attempts.strategy + '`');
            console.error('Using linear strategy');
        }

        if (attempts.delay !== undefined) {
            var wait = strategy(attempts);
        } else {
            var wait = 0;
        }

        job.delay(wait, callback)
    } else {
        job.fail(err, callback);
    }
};

// Strategies
// ---------------

function linear(attempts) {
    return attempts.delay;
}

function exponential(attempts) {
    return attempts.delay * (attempts.count - attempts.remaining);
}

/**
* Starts connection health monitoring to detect connection loss
*/
Worker.prototype.startConnectionHealthMonitoring = function () {
    var self = this;

    // Clear existing interval
    if (this.connectionCheckInterval) {
        clearInterval(this.connectionCheckInterval);
    }

    // Check connection health periodically
    this.connectionCheckInterval = setInterval(function () {
        if (!self.working || !self.db) {
            return;
        }

        // Double-check working flag to prevent operations after stop
        if (!self.working) {
            return;
        }

        // Double-check working flag to prevent operations after stop
        if (!self.working) {
            return;
        }

        // Try a simple operation to check connection
        // Use admin command as it's lightweight
        try {
            self.db.admin().ping(function (err) {
                if (err) {
                    // Connection lost
                    if (!self.dbConnectionLost) {
                        self.dbConnectionLost = true;
                        self.emit('warning', 'Database connection lost - resume token saves will be queued');
                    }
                } else {
                    // Connection restored
                    if (self.dbConnectionLost) {
                        self.dbConnectionLost = false;
                        self.emit('info', 'Database connection restored - processing queued resume token saves');
                        // Process queued saves
                        self.processPendingResumeTokenSavesOnReconnect();
                    }
                }
            });
        } catch (e) {
            // If ping fails, mark as disconnected
            if (!self.dbConnectionLost) {
                self.dbConnectionLost = true;
                self.emit('warning', 'Database connection check failed: ' + e.message);
            }
        }
    }, this.connectionCheckIntervalMs);
};

/**
* Starts cleanup interval for recently processed jobs tracking
* Removes old entries based on TTL to prevent memory leaks
*/
Worker.prototype.startRecentlyProcessedJobsCleanup = function () {
    var self = this;

    // Clear existing interval
    if (this.recentlyProcessedJobsCleanupInterval) {
        clearInterval(this.recentlyProcessedJobsCleanupInterval);
    }

    // Clean up recently processed jobs map periodically based on TTL
    this.recentlyProcessedJobsCleanupInterval = setInterval(function () {
        if (!self.working) {
            return;
        }

        // Double-check working flag to prevent operations after stop
        if (!self.working) {
            return;
        }

        var now = Date.now();
        var expired = [];

        // Find expired entries
        self.recentlyProcessedJobs.forEach(function (timestamp, jobId) {
            var age = now - timestamp;
            if (age > self.recentlyProcessedJobsTTL) {
                expired.push(jobId);
            }
        });

        // Remove expired entries
        expired.forEach(function (jobId) {
            self.recentlyProcessedJobs.delete(jobId);
        });

        // If map is still too large after cleanup, remove oldest entries
        // Optimize: Use a more efficient approach - collect timestamps and remove in one pass
        if (self.recentlyProcessedJobs.size > self.recentlyProcessedJobsMaxSize) {
            var toRemove = self.recentlyProcessedJobs.size - self.recentlyProcessedJobsMaxSize;
            var entries = [];

            // Collect all entries with timestamps in one pass
            self.recentlyProcessedJobs.forEach(function (timestamp, jobId) {
                entries.push({ jobId: jobId, timestamp: timestamp });
            });

            // Sort by timestamp (oldest first) - only sort what we need
            entries.sort(function (a, b) {
                return a.timestamp - b.timestamp;
            });

            // Remove oldest entries (limit to reasonable number to avoid blocking)
            var removeCount = Math.min(toRemove, entries.length);
            for (var i = 0; i < removeCount; i++) {
                self.recentlyProcessedJobs.delete(entries[i].jobId);
            }
        }
    }, this.recentlyProcessedJobsTTL / 2); // Run cleanup at half the TTL interval
};

/**
* Pauses the change stream to implement backpressure
* NOTE: This doesn't actually pause MongoDB change streams - it only ignores events in the handler.
* MongoDB will continue sending events, which may be lost if the buffer overflows.
* Events that arrive during pause may be missed when resuming (resume token advances past them).
* For production use, consider closing and recreating the stream with a resume token instead.
*/
/**
* Pauses the change stream by closing it and saving the resume token
* This prevents MongoDB from sending events and avoids buffer overflow
* The stream will be recreated with the saved resume token when resumed
*/
Worker.prototype.pauseChangeStream = function () {
    var self = this;

    if (this.changeStream && !this.changeStreamPaused) {
        this.changeStreamPaused = true;
        this.emit('info', 'Pausing change stream due to backpressure');

        // Save the current resume token before closing
        // We need to capture the latest token from the last processed event
        var resumeTokenToSave = this.pendingResumeTokenSave || this.lastResumeToken;

        // Close the change stream to actually pause MongoDB from sending events
        // This prevents buffer overflow and event loss
        if (this.changeStream) {
            var streamToClose = this.changeStream;
            this.changeStream = null; // Clear reference immediately to prevent new events

            // Wait for any pending resume token saves to complete before pausing
            function waitForPendingSavesAndSave(callback) {
                // First, wait for processing queue to finish
                var maxWaitTime = 5000; // 5 seconds max wait for graceful pause
                var startTime = Date.now();
                var attempts = 0;
                var maxAttempts = 100; // 100 attempts * 50ms = 5 seconds

                function waitForProcessingQueue() {
                    if (!self.processingResumeTokenSaves || self.resumeTokenSaveProcessingQueue.length === 0) {
                        // Processing queue is empty, now save the token directly
                        if (resumeTokenToSave) {
                            // Set as pending and flush directly to ensure it's saved before closing
                            self.pendingResumeTokenSave = resumeTokenToSave;
                            self.flushResumeToken(function (err) {
                                if (err) {
                                    self.emit('error', new Error('Failed to save resume token before pause: ' + err.message));
                                    // Still close the stream even if save failed - better than hanging
                                    return callback();
                                }
                                // Token saved successfully, safe to close stream
                                callback();
                            });
                        } else {
                            // No token to save, safe to close
                            callback();
                        }
                        return;
                    }

                    attempts++;
                    var elapsed = Date.now() - startTime;

                    // Check if we've exceeded max wait time or attempts
                    if (elapsed >= maxWaitTime || attempts >= maxAttempts) {
                        self.emit('warning', 'Timeout waiting for resume token save processing to finish before pause - proceeding anyway');
                        // Proceed with pause even if queue hasn't finished - better than hanging
                        if (resumeTokenToSave) {
                            // Try to save directly, but don't wait
                            self.pendingResumeTokenSave = resumeTokenToSave;
                            self.flushResumeToken(function (err) {
                                if (err) {
                                    self.emit('error', new Error('Failed to save resume token before pause: ' + err.message));
                                }
                                callback();
                            });
                        } else {
                            callback();
                        }
                        return;
                    }

                    // Wait a bit and check again
                    setTimeout(waitForProcessingQueue, 50);
                }

                waitForProcessingQueue();
            }

            // Wait for pending saves and save the token, then close the stream
            waitForPendingSavesAndSave(function () {
                streamToClose.close(function (err) {
                    if (err) {
                        self.emit('error', new Error('Error closing change stream for pause: ' + err.message));
                    }
                });
            });
        }
    }
};

/**
* Resumes the change stream by recreating it with the saved resume token
* This ensures we resume from the correct position without missing events
*/
Worker.prototype.resumeChangeStream = function () {
    var self = this;

    if (!this.changeStream && this.changeStreamPaused && this.working && this.db && this.collectionName) {
        this.changeStreamPaused = false;
        this.emit('info', 'Resuming change stream from saved resume token');

        // Recreate the change stream with the saved resume token
        // This ensures we resume from the correct position
        // Note: Any events that occurred during the pause will be missed, but this is
        // better than buffer overflow and losing events randomly
        // Jobs that were queued during pause will be picked up by delayed job polling
        this.setupChangeStreams(this.db, this.collectionName);
    } else if (this.changeStream && this.changeStreamPaused) {
        // Stream still exists but was marked as paused - this shouldn't happen
        // but handle it gracefully
        this.changeStreamPaused = false;
        this.emit('warning', 'Change stream was marked paused but stream still exists - clearing pause flag');
    }
};
