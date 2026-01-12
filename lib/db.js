exports.index = function (collection) {
    collection.getIndexes(function(err, indexes) {
        if (err) {
            if (err.code === 26) { 
                // MongoError: no collection
                return;
            } 
            return console.log(err);
        }

        dropIndex('status_1_queue_1_enqueued_1');
        dropIndex('status_1_queue_1_enqueued_1_delay_1');

        function dropIndex(name) {
            if (indexes.some(function(index) { return index.name == name; })) {
                collection.dropIndex(name, function(err) {
                    if (err) { console.error(err); }
                });
            }
        }
    });

    // Primary index for dequeue operations (polling-based)
    // Query: { status: 'queued', delay: { $lte: Date }, queue: name, priority: { $gte: minPriority }, name: { $in: [...] } }
    // Sort: { priority: -1, _id: 1 }
    // Status is first b/c querying by status = queued should be very selective
    // Includes name field for callback filtering
    collection.ensureIndex({ 
        status: 1, 
        queue: 1, 
        priority: -1, 
        _id: 1, 
        delay: 1,
        name: 1 
    }, function (err) {
        if (err) console.error(err);
    });

    // Index for dequeueFromChangeStream - atomic job claiming by _id and status
    // Query: { _id: jobId, status: 'queued' }
    // _id is already indexed, but compound index helps with the status check
    collection.ensureIndex({ 
        _id: 1, 
        status: 1 
    }, function (err) {
        if (err) console.error(err);
    });

    // Index for queue filtering in change streams and general queue queries
    // Query: { queue: queueName } or { queue: { $in: [...] } }
    collection.ensureIndex({ 
        queue: 1 
    }, function (err) {
        if (err) console.error(err);
    });

    // Index for status queries (used in various operations)
    // Query: { status: 'queued' } or { status: 'dequeued' } etc.
    collection.ensureIndex({ 
        status: 1 
    }, function (err) {
        if (err) console.error(err);
    });
};

/**
* Creates indexes for the resume token collection
* @param {Object} collection - MongoDB collection for resume tokens
*/
exports.indexResumeTokens = function (collection) {
    // Index for workerId lookups (primary query pattern)
    // Query: { workerId: workerId }
    collection.ensureIndex({ 
        workerId: 1 
    }, { unique: true }, function (err) {
        if (err) console.error(err);
    });
    
    // TTL index to automatically clean up stale resume tokens
    // Tokens older than 7 days are considered stale (workers that crashed and never restarted)
    // This prevents database bloat from abandoned workers
    collection.ensureIndex({ 
        updatedAt: 1 
    }, { 
        expireAfterSeconds: 604800 // 7 days in seconds
    }, function (err) {
        if (err) console.error(err);
    });
};
