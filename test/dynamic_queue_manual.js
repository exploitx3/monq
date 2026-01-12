var mongo = require('mongojs');
var monq = require('../index');
var assert = require('assert');

// Connection string - change if needed
var uri = process.env.MONGO_URI || 'mongodb://localhost:27017/monq_test';
var client = monq(uri);

// Clean up
var db = mongo(uri);
db.collection('jobs').remove({}, function () {
    console.log('Cleaned jobs collection');
    startTest();
});

function startTest() {
    console.log('Starting dynamic queue verification test...');

    // Worker listens ONLY to 'initialQueue' at first
    var worker = client.worker(['initialQueue']);
    worker.name = 'Dynamic Worker';

    var receivedInitial = 0;
    var receivedDynamic = 0;

    // Spy
    var originalProcess = worker.processJobFromChangeStream;

    worker.processJobFromChangeStream = function (queue, job, token) {
        console.log('Worker received job for queue:', job.queue);
        if (job.queue === 'initialQueue') receivedInitial++;
        if (job.queue === 'dynamicQueue') receivedDynamic++;
        originalProcess.apply(this, arguments);
    };

    worker.register({
        test: function (params, callback) {
            callback(null, 'done');
        }
    });

    // Start worker
    worker.start();
    console.log('Worker started on [initialQueue]. Waiting for connection...');

    setTimeout(function () {
        // 1. Enqueue job for initialQueue - should be received
        console.log('Enqueuing job to initialQueue...');
        var initialQueue = client.queue('initialQueue');
        initialQueue.enqueue('test', { valid: true }, function (err, job) {
            if (err) throw err;
            console.log('Enqueued initial job');
        });

        // 2. Enqueue job for dynamicQueue - should NOT be received yet (filtered out)
        console.log('Enqueuing job to dynamicQueue (before addQueue)...');
        var dynamicQueue = client.queue('dynamicQueue');
        dynamicQueue.enqueue('test', { ignored: true }, function (err, job) {
            if (err) throw err;
            console.log('Enqueued pre-dynamic job');
        });

        setTimeout(function () {
            // Check intermediate state
            console.log('Checks after 2s:');
            console.log('- Initial received:', receivedInitial);
            console.log('- Dynamic received:', receivedDynamic);

            if (receivedInitial !== 1) console.error('FAIL: Should have received initial job');
            if (receivedDynamic !== 0) console.error('FAIL: Should NOT have received dynamic job yet');

            // 3. Add dynamicQueue
            console.log('Adding dynamicQueue to worker...');
            worker.addQueue('dynamicQueue');

            // Allow time for change stream refresh
            setTimeout(function () {
                // 4. Enqueue ANOTHER job to dynamicQueue - SHOULD be received now
                console.log('Enqueuing job to dynamicQueue (after addQueue)...');
                dynamicQueue.enqueue('test', { caught: true }, function (err, job) {
                    if (err) throw err;

                    setTimeout(function () {
                        console.log('Final checks:');
                        console.log('- Initial received:', receivedInitial);
                        console.log('- Dynamic received:', receivedDynamic);

                        // Note: The pre-dynamic job might be picked up now if the worker 
                        // processes existing jobs on restart/refresh, or it might stay queued. 
                        // We definitely expect AT LEAST 1 dynamic job (the new one).
                        // If "receivedDynamic >= 1", we confirm the filter updated.

                        var success = receivedInitial === 1 && receivedDynamic >= 1;

                        if (success) {
                            console.log('SUCCESS: Dynamic queue addition updated the filter.');
                        } else {
                            console.log('FAILURE: Filter did not update.');
                        }

                        worker.stop();
                        db.close();
                        process.exit(success ? 0 : 1);
                    }, 2000);
                });
            }, 1000);
        }, 2000);
    }, 2000);
}
