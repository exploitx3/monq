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
    console.log('Starting scaling verification test...');

    // Worker A - listens to 'queueA'
    var workerA = client.worker(['queueA']);
    workerA.name = 'Worker A';

    // Worker B - listens to 'queueB'
    var workerB = client.worker(['queueB']);
    workerB.name = 'Worker B';

    // Spy on processJobFromChangeStream
    var originalProcessA = workerA.processJobFromChangeStream;
    var originalProcessB = workerB.processJobFromChangeStream;

    var receivedA = 0;
    var receivedB = 0;

    workerA.processJobFromChangeStream = function (queue, job, token) {
        console.log('Worker A received job for queue:', job.queue);
        receivedA++;
        // Create full dummy job object to satisfy signature if needed, or just pass through
        // but for this test we mainly care if this method was CALLED
        originalProcessA.apply(this, arguments);
    };

    workerB.processJobFromChangeStream = function (queue, job, token) {
        console.log('Worker B received job for queue:', job.queue);
        receivedB++;
        originalProcessB.apply(this, arguments);
    };

    workerA.register({
        test: function (params, callback) {
            callback(null, 'done');
        }
    });

    workerB.register({
        test: function (params, callback) {
            callback(null, 'done');
        }
    });

    // Start workers
    workerA.start();
    workerB.start();

    console.log('Workers started. Waiting for connection...');

    // Allow time for change streams to establish
    setTimeout(function () {
        console.log('Enqueuing jobs...');

        // Enqueue job for A
        var queueA = client.queue('queueA');
        queueA.enqueue('test', { foo: 'bar' }, function (err, jobA) {
            if (err) throw err;
            console.log('Enqueued job to queueA:', jobA.data._id);
        });

        // Enqueue job for B
        var queueB = client.queue('queueB');
        queueB.enqueue('test', { bar: 'baz' }, function (err, jobB) {
            if (err) throw err;
            console.log('Enqueued job to queueB:', jobB.data._id);
        });

        // Wait for processing
        setTimeout(function () {
            console.log('Checking results...');

            console.log('Worker A received:', receivedA);
            console.log('Worker B received:', receivedB);

            var success = true;

            if (receivedA !== 1) {
                console.error('FAIL: Worker A should have received exactly 1 job, got', receivedA);
                success = false;
            }

            if (receivedB !== 1) {
                console.error('FAIL: Worker B should have received exactly 1 job, got', receivedB);
                success = false;
            }

            if (success) {
                console.log('SUCCESS: Each worker only received jobs for their subscribed queues.');
            } else {
                console.log('FAILURE: Change stream filtering might be incorrect (watch all behavior).');
            }

            workerA.stop();
            workerB.stop();
            db.close();
            process.exit(success ? 0 : 1);

        }, 2000);

    }, 2000);
}
