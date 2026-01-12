var mongo = require('mongojs');
var mongodb = require('mongodb');
var assert = require('assert');

// Test if mongojs (v3.1) can query using an ObjectId from mongodb (v6.3)
var uri = process.env.MONGO_URI || 'mongodb://localhost:27017/monq_test_compatibility';
var db = mongo(uri);
var collection = db.collection('compatibility_test');

async function testCompatibility() {
    console.log('Testing driver compatibility...');

    // 1. Create a native ObjectId (v6 style)
    var nativeId = new mongodb.ObjectId();
    console.log('Native ID created:', nativeId.toString());

    // 2. Insert using mongojs with this ID? 
    // Usually mongojs creates IDs automatically, but we can force one.
    // Let's try inserting with mongojs first to get a "mongojs" document.
    var doc = { name: 'test', _id: nativeId }; // Using native ID

    // We wrap mongojs output in a promise for easier testing
    await new Promise((resolve, reject) => {
        collection.save(doc, function (err, result) {
            if (err) reject(err);
            else resolve(result);
        });
    });
    console.log('Inserted document using mongojs with Native ID.');

    // 3. Query using mongojs passing the Native ID object directly
    // This simulates: collection.findAndModify({ query: { _id: jobDocument._id } })
    // where jobDocument._id comes from the change stream (Native)
    var result = await new Promise((resolve, reject) => {
        collection.findOne({ _id: nativeId }, function (err, doc) {
            if (err) reject(err);
            else resolve(doc);
        });
    });

    if (result && result._id.toString() === nativeId.toString()) {
        console.log('SUCCESS: mongojs found document using Native ObjectId.');
    } else {
        console.log('FAILURE: mongojs did NOT find document using Native ObjectId.');
        console.log('Result:', result);
    }

    // 4. Reverse: Can we compare them?
    // mongojs ID vs Native ID
    // mongojs doesn't expose ObjectId constructor easily in this context without requiring it explicitly
    // but the driver's 'mongo' import does.

    var jsId = mongo.ObjectId(nativeId.toString());
    console.log('MongoJS ID:', jsId);
    console.log('Native ID: ', nativeId);

    // Check strict equality (unlikely)
    console.log('Strict equality:', jsId === nativeId); // False

    // Check toString equality
    console.log('toString equality:', jsId.toString() === nativeId.toString());

    // Clean up
    await new Promise((resolve) => collection.remove({}, resolve));
    db.close();
}

testCompatibility().catch(err => {
    console.error('Test failed:', err);
    process.exit(1);
});
