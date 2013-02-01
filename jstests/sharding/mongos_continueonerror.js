// mongos_continueonerror.js
//
// Tests that mongos handles errors correctly for either value of continueonerror

var applyDefaults = function(options, defaults) {
    var combined = {};
    for (var name in options) {
        combined[name] = options[name];
    }
    for (var name in defaults) {
        if (combined[name] === undefined) { combined[name] = defaults[name]; }
    }
    return combined;
}

var enableSharding = function(mongos, database, collection) {
    var admin = mongos.getDB("admin");
    admin.runCommand({ enableSharding : database._name });
    admin.runCommand({ shardCollection : collection._fullName, key : { _sk : 1 }});
}

var chooseRandomIndexes = function(length, count) {
    var indexes = [];
    for (var i = 0; i < length; i++) {
        indexes[i] = i;
    }

    var chosen = [];
    while (length > 0 && count > 0) {
        var n = randomNumberBetween(0, length - 1);
        chosen.push(indexes[n]);
        indexes.splice(n, 1); // remove element n
        length -= 1;
        count -= 1;
    }

    chosen.sort();
    return chosen;
}

var createErrorsInBatch = function(batch, options) {
    var numberOfErrors = randomNumberBetween(options.numberOfErrors.min, options.numberOfErrors.max);
    var indexes = chooseRandomIndexes(batch.length, numberOfErrors);

    if (indexes.length > 0) {
        if (options.firstDocumentShouldHaveError) {
            indexes[0] = 0;
        } if (options.lastDocumentShouldHaveError && (indexes.length >= 2 || !options.firstDocumentShouldHaveError)) {
            indexes[indexes.length - 1] = batch.length - 1;
        }

        for (var index in indexes) {
            if (options.errorLocation == "mongos") {
                // removing the shard key causes a missing shard key error in mongos
                delete batch[index]._sk;
            }
            else if (options.errorLocation == "mongod")  {
                // setting the _id to -1 causes a duplicate key error in mongod
                batch[index]._id = -1;
            }
            else {
                assert(false, "invalid errorLocation");
            }
        }
    }
}

var randomNumberBetween = function(min, max) {
    return min + Math.floor(Math.random() * (max + 1));
}

var resetCollection = function(collection) {
    print("resetting collection");
    collection.remove(); // use remove instead of drop so collection will still be sharded
    collection.insert({ _id : -1, _sk : -1 }); // _id = -1 is used to create duplicate keys
    print("count({ _id : -1 }) = " + collection.count({ _id : -1 }));
}

var runTests = function(mongos, options) {
    var database = mongos.getDB("testDatabase");
    var collection = database.testCollection;
    enableSharding(mongos, database, collection);

    [false, true].forEach(function(continueOnError) {
        ["mongos", "mongod"].forEach(function(errorLocation) {
            [false, true].forEach(function(firstDocumentShouldHaveError) {
                [false,true].forEach(function(lastDocumentShouldHaveError) {
                    [false, true].forEach(function(scrambleShardKey) {
                        options.continueOnError = continueOnError;
                        options.errorLocation = errorLocation;
                        options.firstDocumentShouldHaveError = firstDocumentShouldHaveError;
                        options.lastDocumentShouldHaveError = lastDocumentShouldHaveError;
                        options.scrambleShardKey = scrambleShardKey;

                        resetCollection(collection);
                        testBatches(collection, options);
                    })
                })
            })
        })
    });
}

var scrambleInteger = function(n) {
    // only works for values up to 3 bytes long
    return ((n << 16) & 0xff0000) | (n & 0xff00) | ((n >> 16) & 0xff);
}

var setupSharding = function(mongos) {
}

var startCluster = function(options) {
    var defaults = {
        shards : 2,
        chunksize : 1,
        config : 3,
        separateConfig : true,
        other : {
            nopreallocj : 1
        }
    };
    options = applyDefaults(options, defaults);

    return new ShardingTest(options);
}

var __strings = ["x"];
for (var i = 1; i < 25; i++) {
    __strings[i] = __strings[i - 1] + __strings[i - 1];
    null;
}

var stringOfLength = function(n) {
    var s = "";
    for (var i = 0; true; i++) {
        if (n <= 0) { return s; }
        if ((n & 1) != 0) { s = s + __strings[i]; }
        n = n >> 1;
    }
}

var testBatch = function(collection, nextId, options) {
    var baseDocument = { _id : 0, _sk : 0, filler : "" };
    var baseSize = Object.bsonsize(baseDocument);

    var batch = [];
    var targetSize = randomNumberBetween(options.batchSizes.min, options.batchSizes.max);
    var batchSize = 0;
    var id = nextId;
    for (var d = 0; batchSize < targetSize; d++) {
        var documentSize = randomNumberBetween(options.documentSizes.min, options.documentSizes.max);
        var document = { _id : id, _sk : id, filler : stringOfLength(documentSize - baseSize) };
        if (options.scrambleShardKey) {
            document._sk = scrambleInteger(document._sk);
        }
        batchSize += documentSize;

        batch[d] = document;
        id++;
    }

    createErrorsInBatch(batch, options);

    var insertFlags = options.continueOnError ? 1 : 0;
    collection.insert(batch, insertFlags);
    verifyBatch(collection, batch, options);

    return id;
}

var testBatches = function(collection, options) {
    var defaults = {
        iterations : 100,
        batchSizes : { min : 1000, max : 1000000 },
        documentSizes : { min : 1000, max : 20000 },
        numberOfErrors  : { min : 0, max : 10 },
        continueOnError : false
    };
    options = applyDefaults(options, defaults);

    print("Testing batches with the following options:");
    print("iterations: " + options.iterations);
    print("batchSizes: " + JSON.stringify(options.batchSizes));
    print("documentSizes: " + JSON.stringify(options.documentSizes));
    print("numberOfErrors: " + options.numberOfErrors);
    print("continueOnError: " + options.continueOnError);
    print("firstDocumentShouldHaveError: " + options.firstDocumentShouldHaveError);
    print("lastDocumentShouldHaveError: " + options.lastDocumentShouldHaveError);
    print("scrambleShardKey: " + options.scrambleShardKey);

    var nextId = 1;
    for (var i = 0; i < options.iterations; i++) {
        print("iteration: " + i);
        nextId = testBatch(collection, nextId, options);
    }
}

var verifyBatch = function(collection, batch, options) {
    var count = collection.count({ _id : -1 });
    assert(count == 1, "somehow the collection ended up with " + count + " documents matching { _id : -1 }");

    var afterFirstError = false;

    for (var i = 0; i < batch.length; i++) {
        var document = batch[i];
        var count = collection.count({ _id : document._id });

        if (document._id == -1) {
            assert(count == 1);
            afterFirstError = true;
        } else if (document._sk === undefined) {
            assert(count == 0);
            afterFirstError = true;
        } else {
            assert(count == afterFirstError && !options.continueOnError ? 0 : 1);
        }
    }

    print("iteration ok (" + batch.length + " documents in batch)");
}

var main = function(options) {
    var start = new Date();

    var options = {
        sharding : {
            config : 3
        },
        batches : {
            iterations : 2,
            batchSizes : { min : 1, max : 1000000 },
            documentSizes : { min : 1000, max : 2000 },
            numberOfErrors : { min : 0, max : 10 }
        }
    }

    print("Starting cluster.");
    var cluster = startCluster(options.sharding);
    var mongos = new Mongo(cluster.s.host);

    print("Running tests.");
    runTests(mongos, options.batches);

    print("Stopping cluster.");
    cluster.stop();

    var end = new Date();
    print("Test completed in " + (end - start) / 1000 + " seconds.");
}

main();
