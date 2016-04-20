var Promise = require('es6-promise').Promise,
    redis = require('redis'),
    MongoClient = require('mongodb').MongoClient,
    fs = require('fs'),
    argv = require('minimist')(process.argv.slice(2), {
        alias: { d: 'flushdb' },
        boolean: ['flushdb', 'redis', 'mongo']
    }),
    debug = require('debug'),
    fsDebug = debug('fs:debug'),
    fsError = debug('fs:error');

var redisModel = {
    logDebug: false,
    logError: false,
    client: false,

    cityPlzMap: {},

    init: function() {
        var model = this;
        return new Promise(function(resolve) {
            model.logDebug = debug('redis:debug');
            model.logError = debug('redis:error');

            model.logDebug('init');

            model.client = redis.createClient();

            model.client.on('error', function (err) {
                model.logError(err);
            });

            model.client.unref();

            resolve();
        });
    },

    addEntry: function(entryId, jsonData) {
        var model = this;
        return new Promise(function(resolve) {
            model.logDebug('add entry for %s', entryId);
            model.client.hmset(entryId, "city", jsonData.city, "loc", JSON.stringify(jsonData.loc), "pop", jsonData.pop, "state", jsonData.state);

            if (!model.cityPlzMap[jsonData.city]) {
                model.cityPlzMap[jsonData.city] = [];
            }
            model.cityPlzMap[jsonData.city].push(entryId);

            resolve();
        });
    },

    finishImport: function() {
        this.logDebug('Insert city to postal maps.');
        for (var key in this.cityPlzMap) {
            if (this.cityPlzMap.hasOwnProperty(key)) {
                this.client.set(key, JSON.stringify(this.cityPlzMap[key]));
            }
        }
    },

    flushdb: function() {
        var model = this;
        return new Promise(function(resolve) {
            model.logDebug('Flush database');
            model.client.flushdb();
            resolve();
        });
    }
};

var mongoModel = {
    logDebug: false,
    logError: false,
    db : false,
    collection: false,

    init: function() {
        var model = this;
        return new Promise(function(resolve, reject) {

            model.logDebug = debug('mongo:debug');
            model.logError = debug('mongo:error');

            var url = 'mongodb://localhost:27017/postal';
            MongoClient.connect(url, function (err, db) {
                if(err) {
                    reject(err);
                    return;
                }

                model.logDebug('db connection established');
                model.db = db;
                model.collection = db.collection('postals');
                resolve();
            });
        });
    },

    flushdb: function() {
        this.logDebug('Flush database');
        return this.collection.deleteMany({});
    },

    addEntry: function(entryId, jsonData) {
        this.logDebug('add entry for %s', entryId);
        return this.collection.insertOne(jsonData);
    },

    finishImport: function() {
        this.db.close();
    }
};

function importFile(file) {
    return new Promise(function(resolve, reject) {
        fs.readFile(file, function (err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(parseStrSync(data.toString()));
            }
        });
    });
}

function parseStrSync(str) {
    return new Promise(function(resolve, reject) {
        var offset = 0;
        var fn = function() {
            var importPromiseList = [];
            var endPos = str.indexOf("\n", offset);
            var part = str.substr(offset, endPos - offset);
            try {
                var jsonData = JSON.parse(part),
                    entryId = jsonData._id;

                activeModelList.forEach(function (model) {
                    importPromiseList.push(model.addEntry(entryId, jsonData));
                });
            } catch (err) {
                fsError('Error parsing', part, err);
            }
            offset = endPos + 1;

            return Promise.all(importPromiseList);
        };

        var wrapperFn = function() {
            if (str.indexOf("\n", offset) !== -1) {
                fsDebug('Import dataset');
                fn().then(wrapperFn, reject);
            } else {
                if (offset < str.length) {
                    fsError('Document must end with EOL');
                }

                resolve();
            }
        };

        wrapperFn();
    });
}

var activeModelList = [];
var sequence = Promise.resolve();

sequence.then(function() {
    if (argv.redis) {
        activeModelList.push(redisModel);
        return redisModel.init();
    }
}).then(function() {
    if (argv.mongo) {
        activeModelList.push(mongoModel);
        return mongoModel.init();
    }
}).then(function() {
    if (argv.flushdb) {
        var flushPromises = [];
        activeModelList.forEach(function(model) {
            flushPromises.push(model.flushdb());
        });
        return Promise.all(flushPromises);
    }
}).then(function() {
    var importPromiseList = [];
    argv._.forEach(function(file) {
        fsDebug('Import file %s', file);
        importPromiseList.push(importFile(file));
    });
    return Promise.all(importPromiseList);
}).then(function() {
    activeModelList.forEach(function(model) {
        model.finishImport();
    });
}).then(function() {
    fsDebug('All done');
}, function(err) {
    fsError(err);
});
