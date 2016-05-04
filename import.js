var Promise = require('es6-promise').Promise,
    hbase = require('hbase'),
    fs = require('fs'),
    argv = require('minimist')(process.argv.slice(2), {
        alias: { d: 'flushdb' },
        boolean: ['flushdb']
    }),
    debug = require('debug'),
    fsDebug = debug('fs:debug'),
    fsError = debug('fs:error');

var hbaseModel = {
    logDebug: false,
    logError: false,
    client : false,
    plzTable: false,

    init: function() {
        var model = this;
        return new Promise(function(resolve, reject) {

            model.logDebug = debug('mongo:debug');
            model.logError = debug('mongo:error');

            model.client = new hbase.Client({ host: '127.0.0.1', port: 8080 });
            model.plzTable = new hbase.Table(model.client, 'plz');

            model.plzTable.exists(function(err, data) {
                console.log(err);
                console.log(data);
                reject(Error('Testing'));
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
    activeModelList.push(hbaseModel);
    return hbaseModel.init();
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
