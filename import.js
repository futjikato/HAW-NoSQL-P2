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

            model.logDebug = debug('hbase:debug');
            model.logError = debug('hbase:error');

            model.client = new hbase.Client({ host: process.env.HBASEHOST, port: 8080 });
            model.plzTable = new hbase.Table(model.client, 'plz');

            model.plzTable.exists(function(err, data) {
                if (err)
                    return reject(err);

                if (data === false) {
                    model.logDebug('create table...');
                    model.plzTable.create({
                        ColumnSchema: [
                            {
                                name: 'location'
                            },
                            {
                                name: 'Fussball'
                            }
                        ]
                    }, function(err) {
                        if (err)
                            return reject(err);

                        resolve();
                    });
                } else {
                    resolve();
                }
            });
        });
    },

    addEntry: function(entryId, jsonData) {
        var model = this;
        return new Promise(function(resolve, reject) {
            model.logDebug('add entry for %s', entryId);
            var cells = [
                {column: 'location:plz', $: entryId},
                {column: 'location:city', $: jsonData.city},
                {column: 'location:lat', $: jsonData.loc[0].toString()},
                {column: 'location:lng', $: jsonData.loc[1].toString()},
                {column: 'location:pop', $: jsonData.pop.toString()},
                {column: 'location:state', $: jsonData.state}
            ];

            if (jsonData.city === 'HAMBURG' || jsonData.city === 'BREMEN') {
                model.logDebug('Add Fussball column');
                cells.push({column : 'Fussball:played', $: 'ja'});
            }

            model.plzTable.row(entryId).put(cells, function(err) {
                if (err)
                    return reject(err);

                resolve();
            });
        });
    },

    finishImport: function() {
        // nothing to do here
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
