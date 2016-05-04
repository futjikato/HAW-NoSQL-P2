var Hapi = require('hapi'),
    Stopwatch = require('timer-stopwatch'),
    hbase = require('hbase'),
    fs = require('fs'),
    hbaseDebug = require('debug')('hbase');

var client = new hbase.Client({ host: process.env.HBASEHOST, port: 8080 });
var plzTable = new hbase.Table(client, 'plz');

var server = new Hapi.Server();
server.connection({
    port: 8090
});

server.route({
    method: 'GET',
    path: '/postal/hbase/{postal}',
    handler: function(request, reply) {
        var postal = request.params.postal;
        plzTable.row(postal).get(function(err, data) {
            if (err) {
                reply({err: err.toString()});
            } else if(!data) {
                reply({err: new Error('Nothing found').toString()});
            } else {
                var resObj = {};
                data.forEach(function(col) {
                    var fullColName = col.column.toString();
                    var columnNameSplit = fullColName.split(':');
                    resObj[columnNameSplit[1]] = col.$.toString();
                });
                reply(resObj);
            }
        });
    }
});

server.route({
    method: 'GET',
    path: '/city/hbase/{city}',
    handler: function(request, reply) {
        var city = request.params.city;
        hbaseDebug(city);
        plzTable.scan({
            filter: {
                "op": "EQUAL",
                "type": "SingleColumnValueFilter",
                "family": new Buffer("location").toString('base64'),
                "qualifier": new Buffer("city").toString('base64'),
                "comparator":{"value": city, "type": "BinaryComparator"}
            }
        }, function(err, data) {
            if (data)
                hbaseDebug(data);

            if (err) {
                reply({err: err.toString()});
            } else if (Array.isArray(data)) {
                var resObj = {};
                data.forEach(function(rowData) {
                    var row = rowData.key.toString();
                    if (!resObj[row]) {
                        resObj[row] = {};
                    }

                    var fullColName = rowData.column.toString();
                    var columnNameSplit = fullColName.split(':');
                    resObj[row][columnNameSplit[1]] = rowData.$.toString();
                });

                var resAry = [];
                for (var key in resObj) {
                    if (resObj.hasOwnProperty(key)) {
                        resAry.push(resObj[key]);
                    }
                }

                reply(resAry);
            }
        })
    }
});

server.register(require('inert'), function(err) {
    if (err) {
        throw err;
    }

    server.route({
        method: 'GET',
        path: '/',
        handler: function (request, reply) {
            reply.file('./static/index.html');
        }
    });

    server.route({
        method: 'GET',
        path: '/static/{param*}',
        handler: {
            directory: {
                path: './static'
            }
        }
    });
});

server.start(function(err) {
    if (err) {
        throw err;
    }
    console.log('Server running');
});
