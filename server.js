var Hapi = require('hapi'),
    redis = require('redis'),
    Stopwatch = require('timer-stopwatch'),
    MongoClient = require('mongodb').MongoClient,
    fs = require('fs'),
    redisDebug = require('debug')('redis'),
    mongoDebug = require('debug')('mongo');

var redisClient = redis.createClient();

var server = new Hapi.Server();
server.connection({
    port: 8080
});

function loadRedisPostalData(postal, reply) {
    redisDebug('call hgetall for postal %s', postal);
    redisClient.hgetall(postal, function(err, data) {
        redisDebug('result %j for hgetall postal %s', data, postal);
        if (err) {
            reply({err: err.toString()});
        } else if(!data) {
            reply({err: new Error('Nothing found').toString()});
        } else {
            reply(data);
        }
    });
}

server.route({
    method: 'GET',
    path: '/postal/redis/{postal}',
    handler: function(request, reply) {
        loadRedisPostalData(request.params.postal, reply);
    }
});

server.route({
    method: 'GET',
    path: '/city/redis/{city}',
    handler: function(request, reply) {
        var city = request.params.city;
        var sw = new Stopwatch();
        redisDebug('call get for city', city);
        sw.start();
        redisClient.get(city, function(err, data) {
            redisDebug('result postal for city %s', city);
            if (err) {
                reply({err: err.toString()});
            } else if(!data) {
                reply({err: new Error('Nothing found').toString()});
            } else {
                var ary = JSON.parse(data);
                var doneCount = 0;
                var replyCollection = [];
                ary.forEach(function(plzKey) {
                    loadRedisPostalData(plzKey, function(plzData) {
                        plzData.plz = plzKey;
                        replyCollection.push(plzData);
                        if (++doneCount >= ary.length) {
                            sw.stop();
                            redisDebug('fetched %d results for city %s in %dms', ary.length, city, sw.ms);
                            reply(replyCollection);
                        }
                    });
                });
            }
        });
    }
});

var url = 'mongodb://localhost:27017/postal';
MongoClient.connect(url, function(err, db) {
    if (err)
        throw err;

    mongoDebug('mongo connection established');
    var postalcollection = db.collection('postals');

    server.route({
        method: 'GET',
        path: '/postal/mongo/{postal}',
        handler: function(request, reply) {
            var postal = request.params.postal;
            mongoDebug('call find for postal %d', postal);
            postalcollection.find({_id: postal}).limit(1).next(function(err, data) {
                if (err) {
                    mongoDebug('err for postal %d => %j', postal, err);
                    reply({err: err.toString()});
                } else if (!data) {
                    mongoDebug('nothing found for postal %d', postal);
                    reply({err: new Error('Nothing found').toString()});
                } else {
                    mongoDebug('found entry %j for postal %d', data, postal);
                    reply(data);
                }
            });
        }
    });

    server.route({
        method: 'GET',
        path: '/city/mongo/{city}',
        handler: function (request, reply) {
            var city = request.params.city;
            mongoDebug('call find for city %s', city);
            postalcollection.find({city: city}).toArray(function(err, data) {
                if (err) {
                    mongoDebug('err for city %s => %j', city, err);
                    reply({err: err.toString()});
                } else if (!data) {
                    mongoDebug('nothing found for city %s', city);
                    reply({err: new Error('Nothing found').toString()});
                } else {
                    mongoDebug('found %d entries for city %s', data.length, city);
                    reply(data);
                }
            });
        }
    });
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
