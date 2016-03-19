var Hapi = require('hapi'),
    redis = require('redis'),
    fs = require('fs');

var client = redis.createClient();

var server = new Hapi.Server();
server.connection({
    port: 8080
});

function loadPostalData(postal, reply) {
    client.hgetall(postal, function(err, data) {
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
    path: '/postal/{postal}',
    handler: function(request, reply) {
        loadPostalData(request.params.postal, reply);
    }
});

server.route({
    method: 'GET',
    path: '/city/{city}',
    handler: function(request, reply) {
        var city = request.params.city;
        client.get(city, function(err, data) {
            if (err) {
                reply({err: err.toString()});
            } else if(!data) {
                reply({err: new Error('Nothing found').toString()});
            } else {
                loadPostalData(data, reply);
            }
        });
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