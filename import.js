var redis = require('redis'),
    fs = require('fs'),
    argv = require('minimist')(process.argv.slice(2), {alias: {d: 'flushdb'}, boolean: ['flushdb']});


var client = redis.createClient();

client.on('error', function(err) {
    console.log(err);
});

if (argv.flushdb) {
    client.flushdb();
    console.log('Flushed DB');
}

argv._.forEach(function(file) {
    importFile(file);
});

client.unref();

function importFile(file) {
    fs.readFile(file, function (err, data) {
        if (err) {
            console.log(err);
        } else {
            parseStrSync(data.toString());
        }
    });
}

function parseStrSync(str) {
    var offset = 0;
    var cityPlzMap = {};
    while (str.indexOf("\n", offset) !== -1) {
        var endPos = str.indexOf("\n", offset);
        var part = str.substr(offset, endPos - offset);
        try {
            var jsonData = JSON.parse(part),
                entryId = jsonData._id;

            client.hmset(entryId, "city", jsonData.city, "loc", JSON.stringify(jsonData.loc), "pop", jsonData.pop, "state", jsonData.state);

            if (!cityPlzMap[jsonData.city]) {
                cityPlzMap[jsonData.city] = [];
            }
            cityPlzMap[jsonData.city].push(entryId);
        } catch (err) {
            console.log('Error parsing', part, err);
        }
        offset = endPos + 1;
    }

    for (var key in cityPlzMap) {
        if (cityPlzMap.hasOwnProperty(key)) {
            client.set(key, JSON.stringify(cityPlzMap[key]));
        }
    }

    if (offset < str.length) {
        throw new Error('Document must end with EOL');
    }
}
