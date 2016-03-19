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
    while (str.indexOf("\n", offset) !== -1) {
        var endPos = str.indexOf("\n", offset);
        var part = str.substr(offset, endPos - offset);
        try {
            var jsonData = JSON.parse(part),
                entryId = jsonData._id;

            client.hset(entryId, "city", jsonData.city);
            client.hset(entryId, "loc", JSON.stringify(jsonData.loc));
            client.hset(entryId, "pop", jsonData.pop);
            client.hset(entryId, "state", jsonData.state);
            console.log('Saved data for postal code', entryId);

            client.set(jsonData.city, entryId);
            console.log('Saved reverse map', jsonData.city);
        } catch (err) {
            console.log('Error parsing', part, err);
        }
        offset = endPos + 1;
    }

    if (offset < str.length) {
        throw new Error('Document must end with EOL');
    }
}
