const { Transform } = require('stream');
const fs = require('fs');

const lineSplitter = require('split');

const commaSplitter = new Transform({
    readableObjectMode: true,
    transform(chunk, encoding, callback) {
        this.push(chunk.toString().trim().split(','));
        callback();
    }
});

const arrayToFilm = new Transform({
    readableObjectMode: true,
    writableObjectMode: true,
    transform(chunk, encoding, callback) {
        var film = { id: '', year: '', title: '' };
        with (film) {
            id = chunk[0];
            year = chunk[1];
            title = chunk[2];
        }
        this.push(film);
        callback();
    }
});

var insertFilms = function (db, callback) {

    var collection = db.collection('films');
    var bulk = collection.initializeOrderedBulkOp();

    fs.createReadStream('netflix_movie_titles.csv')
        .pipe(lineSplitter())
        .pipe(commaSplitter)
        .pipe(arrayToFilm)
        .on('data', function (film) { bulk.insert(film); })
        .on('finish', function () {
            bulk.execute(function (err, result) {
                assert.equal(null, err);
                console.log('Bulk insert done');
                callback(result);
            });
        })
};

var findFilms = function (db, callback) {
    // Get the films collection
    var collection = db.collection('films');
    // Find 10 films from the year 2000
    collection.find({ year: '2000' }).limit(10).toArray(function (err, docs) {
        assert.equal(err, null);
        console.log("Found the following records");
        console.log(docs)
        callback(docs);
    });
};

var MongoClient = require('mongodb').MongoClient
    , assert = require('assert');

// Connection URL
var url = 'mongodb://localhost:27017/netflix';

// Use connect method to connect to the server
MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);
    console.log("Connected successfully to server");

    insertFilms(db, function () {
        findFilms(db, function () {
            db.close();
        });
    });
});