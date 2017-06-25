const { Transform } = require('stream');
const fs = require('fs');

// Split incoming (file) stream into lines
const lineSplitter = require('split');

// Split lines by commas into arrays
const commaSplitter = new Transform({
    readableObjectMode: true,
    transform(chunk, encoding, callback) {
        this.push(chunk.toString().trim().split(','));
        callback();
    }
});

// Convert arrays into film objects
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

// Insert file objects into MongoDB
var insertFilms = function (db, callback) {

    // Get the films collection
    var collection = db.collection('films');
    var bulk = collection.initializeOrderedBulkOp();

    // Open file stream and pipe through transformations
    fs.createReadStream('netflix_movie_titles.csv')
        .pipe(lineSplitter())
        .pipe(commaSplitter)
        .pipe(arrayToFilm)
        .on('data', function (film) { bulk.insert(film); }) // Queue up film objects for bulk insert
        .on('finish', function () {
            // At the end, execute the bulk insert
            bulk.execute(function (err, result) {
                assert.equal(null, err);
                console.log('Bulk insert done');
                callback(result);
            });
        })
};

// Lookup some films from the database
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

// Count films per year by map reduce
var filmsPerYear = function (db, callback) {
    // Get the films collection
    var collection = db.collection("films");

    // Map function - emit year as key and 1 as value
    var map = function () { emit(this.year, 1); };

    // Reduce function
    var reduce = function (k, v) {
        // As each value of v has been mapped to 1, their sum is the number of values
        return v.length;
    };

    // Perform the map reduce
    collection.mapReduce(map, reduce, { out: { replace: 'tempCollection' } }, function (err, collection) {
        collection.find().toArray(function (err, docs) {
            assert.equal(err, null);
            console.log("Map reduce results");
            console.log(docs)
            callback(docs);
        });
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
        filmsPerYear(db, function () {
            db.close();
        });
    });
});