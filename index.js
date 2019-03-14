"use strict";

const aws = require('aws-sdk');
const EventEmitter = require('events').EventEmitter;
const schedule = require('tempus-fugit').schedule;
const ms = require('ms');
const async = require('async');
const debug = require('debug')('DynamodDBSubscriber');

class DynamodDBSubscriber extends EventEmitter {
  constructor (params) {
    super();

    if (!params.arn && !params.table) {
      throw new Error('arn or table are required');
    }

    this._region = params.region;
    this._table = params.table;
    this._streamArn = params.arn;
    this._endpoint = params.endpoint;

    if (typeof params.interval === 'number') {
      this._interval = params.interval;
    } else if (typeof params.interval === 'string') {
      this._interval = ms(params.interval);
    } else {
      this._interval = ms('10s');
    }

    this._ddbStream = params.endpoint 
      ? new aws.DynamoDBStreams({
        region: params.region,
        endpoint: params.endpoint
      }) 
      : new aws.DynamoDBStreams({ region: params.region });
  }

  _getOpenShards (callback) {
    var LastEvaluatedShardId;
    const shards = [];
    async.doWhilst(
      (cb) => {
        debug('stream.describeStream (start) Shard: %s, ExclusiveStartShardId: %s', this._streamArn, LastEvaluatedShardId);
        this._ddbStream.describeStream({
          StreamArn: this._streamArn,
          ExclusiveStartShardId: LastEvaluatedShardId
        }, (err, data) => {
          if (err) {
            return console.log(err);
          }
          LastEvaluatedShardId = data.StreamDescription.LastEvaluatedShardId;

          //filter closed shards.
          const openShards = data.StreamDescription.Shards
                  .filter(s => !s.SequenceNumberRange.EndingSequenceNumber);
          debug('stream.describeStream (end) Open Shards: %d', openShards.length);

          async.map(openShards, (shard, cb) => {
            debug('stream.getShardIterator (start) ShardId: %s', openShards.length);

            this._ddbStream.getShardIterator({
              StreamArn: this._streamArn,
              ShardId: shard.ShardId,
              ShardIteratorType: 'LATEST'
            }, (err, data) => {
              if (err) { return cb(err); }
              debug('stream.getShardIterator (end) Has ShardIterator? %s', !!data.ShardIterator);
              shard.iterator = data.ShardIterator;
              cb(null, shard);
            });

          }, (err, shardsWithIterators) => {
            if (err) { return cb(err); }
            shardsWithIterators.forEach(s => shards.push(s));
            cb();
          });
        });
      },
      () => LastEvaluatedShardId,
      (err) =>  {
        if (err) {
          return callback(err);
        }
        callback(null, shards);
      }
    );
  }

  _process (job) {
    debug('_process (start) Shards: %d', this._shards.length);

    async.each(this._shards, (shard, callback) => {
      debug('stream.getRecords (start) Shard: %s', shard.ShardId);
      this._ddbStream.getRecords({ ShardIterator: shard.iterator }, (err, data) => {
        if (err) {
          return callback(err);
        }

        debug('stream.getRecords (end) Shard: %s, Records: %d, Has NextShardIterator? %s',
            shard.ShardId,
            data.Records.length,
            !!data.NextShardIterator);

        if (data.Records && data.Records.length > 0) {
          data.Records.forEach(r => {
            const key = r.dynamodb && r.dynamodb.Keys && aws.DynamoDB.Converter.output({M: r.dynamodb.Keys});
            this.emit('record', r, key);
          });
        }
        shard.iterator = data.NextShardIterator;
        callback();
      });
    }, (err) => {
      if (err) {
        this.emit('error', err);
        return job.done();
      }
      //if some shard does not longer has an iterator
      //we need to fetch the openshards again and
      //process again.
      if (this._shards.length === 0 || this._shards.some(s => !s.iterator)) {
        debug('Some or all shards are closed retrieving the list of shards.');
        delete this._shards;
        return this._getOpenShards((err, shards) => {
          if (err) {
            this.emit('error', err);
            return job.done();
          }
          this._shards = shards;
          this._process(job);
        });
      }
      job.done();
    });
  }

  start() {
    async.series([
      //try get stream arn with dynamodb.describeTable
      cb => {
        if (this._streamArn) { return cb(); }
        const dynamo = this._endpoint 
          ? new aws.DynamoDB({ region: this._region, endpoint: this._endpoint }) 
          : new aws.DynamoDB({ region: this._region });
        dynamo.describeTable({ TableName: this._table }, (err, tableDescription) => {
          if (err) {
            return cb();
          }
          if (tableDescription && tableDescription.Table && tableDescription.Table.LatestStreamArn) {
            this._streamArn = tableDescription.Table.LatestStreamArn;
          }
          cb();
        });
      },

      //try get stream arn  with dynamodbstream.listStream
      cb => {
        if (this._streamArn) { return cb(); }
        this._ddbStream.listStreams({ TableName: this._table }, (err, result) => {
          if (err) {
            return cb(new Error(`Cannot retrieve the stream arn of ${this._table}: ` + err.message));
          }
          if (result && result.Streams && result.Streams[0]) {
            this._streamArn = result.Streams[0].StreamArn;
          }
          cb();
        });
      },

      //start the job
      cb => {
        this._getOpenShards((err, shards) => {
          if (err) {
            return cb(err);
          }

          this._shards = shards;

          this._job = schedule({
            millisecond: this._interval,
            start: Date.now()
          }, this._process.bind(this));

          cb();
        });
      }
    ], (err) => {
      if (err) {
        this.emit('error', err);
      }
    });
  }

  stop() {
    this._job.cancel();
  }
}

const Readable = require('stream').Readable;

class DynamodDBReadable extends Readable {
  constructor(options) {
    const opts = Object.assign({}, options, { objectMode: true });
    super(opts);
    this._subscriber = new DynamodDBSubscriber(opts);

    this._subscriber.on('record', (record) => {
      if (!this.push(record)) {
        this._subscriber.stop();
      }
    }).on('error', (err) => {
      this.emit('error', err);
    });
  }

  _read () {
    this._subscriber.start();
  }
}

module.exports = DynamodDBSubscriber;
module.exports.Stream = DynamodDBReadable;
