const sinon = require('sinon');
const { DynamoDBStreams } = require('@aws-sdk/client-dynamodb-streams');
const assert = require('chai').assert;
const DynamoDBSubscriber = require('../index.js');

describe('(internal behavior) subscriber._getOpenShards', function () {
  let describeStreamStub, getShardIteratorStub;

  afterEach(function () {
    sinon.restore();
  });

  before(function() {
    describeStreamStub = sinon.stub(DynamoDBStreams.prototype, 'describeStream').callsFake(function(params, callback) {
      assert.equal(params.StreamArn, 'urn:test:test');
      assert.equal(params.ExclusiveStartShardId, undefined);
      callback(null, {
        StreamDescription: {
          Shards: [
            { ShardId: '123', SequenceNumberRange: { EndingSequenceNumber: '1234' } },
            { ShardId: '456', SequenceNumberRange: { } }
          ]
        }
      });
    });

    getShardIteratorStub = sinon.stub(DynamoDBStreams.prototype, 'getShardIterator').callsFake(function(params, callback) {
      callback(null, { ShardIterator: '123' });
    });
  });

  after(function() {
    sinon.restore();
  });

  it('should work', function (done) {
    var subscriber = new DynamoDBSubscriber({ arn: 'urn:test:test' });
    subscriber._getOpenShards((err, shards) => {
      if (err) { return done(err); }
      assert.equal(shards.length, 1);
      assert.isOk(shards.every(s => !s.EndingSequenceNumber && s.iterator === '123'));
      done();
    });
  });
});
