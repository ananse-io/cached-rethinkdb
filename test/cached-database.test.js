/*
  Copyright 2016 Ananse Limited

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

'use strict';

const CachedRethinkDB = require('../lib/cached-database');

const Promise = require('bluebird');
const expect = require('chai').expect;
const bunyan = require('bunyan');
const ringbuffer = new bunyan.RingBuffer({ limit: 300 });

const logger = bunyan.createLogger({
  name: 'cached-rethinkdb-test',
  streams: [
    {
      level: 'info',
      type: 'file',
      path: 'logs/unittest-info.log',
    }, {
      level: 'trace',
      type: 'raw',
      stream: ringbuffer,
    },
  ],
  serializers: {
    err: bunyan.stdSerializers.err,
  },
});

const redis = require('redis');
const redisOpts = {
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT || 6379,
};
let redisClient;

const r = require('rethinkdb');
const rethinkTestDB = 'cached_rethinkdb_test';
const rethinkdbOpts = {
  host: process.env.RETHINKDB_HOST || 'localhost',
  port: process.env.RETHINKDB_PORT || 28015,
  user: process.env.RETHINKDB_USER || 'admin',
  passport: process.env.RETHINKDB_PASSWORD || '',
  timeout: process.env.RETHINKDB_TIMEOUT || 15,
  db: rethinkTestDB,
};
let rethinkdbConnection;

// Standard Table
const testTable = 'unittest';
const stdEntry = {
  uuid: '1234567890',
  foo: 'bar',
  bar: 'foo',
  arr: [0, 1, 2, 3],
  nested: {
    foo: 'foobar',
    custom: 'hello',
  },
};
const stdInexistId = {
  uuid: '1141516151',
};
const stdId = {
  uuid: '1234567890',
};
const entryWithoutUUID = {
  foo: 'bar',
  bar: 'foo',
  arr: [0, 1, 2, 3],
  nested: {
    foo: 'foobar',
    custom: 'hello',
  },
};

// Table with Custom UUID and validator
const testTableCustom = 'unittestcustom';
const testTableCustomUUID = 'cUUID';
const testTableCustomPrefix = 'c-';
const checkKey = 'I_AM_A_SECRET_KEY';
const verificationField = 'vField';
const custEntry = {
  [testTableCustomUUID]: `${testTableCustomPrefix}123456789012345`,
  foo: 'bar',
  arr: [0, 1, 2, 3],
  nested: {
    foo: 'foobar',
    custom: 'hello',
  },
  [verificationField]: `${checkKey}:${testTableCustomPrefix}123456789012345`,
};
const custId = {
  [testTableCustomUUID]: `${testTableCustomPrefix}123456789012345`,
};
const invalidVerificationEntry = {
  [testTableCustomUUID]: `${testTableCustomPrefix}125160181894101`,
  foo: 'bar',
  arr: [0, 1, 2, 3],
  nested: {
    foo: 'foobar',
    custom: 'hello',
  },
  [verificationField]: '',
};
const invalidVerificationId = {
  [testTableCustomUUID]: `${testTableCustomPrefix}125160181894101`,
};

function redisKeyFn(idOrEntry) {
  return idOrEntry[testTableCustomUUID];
}

function entryIdentifier(idOrEntry) {
  return idOrEntry[testTableCustomUUID];
}

function retrieveValidator(entry) {
  return entry[verificationField] === `${checkKey}:${entry[testTableCustomUUID]}`;
}

describe('Cached RethinkDB', () => {
  let cachedDb;
  let cachedDbCustom;
  let newEntry;
  let newId;

  before(() => new Promise((resolve, reject) => {
    const client = redis.createClient(redisOpts);
    Promise.promisifyAll(client);
    client.on('ready', () => {
      resolve(client);
    });

    client.on('error', (err) => {
      reject(err);
    });
  }).then((client) => {
    redisClient = client;
    return r.connect(rethinkdbOpts);
  }).then((connection) => {
    rethinkdbConnection = connection;
    return r.dbCreate(rethinkTestDB).run(rethinkdbConnection);
  }));

  after(() => Promise.resolve()
    .then(() => r.dbDrop(rethinkTestDB).run(rethinkdbConnection)));

  it('should throw error when constructor parameters are not correct', () => {
    try {
      cachedDb = new CachedRethinkDB({
        rethinkdb: rethinkdbConnection,
        table: testTable,
        logger,
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'Missing redis instance');
    }
    try {
      cachedDb = new CachedRethinkDB({
        redis: redisClient,
        table: testTable,
        logger,
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'Missing database connection');
    }
    try {
      cachedDb = new CachedRethinkDB({
        redis: redisClient,
        rethinkdb: rethinkdbConnection,
        logger,
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'Missing table');
    }
    try {
      cachedDb = new CachedRethinkDB({
        redis: redisClient,
        rethinkdb: rethinkdbConnection,
        table: testTable,
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'Missing bunyan logger');
    }
    try {
      cachedDb = new CachedRethinkDB({
        redis: redisClient,
        rethinkdb: rethinkdbConnection,
        table: testTable,
        logger,
        retrieveValidator: 'notFunction',
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'retrieveValidator must be a function');
    }
    try {
      cachedDb = new CachedRethinkDB({
        redis: redisClient,
        rethinkdb: rethinkdbConnection,
        table: testTable,
        logger,
        redisKeyFn: 'notFunction',
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'redisKeyFn must be a function');
    }
    try {
      cachedDb = new CachedRethinkDB({
        redis: redisClient,
        rethinkdb: rethinkdbConnection,
        table: testTable,
        logger,
        entryIdentifier: 'notFunction',
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'entryIdentifier must be a function');
    }
    try {
      cachedDb = new CachedRethinkDB({
        redis: redisClient,
        rethinkdb: rethinkdbConnection,
        table: testTable,
        logger,
        redisTTL: '7200',
      });
      expect(cachedDb).to.not.exist;
    } catch (err) {
      expect(err).to.be.instanceof(Error);
      expect(err).to.have.deep.property('message', 'redisTTL must be a number');
    }
  });

  it('should be able to construct properly', () => {
    cachedDb = new CachedRethinkDB({
      redis: redisClient,
      rethinkdb: rethinkdbConnection,
      table: testTable,
      logger,
    });

    expect(cachedDb).to.exist;
    expect(cachedDb).to.have.deep.property('redis', redisClient);
    expect(cachedDb).to.have.deep.property('dbConn', rethinkdbConnection);
    expect(cachedDb).to.have.deep.property('table', testTable);
    expect(cachedDb).to.have.deep.property('redisTTL', 7200);
  });

  it('should be able to contruct properly with custom uuid key and prefix', () => {
    cachedDbCustom = new CachedRethinkDB({
      redis: redisClient,
      rethinkdb: rethinkdbConnection,
      table: testTableCustom,
      logger,
      uuidPrefix: testTableCustomPrefix,
      uuidField: testTableCustomUUID,
      retrieveValidator,
      redisKeyFn,
      entryIdentifier,
    });
    expect(cachedDbCustom).to.exist;
    expect(cachedDbCustom).to.have.deep.property('redis', redisClient);
    expect(cachedDbCustom).to.have.deep.property('dbConn', rethinkdbConnection);
    expect(cachedDbCustom).to.have.deep.property('table', testTableCustom);
    expect(cachedDbCustom).to.have.deep.property('uuidPrefix', testTableCustomPrefix);
    expect(cachedDbCustom).to.have.deep.property('uuidField', testTableCustomUUID);
    expect(cachedDbCustom).to.have.deep.property('redisTTL', 7200);
    expect(cachedDbCustom).to.have.deep.property('retrieveValidator', retrieveValidator);
    expect(cachedDbCustom).to.have.deep.property('redisKeyFn', redisKeyFn);
    expect(cachedDbCustom).to.have.deep.property('entryIdentifier', entryIdentifier);
  });

  // DATABASE ADMIN FUNCTIONS

  it('should be able to create standard table properly', () => cachedDb.dbCreateTable({ }).then((table) => {
    expect(table).to.equal(testTable);
  }));

  it('should be able to create custom table properly', () => cachedDbCustom.dbCreateTable({ }).then((table) => {
    expect(table).to.equal(testTableCustom);
  }));

  it('should fail to create duplicate table', () => cachedDb.dbCreateTable({ }).then((table) => {
    expect(table).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(r.Error.ReqlOpFailedError);
  }));

  it('should be able to create simple index properly', () => cachedDb.dbCreateSimpleIndex({ field: verificationField })
    .then((indexField) => {
      expect(indexField).to.equal(verificationField);
    }));

  it('should fail to create duplicate simple index', () => cachedDb.dbCreateSimpleIndex({ field: verificationField })
    .then((indexField) => {
      expect(indexField).to.not.exist;
    })
    .catch((err) => {
      expect(err).to.be.instanceof(r.Error.ReqlOpFailedError);
    }));

  it('should be able to create compound index properly', () => cachedDb.dbCreateCompoundIndex({ name: 'cIndex', fields: [testTableCustomUUID, verificationField] })
    .then((indexField) => {
      expect(indexField).to.equal('cIndex');
    }));

  it('should fail to create duplicate compound index', () => cachedDb.dbCreateCompoundIndex({ name: 'cIndex', fields: [testTableCustomUUID, verificationField] })
    .then((indexField) => {
      expect(indexField).to.not.exist;
    })
    .catch((err) => {
      expect(err).to.be.instanceof(r.Error.ReqlOpFailedError);
    }));

  it('should be able to generateUUID properly', () => cachedDbCustom.generateUUID({ }).then((genUUID) => {
    expect(genUUID).to.exist;
    expect(genUUID.startsWith(testTableCustomPrefix)).to.be.true;
  }));

  // REDIS CACHE FUNCTIONS
  // cacheSet
  it('should fail to cache into redis without uuid', () => cachedDb.cacheSet({ entry: { } }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'redis key cannot be undefined or null');
  }));

  it('should be able to cache into redis with uuid', () => cachedDb.cacheSet({ entry: stdEntry }).then((entry) => {
    expect(entry).to.equal(stdEntry);
  }));

  it('should be able to cache into redis with custom UUID field', () => cachedDbCustom.cacheSet({ entry: custEntry }).then((entry) => {
    expect(entry).to.equal(custEntry);
  }));

  it('should be able to cache into redis with custom UUID field and invalid verification info', () => cachedDbCustom.cacheSet({ entry: invalidVerificationEntry }).then((entry) => {
    expect(entry).to.equal(invalidVerificationEntry);
  }));

  // cacheFetch
  it('should fail to fetch redis cache without uuid', () => cachedDb.cacheFetch({ id: { } }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'redis key cannot be undefined or null');
  }));

  it('should be able to fetch null from redis cache for inexist item', () => cachedDb.cacheFetch({ id: stdInexistId }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to fetch from redis cache for existing item', () => cachedDb.cacheFetch({ id: stdId }).then((entry) => {
    expect(entry).to.deep.equal(stdEntry);
  }));

  it('should be able to fetch null from redis cache for item failing validation', () => cachedDbCustom.cacheFetch({ id: invalidVerificationId }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to fetch from redis cache for item passing validation', () => cachedDbCustom.cacheFetch({ id: custId }).then((entry) => {
    expect(entry).to.deep.equal(custEntry);
  }));

  // cacheInvalidate
  it('should fail to invalidate redis cache without uuid', () => cachedDb.cacheInvalidate({ id: { } }).then((id) => {
    expect(id).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'redis key cannot be undefined or null');
  }));

  it('should be able to invalidate redis cache with uuid', () => cachedDb.cacheInvalidate({ id: stdId }).then((invalidatedId) => {
    expect(invalidatedId).to.equal(stdId);
    return cachedDb.cacheFetch({ id: invalidatedId });
  }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to invalidate redis cache with custom UUID field', () => cachedDbCustom.cacheInvalidate({ id: custId }).then((invalidatedId) => {
    expect(invalidatedId).to.equal(custId);
    return cachedDbCustom.cacheFetch({ id: invalidatedId });
  }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to invalidate redis cache with custom UUID field and invalid verification info', () => cachedDbCustom.cacheInvalidate({ id: invalidVerificationId }).then((invalidatedId) => {
    expect(invalidatedId).to.equal(invalidVerificationId);
    return cachedDbCustom.cacheFetch({ id: invalidatedId });
  }).then((entry) => {
    expect(entry).to.be.null;
  }));

  // DATABASE FUNCTIONS
  // dbExist
  it('should fail to check entry existance without uuid', () => cachedDb.dbExist({ id: { } }).then((exist) => {
    expect(exist).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  it('should be able to check in exist entry existance properly', () => cachedDb.dbExist({ id: stdId }).then((exist) => {
    expect(exist).to.be.false;
  }));

  // dbCreate
  it('should fail to create entry without uuid', () => cachedDb.dbCreate({ entry: { } }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  it('should be able to create entry with uuid', () => cachedDb.dbCreate({ entry: stdEntry }).then((entry) => {
    expect(entry).to.equal(stdEntry);
  }));

  it('should be able to create entry with custom uuid', () => cachedDbCustom.dbCreate({ entry: custEntry }).then((entry) => {
    expect(entry).to.equal(custEntry);
  }));

  it('should be able to create entry with custom uuid and invalid verification info', () => cachedDbCustom.dbCreate({ entry: invalidVerificationEntry }).then((entry) => {
    expect(entry).to.equal(invalidVerificationEntry);
  }));

  it('should be able to check entry existance properly', () => cachedDb.dbExist({ id: stdId }).then((exist) => {
    expect(exist).to.be.true;
  }));

  // dbRetrieve
  it('should fail to retrieve entry from db without uuid', () => cachedDb.dbRetrieve({ id: { } }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  it('should be able to retrieve null from db for inexist item', () => cachedDb.dbRetrieve({ id: stdInexistId }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to retrieve from db for existing item', () => cachedDb.dbRetrieve({ id: stdId }).then((entry) => {
    expect(entry).to.deep.equal(stdEntry);
  }));

  it('should be able to retrieve null from db for item failing validation', () => cachedDbCustom.dbRetrieve({ id: invalidVerificationId }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to retrieve from db for item passing validation', () => cachedDbCustom.dbRetrieve({ id: custId }).then((entry) => {
    expect(entry).to.deep.equal(custEntry);
  }));

  // dbUpdate
  it('should fail to update db entry without uuid', () => cachedDb.dbUpdate({ id: { } }).then((changes) => {
    expect(changes).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  it('should fail to update db entry without update function or update object', () => cachedDb.dbUpdate({ id: stdId }).then((changes) => {
    expect(changes).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'Parameter updateObjOrFn must be a function or an object');
  }));

  it('should fail to update db entry with inexist uuid', () => cachedDb.dbUpdate({ id: stdInexistId, updateObjOrFn: { } }).then((id) => {
    expect(id).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err.message.endsWith('to not exist')).to.be.true;
  }));

  it('should be able to update db entry with proper update function', () => cachedDb.dbUpdate({
    id: stdId,
    updateObjOrFn: (entry) => r.branch(
      entry('foo').eq('bar'),
      { bar: 'true' },
      { bar: 'false' }
    ),
  }).then((changes) => {
    expect(changes).to.exist;
    expect(changes).to.have.deep.property('[0].new_val.bar', 'true');
  }));

  it('should be able to update db entry with proper update json', () => cachedDb.dbUpdate({
    id: stdId,
    updateObjOrFn: { nested: { custom: 'world' } },
  }).then((changes) => {
    expect(changes).to.exist;
    expect(changes).to.have.deep.property('[0].new_val.nested.foo', 'foobar');
    expect(changes).to.have.deep.property('[0].new_val.nested.custom', 'world');
  }));

  // dbDelete
  it('should fail to delete db entry without uuid', () => cachedDb.dbDelete({ id: { } }).then((id) => {
    expect(id).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  it('should fail to delete db entry with inexist uuid', () => cachedDb.dbDelete({ id: stdInexistId }).then((id) => {
    expect(id).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err.message.endsWith('to not exist')).to.be.true;
  }));

  it('should be able to delete db entry with uuid', () => cachedDb.dbDelete({ id: stdId }).then((invalidatedId) => {
    expect(invalidatedId).to.equal(stdId);
    return cachedDb.dbRetrieve({ id: invalidatedId });
  }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to delete db entry with custom UUID field', () => cachedDbCustom.dbDelete({ id: custId }).then((invalidatedId) => {
    expect(invalidatedId).to.equal(custId);
    return cachedDbCustom.dbRetrieve({ id: invalidatedId });
  }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to delete db entry field and invalid verification info', () => cachedDbCustom.dbDelete({ id: invalidVerificationId }).then((invalidatedId) => {
    expect(invalidatedId).to.equal(invalidVerificationId);
    return cachedDbCustom.dbRetrieve({ id: invalidatedId });
  }).then((entry) => {
    expect(entry).to.be.null;
  }));

  // CRUD functions
  // load
  it('should fail to load enntry via load operation without uuid', () => cachedDb.load({ id: {} }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  // create
  it('should fail to create entry via CRUD operation without entry', () => cachedDb.create({ }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(TypeError);
  }));

  it('should be able to create entry via CRUD operation properly', () => cachedDb.create({ entry: entryWithoutUUID }).then((entry) => {
    expect(entry).to.have.deep.property('uuid');
    newId = { uuid: entry.uuid };
    newEntry = Object.assign(entryWithoutUUID, newId);
    expect(entry).to.deep.equal(newEntry);
  }));

  // retrieve
  it('should fail to retrieve entry via CRUD operation without uuid', () => cachedDb.retrieve({ }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'redis key cannot be undefined or null');
  }));

  it('should be able to retrieve null via CRUD operation with inexisting uuid', () => cachedDb.retrieve({ id: stdInexistId }).then((entry) => {
    expect(entry).to.be.null;
  }));

  it('should be able to retrieve entry via CRUD operation properly', () => cachedDb.retrieve({ id: newId }).then((entry) => {
    expect(entry).to.deep.equal(newEntry);
  }));

  it('should be able to retrieve entry via CRUD operation properly with cache removed', () => cachedDb.cacheInvalidate({ id: newId }).then((invalidatedId) => {
    expect(invalidatedId).to.equal(newId);
    return cachedDb.retrieve({ id: newId });
  }).then((entry) => {
    expect(entry).to.deep.equal(newEntry);
  }));

  // update
  it('should fail to update entry via CRUD operation properly without uuid', () => cachedDb.update({ id: {} }).then((entry) => {
    expect(entry).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  it('should fail to update entry via CRUD operation properly without update function or update object', () => cachedDb.update({ id: stdId }).then((changes) => {
    expect(changes).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'Parameter updateObjOrFn must be a function or an object');
  }));

  it('should be able to update entry via CRUD operation properly', () => cachedDb.update({
    id: newId,
    updateObjOrFn: { nested: { custom: 'world' } },
  }).then((entry) => {
    expect(entry).to.have.deep.property('nested.custom', 'world');
    newEntry.nested.custom = 'world';
    expect(entry).to.deep.equal(newEntry);
  }));

  // delete
  it('should fail to delete entry via CRUD operation without uuid', () => cachedDb.delete({ id: {} }).then((id) => {
    expect(id).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(Error);
    expect(err).to.have.deep.property('message', 'uuid cannot be undefined or null');
  }));

  it('should be able to delete inexist entry via CRUD operation properly', () => cachedDb.delete({ id: stdInexistId }).then((id) => {
    expect(id).to.equal(stdInexistId);
  }));

  it('should be able to delete entry via CRUD operation properly', () => cachedDb.delete({ id: newId }).then((id) => {
    expect(id).to.equal(newId);
    return cachedDb.retrieve({ id: newId });
  }).then((entry) => {
    expect(entry).to.be.null;
  }));

  // CLEAN UP AFTER TEST

  it('should be able to drop standard table properly', () => cachedDb.dbDropTable({ }).then((table) => {
    expect(table).to.equal(testTable);
  }));

  it('should fail to drop inexist table', () => cachedDb.dbDropTable({ }).then((table) => {
    expect(table).to.not.exist;
  }).catch((err) => {
    expect(err).to.be.instanceof(r.Error.ReqlOpFailedError);
  }));

  it('should be able to drop custom table properly', () => cachedDbCustom.dbDropTable({ }).then((table) => {
    expect(table).to.equal(testTableCustom);
  }));
});
