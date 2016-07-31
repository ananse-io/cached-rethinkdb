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

const Promise = require('bluebird'); // eslint-disable-line no-unused-vars
const Immutable = require('seamless-immutable');
const r = require('rethinkdb');

const uuid = require('uuid');

function validateRedisKey(idOrEntry, redisKeyFn) {
  return Promise.resolve().then(() => {
    const redisKey = redisKeyFn(idOrEntry);
    return redisKey;
  });
}

function validateUUID(idOrEntry, entryIdFn) {
  return Promise.resolve().then(() => {
    const identifier = entryIdFn(idOrEntry);
    return identifier;
  });
}

function defaultRedisKeyFn(idOrEntry) {
  if (!idOrEntry || !idOrEntry.uuid) {
    throw new Error('redis key cannot be undefined or null');
  }
  return idOrEntry.uuid;
}

function defaultEntryIdentifier(idOrEntry) {
  if (!idOrEntry || !idOrEntry.uuid) {
    throw new Error('uuid cannot be undefined or null');
  }
  return idOrEntry.uuid;
}

class CachedRethinkDB {
  constructor({redis, rethinkdb, table, logger, retrieveValidator, redisTTL, uuidPrefix = '', uuidField = 'uuid', redisKeyFn = defaultRedisKeyFn, entryIdentifier = defaultEntryIdentifier}) {
    if (!redis) {
      throw new Error('Missing redis instance');
    }
    this.redis = redis;

    if (!rethinkdb) {
      throw new Error('Missing database connection');
    }
    this.dbConn = rethinkdb;

    if (!table) {
      throw new Error('Missing table');
    }
    this.table = table;

    if (!logger) {
      throw new Error('Missing bunyan logger');
    }
    this.logger = logger.child({
      widget_type: 'CachedRethinkDB',
    });

    if (retrieveValidator && typeof retrieveValidator !== 'function') {
      throw new Error('retrieveValidator must be a function');
    }
    this.retrieveValidator = retrieveValidator;

    if (redisKeyFn && typeof redisKeyFn !== 'function') {
      throw new Error('redisKeyFn must be a function');
    }
    this.redisKeyFn = redisKeyFn || defaultRedisKeyFn;

    if (entryIdentifier && typeof entryIdentifier !== 'function') {
      throw new Error('entryIdentifier must be a function');
    }
    this.entryIdentifier = entryIdentifier || defaultEntryIdentifier;

    this.uuidPrefix = uuidPrefix || '';
    this.uuidField = uuidField || 'uuid';

    if (redisTTL && typeof redisTTL !== 'number') {
      throw new Error('redisTTL must be a number');
    }
    this.redisTTL = redisTTL || 7200;
  }

  // General Operation
  generateUUID({reqId, entry}) {
    const genUUID = { [this.uuidField]: `${this.uuidPrefix}${uuid.v4()}`};
    let generatedEntry;
    if (entry) {
      generatedEntry = Immutable.from(entry).merge(genUUID);
    } else {
      generatedEntry = genUUID;
    }
    return this.dbExist({ reqId, id: generatedEntry }).then((exist) => {
      if (exist) {
        return this.generateUUID({ reqId });
      }
      return generatedEntry;
    });
  }

  // Redis Operation
  cacheSet({reqId, entry}) {
    const ttl = this.redisTTL;
    let redisKey;
    let uid;
    return validateRedisKey(entry, this.redisKeyFn).then((rKey) => {
      redisKey = rKey;
      return validateUUID(entry, this.entryIdentifier);
    }).then((identifier) => {
      uid = identifier;
      return this.redis.setAsync(`${this.table}:${redisKey}`, JSON.stringify(entry));
    }).then((resolve) => {
      this.logger.trace({
        reqId,
        id: uid,
        table: this.table,
        redisResult: resolve,
      }, 'Cached entry');
      return this.redis.expireAsync(redisKey, ttl);
    }).then((resolve) => {
      this.logger.trace({
        reqId,
        ttl,
        id: uid,
        table: this.table,
        redisResult: resolve,
      }, 'Set cache TTL');
      return entry;
    }).catch((err) => {
      this.logger.error({ entry, err, id: uid, table: this.table }, 'Cannot cache');
      throw err;
    });
  }

  cacheFetch({reqId, id}) {
    let redisKey;
    return validateRedisKey(id, this.redisKeyFn).then((rKey) => {
      redisKey = rKey;
      return this.redis.getAsync(`${this.table}:${redisKey}`);
    }).then((resolve) => {
      const entry = JSON.parse(resolve);
      if (!entry) {
        this.logger.trace({
          reqId,
          id,
          table: this.table,
          redisResult: resolve,
        }, 'Not found in cache');
        return null;
      }
      this.logger.trace({
        reqId,
        id,
        table: this.table,
        redisResult: resolve,
      }, 'Fetched cache');
      if (this.retrieveValidator && !this.retrieveValidator(entry)) {
        this.logger.warn({
          reqId,
          id,
          table: this.table,
          selectValidation: 'failed',
        }, 'Failed validation during fetch');
        return null;
      }
      return entry;
    }).catch((err) => {
      this.logger.error({
        reqId,
        id,
        err,
        table: this.table,
      }, 'Cannot fetch');
      throw err;
    });
  }

  cacheInvalidate({reqId, id}) {
    let redisKey;
    return validateRedisKey(id, this.redisKeyFn).then((rKey) => {
      redisKey = rKey;
      return this.redis.delAsync(`${this.table}:${redisKey}`);
    }).then((resolve) => {
      this.logger.trace({
        reqId,
        id,
        table: this.table,
        redisResult: resolve,
      }, 'Invalidated cache');
      return id;
    }).catch((err) => {
      this.logger.error({
        reqId,
        id,
        err,
        table: this.table,
      }, 'Cannot invalidate');
      throw err;
    });
  }

  // DB Operation
  dbExist({reqId, id}) {
    return validateUUID(id, this.entryIdentifier).then((uid) => r.table(this.table).get(uid).run(this.dbConn)
    ).then((entryUuid) => {
      this.logger.trace({
        reqId,
        id,
        table: this.table,
        isExist: !!entryUuid,
      });
      return !!entryUuid;
    }).catch((err) => {
      this.logger.error({
        reqId,
        id,
        err,
        table: this.table,
      }, 'Cannot get uuid status');
      throw err;
    });
  }

  dbCreate({reqId, entry}) {
    let uid;
    return validateUUID(entry, this.entryIdentifier).then((identifier) => {
      uid = identifier;
      return r.table(this.table).insert(entry, {
        conflict: 'replace',
      }).run(this.dbConn);
    }).then((dbResult) => {
      if (dbResult.errors) {
        const err = new Error('Error occurred during create entry');
        err.dbResult = dbResult;
        throw err;
      }
      this.logger.trace({
        reqId,
        dbResult,
        id: uid,
        table: this.table,
      }, 'Created entry');
      return entry;
    }).catch((err) => {
      this.logger.error({
        reqId,
        entry,
        err,
        dbResult: err.dbResult || undefined,
        id: uid,
        table: this.table,
      }, 'Cannot create');
      throw err;
    });
  }

  dbRetrieve({reqId, id}) {
    let uid;
    return validateUUID(id, this.entryIdentifier).then((identifier) => {
      uid = identifier;
      return r.table(this.table).get(uid).run(this.dbConn);
    }).then((entry) => {
      if (!entry) {
        this.logger.trace({
          reqId,
          id,
          table: this.table,
        }, 'Entry does not exist');
        return null;
      }
      this.logger.trace({
        reqId,
        id: uid,
        table: this.table,
      }, 'Retrieved entry');

      if (this.retrieveValidator && !this.retrieveValidator(entry)) {
        this.logger.warn({
          reqId,
          id: uid,
          table: this.table,
          selectValidation: 'failed',
        }, 'Failed validation during retrieve');
        return null;
      }
      return entry;
    }).catch((err) => {
      this.logger.error({
        reqId,
        id,
        err,
        table: this.table,
      }, 'Cannot retrieve');
      throw err;
    });
  }

  dbUpdate({reqId, id, updateObjOrFn, nonAtomic = false}) {
    return validateUUID(id, this.entryIdentifier).then((uid) => {
      if (typeof updateObjOrFn !== 'function' && typeof updateObjOrFn !== 'object') {
        throw new Error('Parameter updateObjOrFn must be a function or an object');
      }
      return r.table(this.table).get(uid).update(updateObjOrFn, { returnChanges: true, nonAtomic: !!nonAtomic }).run(this.dbConn);
    }).then((dbResult) => {
      if (dbResult.errors) {
        const err = new Error('Error occurred during update entry');
        err.dbResult = dbResult;
        throw err;
      }
      this.logger.trace({
        reqId,
        dbResult,
        id,
        table: this.table,
      }, 'Updated entry');
      return dbResult.changes;
    }).catch((err) => {
      this.logger.error({
        reqId,
        id,
        err,
        dbResult: err.dbResult || undefined,
        table: this.table,
      }, 'Cannot update');
      throw err;
    });
  }

  dbDelete({reqId, id}) {
    return validateUUID(id, this.entryIdentifier).then((uid) => r.table(this.table).get(uid).delete().run(this.dbConn)).then((dbResult) => {
      if (dbResult.errors) {
        const err = new Error('Error occurred during delete entry');
        err.dbResult = dbResult;
        throw err;
      }
      this.logger.trace({
        reqId,
        dbResult,
        id,
        table: this.table,
      }, 'Deleted entry');
      return id;
    }).catch((err) => {
      this.logger.error({
        reqId,
        id,
        err,
        dbResult: err.dbResult || undefined,
        table: this.table,
      }, 'Cannot delete');
      throw err;
    });
  }

  // DB Admin
  dbCreateTable({reqId}) {
    return r.tableCreate(this.table, {
      primaryKey: this.uuidField,
    }).run(this.dbConn).then((dbResult) => {
      this.logger.trace({
        reqId,
        dbResult,
        table: this.table,
      }, 'Created table');
      return this.table;
    }).catch((err) => {
      this.logger.error({
        reqId,
        err,
      }, 'Cannot create table');
      throw err;
    });
  }

  dbDropTable({reqId}) {
    return r.tableDrop(this.table).run(this.dbConn).then((dbResult) => {
      this.logger.trace({
        reqId,
        dbResult,
        table: this.table,
      }, 'Dropped table');
      return this.table;
    }).catch((err) => {
      this.logger.error({
        reqId,
        err,
      }, 'Cannot drop table');
      throw err;
    });
  }

  dbCreateSimpleIndex({reqId, field}) {
    return r.table(this.table).indexCreate(field).run(this.dbConn).then((dbResult) => {
      this.logger.trace({
        reqId,
        dbResult,
        field,
        indexname: field,
        table: this.table,
      }, 'Creating simple index');

      return r.table(this.table).indexWait(field).run(this.dbConn);
    }).then((dbResult) => {
      this.logger.trace({
        reqId,
        dbResult,
        field,
        indexname: field,
        table: this.table,
      }, 'Created simple index');
      return field;
    }, (err) => {
      this.logger.error({
        reqId,
        err,
        field,
        indexname: field,
        table: this.table,
      }, 'Cannot create simple index');
    });
  }

  dbCreateCompoundIndex({reqId, name, fields}) {
    const rFields = fields.map(field => r.row(field));

    return r.table(this.table).indexCreate(name, rFields).run(this.dbConn).then((dbResult) => {
      this.logger.trace({
        reqId,
        dbResult,
        fields,
        indexname: name,
        table: this.table,
      }, 'Creating compound index');

      return r.table(this.table).indexWait(name).run(this.dbConn);
    }).then((dbResult) => {
      this.logger.trace({
        reqId,
        dbResult,
        fields,
        indexname: name,
        table: this.table,
      }, 'Created compound index');
      return name;
    }, (err) => {
      this.logger.error({
        reqId,
        err,
        fields,
        indexname: name,
        table: this.table,
      }, 'Cannot create compound index');
    });
  }

  // Basic CRUD operation for cached DB
  // Get a fresh copy from DB and cache it
  load({reqId, id}) {
    const action = 'load';
    return this.dbRetrieve({ reqId, id }).then((dbEntry) => {
      if (!dbEntry) {
        this.logger.trace({
          reqId,
          action,
          id,
          table: this.table,
        }, 'Entry not found');
        return null;
      }
      return this.cacheSet({ reqId, entry: dbEntry });
    }).then((entry) => {
      this.logger.trace({
        reqId,
        action,
        id,
        table: this.table,
      }, 'Entry loaded');
      return entry;
    }).catch((err) => {
      this.logger.error({
        reqId,
        action,
        err,
        id,
      }, 'Cannot load entry');
      throw err;
    });
  }

  create({reqId, entry}) {
    const action = 'create';
    let uid;
    let entryToBeCreated = entry || { };
    return this.generateUUID({ reqId, entry: entryToBeCreated }).then((entryWithUUID) => {
      entryToBeCreated = entryWithUUID;
      return validateUUID(entryToBeCreated, this.entryIdentifier);
    }).then((identifier) => {
      uid = identifier;
      return this.dbCreate({ reqId, entry: entryToBeCreated });
    }).then((e) => {
      this.logger.trace({
        reqId,
        action,
        entry: e,
        table: this.table,
      }, 'Entry created in DB');
      return this.cacheSet({ reqId, entry: e });
    }, (err) => {
      this.logger.error({
        reqId,
        action,
        err,
        id: uid,
      }, 'Cannot retrieve entry');
      throw err;
    });
  }

  retrieve({reqId, id}) {
    const action = 'retrieve';
    return this.cacheFetch({ reqId, id }).then((cacheEntry) => {
      if (!cacheEntry) {
        this.logger.trace({
          reqId,
          action,
          id,
          cache: 'miss',
          table: this.table,
        }, 'Entry cache miss');
        return this.load({ reqId, id });
      }
      this.logger.trace({
        reqId,
        action,
        id,
        cache: 'hit',
        table: this.table,
      }, 'Entry cache hit');
      return cacheEntry;
    }).catch((err) => {
      this.logger.error({
        reqId,
        action,
        err,
        id,
      }, 'Cannot retrieve entry');
      throw err;
    });
  }

  update({reqId, id, updateObjOrFn}) {
    const action = 'update';
    return this.dbUpdate({ reqId, id, updateObjOrFn }).then((changes) => {
      this.logger.trace({
        reqId,
        action,
        id,
        changes,
        table: this.table,
      }, 'Entry updated in DB');
      return this.load({ reqId, id });
    }, (err) => {
      this.logger.error({
        reqId,
        action,
        err,
        id,
      }, 'Cannot update entry');
      throw err;
    });
  }

  delete({reqId, id}) {
    const action = 'delete';
    return this.dbExist({ reqId, id }).then((exist) => {
      if (!exist) {
        return id;
      }
      return this.dbDelete({ reqId, id });
    }).then((deletedId) => {
      this.logger.trace({
        reqId,
        action,
        id,
        table: this.table,
      }, 'Entry deleted from DB');
      return this.cacheInvalidate({ reqId, id: deletedId });
    }, (err) => {
      this.logger.error({
        reqId,
        action,
        err,
        id,
      }, 'Cannot delete entry');
      throw err;
    });
  }
}

module.exports = CachedRethinkDB;
module.exports.r = r;
