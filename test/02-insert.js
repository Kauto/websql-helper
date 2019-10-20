const { describe, it, afterEach } = require('mocha')
const { expect } = require('chai')
const DB = require('../src/database')
const openDatabase = require('websql')
const migrate = require('./migrations/migration.js')

let db = null
const dbOptions = () => ({
  db: openDatabase(':memory:', '1.0', 'description', 1),
  migrate: migrate
})

describe('Database Insert', function () {
  afterEach(async () => {
    db = null
  })

  it('can insert one line', async function () {
    db = new DB(dbOptions())
    expect(await db.insert('Setting', {
      key: 'test2',
      value: '1234',
      type: 0
    })).to.be.equal(2)
  })

  it('can insert more than one line', async function () {
    db = new DB(dbOptions())
    expect(await db.insert('Setting', [{
      key: 'test2',
      value: '1234',
      type: 0
    }, {
      key: 'test3',
      value: '12345',
      type: 0
    }])).to.be.equal(3)

    expect(await db.queryFirstCell('SELECT COUNT(1) FROM Setting WHERE key IN (?,?)', 'test2', 'test3')).to.be.equal(2)
  })

  it('can insert with whitelist', async function () {
    db = new DB(dbOptions())
    expect(await db.insert('Setting', [{
      key: 'test2',
      value: '1234',
      type: 0
    }, {
      key: 'test3',
      value: '12345',
      type: 0
    }], ['key'])).to.be.equal(3)

    // eslint-disable-next-line no-unused-expressions
    expect(await db.queryFirstCell('SELECT value FROM Setting WHERE key = ?', 'test2')).to.be.null
  })

  it('can insert with blacklist', async function () {
    db = new DB(dbOptions())
    expect(await db.insertWithBlackList('Setting', [{
      key: 'test2',
      value: '1234',
      type: 1
    }, {
      key: 'test3',
      value: '12345',
      type: 0
    }], ['type'])).to.be.equal(3)
    expect(await db.queryFirstCell('SELECT type FROM Setting WHERE key = ?', 'test2')).to.equal(0)
  })

  it('can replace with blacklist', async function () {
    db = new DB(dbOptions())
    expect(await db.replaceWithBlackList('Setting', [{
      key: 'test2',
      value: '1234',
      type: 1
    }, {
      key: 'test3',
      value: '12345',
      type: 0
    }], ['type'])).to.be.equal(2)
    expect(await db.queryFirstCell('SELECT type FROM Setting WHERE key = ?', 'test2')).to.equal(0)
  })
})
