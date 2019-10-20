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

describe('Database Update', function () {
  afterEach(async () => {
    db = null
  })

  it('can update with object as where', async function () {
    db = new DB(dbOptions())
    expect(await db.update('Setting', {
      value: '1234'
    }, {
      key: 'test',
      value: 'now'
    })).to.be.equal(1)
  })

  it('can update with array as where', async function () {
    db = new DB(dbOptions())
    expect(await db.update('Setting', {
      key: 'test2',
      value: '1234'
    }, ['`key` = ? AND `value` = ?', 'test', 'now'])).to.be.equal(1)

    expect(await db.queryFirstCell('SELECT COUNT(1) FROM Setting WHERE key = ?', 'test2')).to.be.equal(1)
  })

  it('can update with whitelist', async function () {
    db = new DB(dbOptions())
    expect(await db.update('Setting', {
      key: 'test2',
      value: '1234'
    }, ['`key` = ? AND `value` = ?', 'test', 'now'], ['key'])).to.be.equal(1)

    // eslint-disable-next-line no-unused-expressions
    expect(await db.queryFirstCell('SELECT value FROM Setting WHERE key = ?', 'test2')).to.be.equal('now')
  })

  it('can update with blacklist', async function () {
    db = new DB(dbOptions())
    expect(await db.updateWithBlackList('Setting', {
      key: 'test2',
      value: '1234'
    }, ['`key` = ? AND `value` = ?', 'test', 'now'], ['key', 'fasdasd'])).to.be.equal(1)

    expect(await db.queryFirstCell('SELECT value FROM Setting WHERE key = ?', 'test')).to.equal('1234')
  })
})
