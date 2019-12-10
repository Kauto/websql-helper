/* eslint-disable no-unused-expressions */
const { describe, it, afterEach } = require('mocha')
const { expect } = require('chai')
const DB = require('../src/database')
const openDatabase = require('websql')
const migrate = require('./migrations/migration.js')
let db = null

describe('Database Basics', function () {
  afterEach(async () => {
    db = null
  })

  it('should run queries with run', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })

    expect(await db.run('SELECT 1')).to.not.throw
  })

  it('should exec many queries with exec', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })

    expect(await db.run('SELECT 1; SELECT 2')).to.not.throw
  })

  it('should return all rows with query', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })
    expect(await db.query('SELECT ? as `1` UNION SELECT ? as `1`', 1, 2)).to.deep.equal([{ 1: 1 }, { 1: 2 }])
  })

  it('should return first row with queryFirstRow', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })
    expect(await db.queryFirstRow('SELECT ? as `1` UNION SELECT ? as `1`', 1, 2)).to.deep.equal({ 1: 1 })
  })

  it('should return first cell with queryFirstCell', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })
    expect(await db.queryFirstCell('SELECT ?', 1)).to.equal(1)
  })

  it('should return undefined when queryFirstCell does not hit', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })

    expect(await db.queryFirstCell('SELECT 1 WHERE 1=0')).to.be.undefined
  })

  it('should return a column with queryColumn', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })
    expect(await db.queryColumn('2', 'SELECT ? as `1`, ? as `2` UNION SELECT ? as `1`, ? as `2`', 1, 2, 3, 4)).to.deep.equal([2, 4])
  })

  it('should return a object with key and value from columns defined in queryKeyAndColumn', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: false
    })
    expect(await db.queryKeyAndColumn('1', '2', 'SELECT ? as `1`, ? as `2` UNION SELECT ? as `1`, ? as `2`', 1, 2, 3, 4)).to.deep.equal({ 1: 2, 3: 4 })
  })

  it('should migrate files', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: migrate
    })
    expect(await db.queryFirstCell('SELECT `value` FROM Setting WHERE `key` = ?', 'test')).to.be.equal('now')
  })

  it('should migrate files with force', async function () {
    db = new DB({
      db: openDatabase(':memory:', '1.0', 'description', 1),
      migrate: { force: 'true', migrations: migrate }
    })
    await db.migrate()
    await db.migrate()
    expect(await db.queryFirstCell('SELECT `value` FROM Setting WHERE `key` = ?', 'test')).to.be.equal('now')
  })
})
