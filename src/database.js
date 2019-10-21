const AwaitLock = require('await-lock').default

let instance = null

/**
 * Class to control database-connections
 *
 * @returns {DB}
 * @constructor
 */
function DB (options = {}) {
  if (!(this instanceof DB)) {
    instance = instance || new DB(...arguments)
    return instance
  }
  if (typeof (options) !== 'object') {
    throw new Error('parameter needs to be a WebSQL')
  }
  if (options && options.transaction) {
    this.options = { db: options }
  } else {
    this.options = options
  }
  if (!this.options.db) {
    throw new Error('db parameter is missing')
  }
  this.awaitLock = new AwaitLock()
}

DB.prototype.connection = async function () {
  await this.awaitLock.acquireAsync()
  if (this.db) {
    this.awaitLock.release()
    return this.db
  }
  try {
    this.db = this.options.db

    if (this.options.migrate) {
      await this.migrate(Array.isArray(this.options.migrate) ? { migrations: this.options.migrate } : (typeof this.options.migrate === 'object' ? this.options.migrate : {}))
    }

    return this.db
  } catch (e) {
    this.db = undefined
    throw e
  } finally {
    this.awaitLock.release()
  }
}

/**
 * Executes the prepared statement. When execution completes it returns an info object describing any changes made. The info object has two properties:
 *
 * info.changes: The total number of rows that were inserted, updated, or deleted by this operation. Changes made by foreign key actions or trigger programs do not count.
 * info.lastID: The rowid of the last row inserted into the database (ignoring those caused by trigger programs). If the current statement did not insert any rows into the database, this number should be completely ignored.
 *
 * If execution of the statement fails, an Error is thrown.
 * @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#runbindparameters---object
 *
 * @param {Object} query the SQL-Query that should be run. Can contain placeholders for bind parameters.
 * @param {any} bindParameters You can specify bind parameters @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#binding-parameters
 * @returns {object}
 */
DB.prototype.run = async function (query, ...bindParameters) {
  const db = await this.connection()
  return new Promise((resolve, reject) => {
    db.transaction((tx) => {
      tx.executeSql(query, bindParameters, function (tx, rs) {
        resolve(rs)
      }, function (tx, error) {
        error.query = query
        error.parameters = bindParameters
        reject(error)
      })
    })
  })
}

/**
 * Returns all values of a query
 * @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#allbindparameters---array-of-rows
 *
 * @param {Object} query the SQL-Query that should be run. Can contain placeholders for bind parameters.
 * @param {any} bindParameters You can specify bind parameters @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#binding-parameters
 * @returns {array}
 */
DB.prototype.query = async function (query, ...bindParameters) {
  const db = await this.connection()
  return new Promise((resolve, reject) => {
    db.readTransaction((tx) => {
      tx.executeSql(query, bindParameters, function (tx, rs) {
        resolve(Array.from({ length: rs.rows.length }, (v, i) => rs.rows.item(i)))
      }, function (tx, error) {
        error.query = query
        error.parameters = bindParameters
        reject(error)
      })
    })
  })
}

/**
 * Returns the values of the first row of the query-result
 * @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#getbindparameters---row
 *
 * @param {Object} query the SQL-Query that should be run. Can contain placeholders for bind parameters.
 * @param {any} bindParameters You can specify bind parameters @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#binding-parameters
 * @returns {Object|null}
 */
DB.prototype.queryFirstRow = async function (query, ...bindParameters) {
  const db = await this.connection()
  return new Promise((resolve, reject) => {
    db.readTransaction((tx) => {
      tx.executeSql(query, bindParameters, function (tx, rs) {
        resolve(rs.rows.length ? rs.rows.item(0) : null)
      }, function (tx, error) {
        error.query = query
        error.parameters = bindParameters
        reject(error)
      })
    })
  })
}

/**
 * Returns the values of the first row of the query-result
 * @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#getbindparameters---row
 * It returns always an object and thus can be used with destructuring assignment
 *
 * @example const {id, name} = DB().queryFirstRowObject(sql)
 * @param {Object} query the SQL-Query that should be run. Can contain placeholders for bind parameters.
 * @param {any} bindParameters You can specify bind parameters @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#binding-parameters
 * @returns {Object}
 */
DB.prototype.queryFirstRowObject = async function (query, ...bindParameters) {
  const db = await this.connection()
  return new Promise((resolve, reject) => {
    db.readTransaction((tx) => {
      tx.executeSql(query, bindParameters, function (tx, rs) {
        resolve(rs.rows.length ? rs.rows.item(0) : {})
      }, function (tx, error) {
        error.query = query
        error.parameters = bindParameters
        reject(error)
      })
    })
  })
}

/**
 * Returns the value of the first column in the first row of the query-result
 *
 * @param {Object} query the SQL-Query that should be run. Can contain placeholders for bind parameters.
 * @param {any} bindParameters You can specify bind parameters @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#binding-parameters
 * @returns {any}
 */
DB.prototype.queryFirstCell = async function (query, ...bindParameters) {
  const db = await this.connection()
  return new Promise((resolve, reject) => {
    db.readTransaction((tx) => {
      tx.executeSql(query, bindParameters, function (tx, rs) {
        const obj = rs.rows.length ? rs.rows.item(0) : {}
        const keys = Object.keys(obj)
        resolve(keys.length ? obj[keys[0]] : undefined)
      }, function (tx, error) {
        error.query = query
        error.parameters = bindParameters
        reject(error)
      })
    })
  })
}

/**
 * Returns an Array that only contains the values of the specified column
 *
 * @param {Object} column Name of the column
 * @param {Object} query the SQL-Query that should be run. Can contain placeholders for bind parameters.
 * @param {any} bindParameters You can specify bind parameters @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#binding-parameters
 * @returns {array}
 */
DB.prototype.queryColumn = async function (column, query, ...bindParameters) {
  const db = await this.connection()
  return new Promise((resolve, reject) => {
    db.readTransaction((tx) => {
      tx.executeSql(query, bindParameters, function (tx, rs) {
        resolve(Array.from({ length: rs.rows.length }, (v, i) => rs.rows.item(i)[column]))
      }, function (tx, error) {
        error.query = query
        error.parameters = bindParameters
        reject(error)
      })
    })
  })
}

/**
 * Returns a Object that get it key-value-combination from the result of the query
 *
 * @param {String} key Name of the column that values should be the key
 * @param {Object} column Name of the column that values should be the value for the object
 * @param {Object} query the SQL-Query that should be run. Can contain placeholders for bind parameters.
 * @param {any} bindParameters You can specify bind parameters @see https://github.com/JoshuaWise/better-sqlite3/wiki/API#binding-parameters
 * @returns {object}
 */
DB.prototype.queryKeyAndColumn = async function (key, column, query, ...bindParameters) {
  const db = await this.connection()
  return new Promise((resolve, reject) => {
    db.readTransaction((tx) => {
      tx.executeSql(query, bindParameters, function (tx, rs) {
        resolve(Object.fromEntries(Array.from({ length: rs.rows.length }, (v, i) => {
          const item = rs.rows.item(i)
          return [item[key], item[column]]
        })))
      }, function (tx, error) {
        error.query = query
        error.parameters = bindParameters
        reject(error)
      })
    })
  })
}

/**
 * Create an update statement; create more complex one with exec yourself.
 *
 * @param {String} table Name of the table
 * @param {Object} data a Object of data to set. Key is the name of the column. Value 'undefined' is filtered
 * @param {String|Array|Object} where required. array with a string and the replacements for ? after that. F.e. ['id > ? && name = ?', id, name]. Or an object with key values. F.e. {id: params.id}. Or simply an ID that will be translated to ['id = ?', id]
 * @param {undefined|Array} whiteList optional List of columns that can only be updated with "data"
 * @returns {Integer}
 */
DB.prototype.update = async function (table, data, where, whiteList) {
  if (!where) {
    throw new Error('Where is missing for the update command of DB()')
  }
  if (!table) {
    throw new Error('Table is missing for the update command of DB()')
  }
  if (!typeof data === 'object' || !Object.keys(data).length) {
    return 0
  }

  // Build start of where query
  let sql = `UPDATE \`${table}\` SET `
  let parameter = []

  // Build data part of the query
  const setStringBuilder = []
  for (const keyOfData in data) {
    const value = data[keyOfData]
    // don't set undefined and only values in an optional whitelist
    if (value !== undefined && (!whiteList || whiteList.includes(keyOfData))) {
      parameter.push(value)
      setStringBuilder.push(`\`${keyOfData}\` = ?`)
    }
  }
  if (!setStringBuilder.length) {
    // nothing to update
    return 0
  }
  sql += setStringBuilder.join(', ')

  // Build where part of query
  sql += ' WHERE '
  if (Array.isArray(where)) {
    const [whereTerm, ...whereParameter] = where
    sql += whereTerm
    parameter = [...parameter, ...whereParameter]
  } else if (typeof where === 'object') {
    const whereStringBuilder = []
    for (const keyOfWhere in where) {
      const value = where[keyOfWhere]
      if (value !== undefined) {
        parameter.push(value)
        whereStringBuilder.push(`\`${keyOfWhere}\` = ?`)
      }
    }
    if (!whereStringBuilder.length) {
      throw new Error('Where is not constructed for the update command of DB()')
    }
    sql += whereStringBuilder.join(' AND ')
  } else {
    sql += 'id = ?'
    parameter.push(where)
  }

  return (await this.run(
    sql,
    ...parameter
  )).rowsAffected
}

/**
 * Create an update statement; create more complex one with exec yourself.
 *
 * @param {String} table Name of the table
 * @param {Object} data a Object of data to set. Key is the name of the column. Value 'undefined' is filtered
 * @param {String|Array|Object} where required. array with a string and the replacements for ? after that. F.e. ['id > ? && name = ?', id, name]. Or an object with key values. F.e. {id: params.id}. Or simply an ID that will be translated to ['id = ?', id]
 * @param {undefined|Array} whiteBlackList optional List of columns that can not be updated with "data" (blacklist)
 * @returns {Integer}
 */
DB.prototype.updateWithBlackList = async function (table, data, where, blackList) {
  return this.update(table, data, where, await createWhiteListByBlackList.bind(this)(table, blackList))
}

/**
 * Create an insert statement; create more complex one with exec yourself.
 *
 * @param {String} table Name of the table
 * @param {Object|Array} data a Object of data to set. Key is the name of the column. Can be an array of objects.
 * @param {undefined|Array} whiteList optional List of columns that only can be updated with "data"
 * @returns {Integer} The ID of the last inserted row
 */
DB.prototype.insert = async function (table, data, whiteList) {
  return (await this.run(
    ...createInsertOrReplaceStatement('INSERT', table, data, whiteList)
  )).insertId
}

/**
 * Create an insert statement; create more complex one with exec yourself.
 *
 * @param {String} table Name of the table
 * @param {Object|Array} data a Object of data to set. Key is the name of the column. Can be an array of objects.
 * @param {undefined|Array} whiteBlackList optional List of columns that can not be updated with "data" (blacklist)
 * @returns {Integer} The ID of the last inserted row
 */
DB.prototype.insertWithBlackList = async function (table, data, blackList) {
  return this.insert(table, data, await createWhiteListByBlackList.bind(this)(table, blackList))
}

/**
 * Create an replace statement; create more complex one with exec yourself.
 *
 * @param {String} table Name of the table
 * @param {Object|Array} data a Object of data to set. Key is the name of the column. Can be an array of objects.
 * @param {undefined|Array} whiteList optional List of columns that only can be updated with "data"
 * @returns {Integer} The number of replaced entries
 */
DB.prototype.replace = async function (table, data, whiteList) {
  return (await this.run(
    ...createInsertOrReplaceStatement('REPLACE', table, data, whiteList)
  )).rowsAffected
}

/**
 * Create an replace statement; create more complex one with exec yourself.
 *
 * @param {String} table Name of the table
 * @param {Object|Array} data a Object of data to set. Key is the name of the column. Can be an array of objects.
 * @param {undefined|Array} whiteBlackList optional List of columns that can not be updated with "data" (blacklist)
 * @returns {Integer} The ID of the last replaced row
 */
DB.prototype.replaceWithBlackList = async function (table, data, blackList) {
  return this.replace(table, data, await createWhiteListByBlackList.bind(this)(table, blackList))
}

DB.prototype.parseTable = function (table) {
  var s = table.sql.split(',')
  s[0] = s[0].replace(new RegExp('create\\s+table\\s+`?' + table.name + '`?\\s*\\(', 'i'), '')
  table.fields = s
    .filter(function (i) {
      return (i.indexOf(')') === -1)
    }).map(function (i) {
      return i.trim().split(/\s/).shift().replace(/`/g, '')
    })
  return table
}

async function createWhiteListByBlackList (table, blackList) {
  let whiteList
  if (Array.isArray(blackList)) {
    // get all avaible columns
    try {
      whiteList = await this.queryColumn('name', `PRAGMA table_info('${table}')`)
    } catch (e) {
      whiteList = this.parseTable(await this.queryFirstRowObject(`SELECT * FROM sqlite_master WHERE tbl_name = '${table}'`)).fields
    }
    // get only those not in the whiteBlackList
    whiteList = whiteList.filter(v => !blackList.includes(v))
  }
  return whiteList
}

function createInsertOrReplaceStatement (insertOrReplace, table, data, whiteList) {
  if (!table) {
    throw new Error(`Table is missing for the ${insertOrReplace} command of DB()`)
  }
  if (!Array.isArray(data)) {
    data = [data]
  }

  let fields = Object.keys(data[0])

  if (Array.isArray(whiteList)) {
    fields = fields.filter(v => whiteList.includes(v))
  }

  // Build start of where query
  const parameter = []

  const sql = data.reduce((sql, rowData, index) => {
    fields.forEach(field => parameter.push(rowData[field]))
    return sql + (index ? ',' : '') + '(' + Array.from({ length: fields.length }, () => '?').join(',') + ')'
  }, `${insertOrReplace} INTO \`${table}\` (\`${fields.join('`,`')}\`) VALUES `)
  return [sql, ...parameter]
}

/**
 * Migrates database schema to the latest version
 */
DB.prototype.migrate = async function ({ force = false, table = 'migrations', migrations = [] } = {}) {
  if (!this.options.migrate) {
    // We don't call `connection` if `options.migrate` is `true`, because in this case `connection` will call `migrate`
    // which would lead into a dead-lock.
    await this.connection()
  }

  if (!migrations.length) {
    // No migration files found
    return
  }

  // Ge the list of migrations, for example:
  //   { id: 1, name: 'initial', filename: '001-initial.sql', up: ..., down: ... }
  //   { id: 2, name: 'feature', fielname: '002-feature.sql', up: ..., down: ... }
  migrations = migrations.map((migration, index) => {
    const [up, down] = migration.split(/^--\s+?down\b/mi)
    if (!down) {
      const message = `${index} entry does not contain '-- Down' separator.`
      throw new Error(message)
    }
    return {
      id: index,
      up: up.replace(/^-- .*?$/gm, '').trim(),
      down: down.replace(/^-- .*?$/gm, '').trim()
    }
  })

  // Create a database table for migrations meta data if it doesn't exist
  let query = ''
  let parameters = []
  await new Promise((resolve, reject) => {
    this.db.transaction((tx) => {
      query = `CREATE TABLE IF NOT EXISTS "${table}" (
        id   INTEGER PRIMARY KEY,
        up   TEXT    NOT NULL,
        down TEXT    NOT NULL
      )`
      tx.executeSql(query, [], function (tx, rs) {
        resolve(rs)
      }, function (tx, error) {
        error.query = query
        error.parameters = []
        reject(error)
      })
    })
  })

  // Get the list of already applied migrations
  const dbMigrations = await new Promise((resolve, reject) => {
    this.db.transaction((tx) => {
      query = `SELECT id, up, down FROM "${table}" ORDER BY id ASC`
      tx.executeSql(query, [], function (tx, rs) {
        resolve(Array.from({ length: rs.rows.length }, (v, i) => rs.rows.item(i)))
      }, function (tx, error) {
        error.query = query
        error.parameters = []
        reject(error)
      })
    })
  })

  // Undo migrations that exist only in the database but not in files,
  // also undo the last migration if the `force` option was set.
  const lastMigration = migrations[migrations.length - 1]
  for (const migration of dbMigrations.slice().sort((a, b) => Math.sign(b.id - a.id))) {
    if (!migrations.some(x => x.id === migration.id) ||
        (force && migration.id === lastMigration.id)) {
      await new Promise((resolve, reject) => {
        this.db.transaction((tx) => {
          query = migration.down
          parameters = []
          tx.executeSql(query, [], () => {}, (tx, error) => {
            error.query = query
            error.parameters = parameters
            reject(error)
          })
          query = `DELETE FROM "${table}" WHERE id = ?`
          parameters = [migration.id]
          tx.executeSql(query, parameters, () => {}, (tx, error) => {
            error.query = query
            error.parameters = parameters
            reject(error)
          })
        }, () => {}, function () {
          resolve()
        })
      })
    } else {
      break
    }
  }

  // Apply pending migrations
  const lastMigrationId = dbMigrations.length ? dbMigrations[dbMigrations.length - 1].id : -1
  for (const migration of migrations) {
    if (migration.id > lastMigrationId) {
      await new Promise((resolve, reject) => {
        this.db.transaction((tx) => {
          query = migration.up
          parameters = []
          tx.executeSql(query, parameters, () => {}, (tx, error) => {
            error.query = query
            error.parameters = parameters
            reject(error)
          })
          query = `INSERT INTO "${table}" (id, up, down) VALUES (?, ?, ?)`
          parameters = [migration.id, migration.up, migration.down]
          tx.executeSql(query, parameters, () => {}, (tx, error) => {
            error.query = query
            error.parameters = parameters
            reject(error)
          })
        }, () => {}, function () {
          resolve()
        })
      })
    }
  }
}

module.exports = DB
