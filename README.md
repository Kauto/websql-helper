# websql-helper

A promise based wrapper library for the work with WebSQL. It's intended for simple cordova-apps and offer some new functions and a migration-system.

## How to install

Like always

```bash
npm i websql-helper
```

## How to use

In every file you want access to a WebSQL database simply require the library and use it right away in any *async*-function (or as a promise). You have to setup it once. 
Also you can give the migration sqls to the constructor at the start of the program.
##### anyServerFile.js
```js
const DB = require('websql-helper');
DB({
  db: openDatabase('demo', '1.0', 'Demo', 2 * 1024 * 1024),
  migration: [
`-- Up
CREATE TABLE users (
  id INTEGER PRIMARY KEY, 
  firstName TEXT NOT NULL, 
  lastName TEXT NOT NULL, 
  email TEXT NOT NULL
);

-- Down
DROP TABLE IF EXISTS users;`
  ]
});

(async () => {
  let row = await DB().queryFirstRow('SELECT * FROM users WHERE id=?', userId);
  console.log(row.firstName, row.lastName, row.email);
})()
```

And that's it!

## One global instance
A normal, simple application is mostly working with only one database. To make the class management more easy, this library does the access-control for you - mainly as a singleton. (But you can create a new instance to access other databases.)

You need to call the library once in the beginning of your code and thus setting it up.
##### index.js
```js
const DB = require('websql-helper');

// The first call creates the global instance with your settings
DB({
  db: openDatabase('demo', '1.0', 'Demo', 2 * 1024 * 1024),
  migration: [ require('migration/001.sql'), require('migration/002.sql') ]
})
```

After that you can use the library without parameter:
##### anotherAPIFile.js
```js
const DB = require('websql-helper');

// a second call directly returns the global instance
(async ()=>{
  let row = await DB().queryFirstRow('SELECT * FROM users WHERE id=?', userId);
  console.log(row.firstName, row.lastName, row.email);
})()
```

## New Functions
This class implements shorthand methods for WebSQL.

```js
(async ()=>{
  // shorthand for db.prepare('SELECT * FROM users').all(); 
  let allUsers = await DB().query('SELECT * FROM users');
  // result: [{id: 1, firstName: 'a', lastName: 'b', email: 'foo@b.ar'},{},...]
  // result for no result: []

  // shorthand for db.prepare('SELECT * FROM users WHERE id=?').get(userId); 
  let row = await DB().queryFirstRow('SELECT * FROM users WHERE id=?', userId);
  // result: {id: 1, firstName: 'a', lastName: 'b', email: 'foo@b.ar'}
  // result for no result: undefined

  // shorthand for db.prepare('SELECT * FROM users WHERE id=?').get(999) || {}; 
  let {id, firstname} = await DB().queryFirstRowObject('SELECT * FROM users WHERE id=?', userId);
  // result: id = 1; firstName = 'a'
  // result for no result: id = undefined; firstName = undefined

  // shorthand for db.prepare('SELECT * FROM users WHERE id=?').pluck(true).get(userId); 
  let email = await DB().queryFirstCell('SELECT email FROM users WHERE id=?', userId);
  // result: 'foo@b.ar'
  // result for no result: undefined

  // shorthand for db.prepare('SELECT * FROM users').all().map(e => e.email); 
  let emails = await DB().queryColumn('email', 'SELECT email FROM users');
  // result: ['foo@b.ar', 'foo2@b.ar', ...]
  // result for no result: []

  // shorthand for db.prepare('SELECT * FROM users').all().reduce((o, e) => {o[e.lastName] = e.email; return o;}, {});
  let emailsByLastName = await DB().queryKeyAndColumn('lastName', 'email', 'SELECT lastName, name FROM users');
  // result: {b: 'foo@b.ar', c: 'foo2@b.ar', ...}
  // result for no result: {}
})()
```

## Insert, Update and Replace

There are shorthands for `update`, `insert` and `replace`. They are intended to make programming of CRUD-Rest-API-functions easier. With a `blacklist` or a `whitelist` it's even possible to send a request's query (or body) directly into the database.

### Update
```js
// const numberOfChangedRows = DB().update(table, data, where, whitelist = undefined)

// simple use with a object as where and no whitelist
await DB().update('users', {
  lastName: 'Mustermann',
  firstName: 'Max'
}, {
  email: 'unknown@emailprovider.com'
})

// data from a request and a array as a where and only editing of lastName and firstName is allowed
await DB().update('users', req.body, ['email = ?', req.body.email], ['lastName', 'firstName'])


// update with blacklist (id and email is not allowed; only valid columns of the table are allowed) and where is a shorthand for ['id = ?', req.body.id]
await DB().updateWithBlackList('users', req.body, req.body.id, ['id', 'email'])
```

### Insert and replace
```js
// const lastInsertID = DB().insert(table, datas, whitelist = undefined)
// const lastInsertID = DB().replace(table, datas, whitelist = undefined)

// simple use with an object and no whitelist
await DB().insert('users', {
  lastName: 'Mustermann',
  firstName: 'Max',
  email: 'unknown@emailprovider.com'
})

// inserting two users
await DB().insert('users', [{
  lastName: 'Mustermann',
  firstName: 'Max',
  email: 'unknown@emailprovider.com'
}, {
  lastName: 'Mustermann2',
  firstName: 'Max2',
  email: 'unknown2@emailprovider.com'
}])

// data from a request and only lastName and firstName are set
await DB().replace('users', req.body, ['lastName', 'firstName'])


// replace with blacklist (id and email is not allowed; only valid columns of the table are allowed)
await DB().replaceWithBlackList('users', req.body, ['id', 'email']) // or insertWithBlackList
```

### Try and catch

If you want to put invalid values into the database, the functions will throw an error. So don't forget to surround the functions with a `try-catch`. Here is an example for an express-server:
```js
const { Router } = require('express')
const bodyParser = require('body-parser')
const DB = require('sqlite3-helper')

router.patch('/user/:id', bodyParser.json(), async function (req, res, next) {
  try {
    if (!req.params.id) {
      res.status(400).json({error: 'missing id'})
      return
    }
    await DB().updateWithBlackList(
      'users',
      req.body,
      req.params.id,
      ['id']
    )

    res.statusCode(200)
  } catch (e) {
    console.error(e)
    res.status(503).json({error: e.message})
  }
})
```



## Migrations

The migration in this library mimics the migration system of the excellent [sqlite](https://www.npmjs.com/package/sqlite) by Kriasoft. 

To use this feature you have to give the migration queries to the initialization of the object.

## License

[MIT](https://github.com/Kauto/sqlite3-helper/blob/master/LICENSE)