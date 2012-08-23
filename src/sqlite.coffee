factory = (sqlite3) ->
  sqli = require('./sqli')
  sqli.register 'sqlite',
    connect: (filename, cb) ->
      db = new sqlite3.Database filename, (err) ->
        if err then return cb(err, null)
        cb(null, db)

    close: (db) -> db.close()

    execute: (db, sql, params, rowCb, endCb, errCb) ->
      db.each sql, params
      , (err, row) ->
        if err then errCb(err)
        else rowCb(row)
      , (err, count) ->
        if err then errCb(err)
        else endCb()

    begin: (isolation) ->
      switch isolation
        when sqli.REPEATABLE_READ
          return 'BEGIN IMMEDIATE TRANSACTION'
        when sqli.SERIALIZABLE
          return 'BEGIN EXCLUSIVE TRANSACTION'
        else
          return 'BEGIN TRANSACTION'

    save: (savepoint) -> 'SAVEPOINT ' + savepoint

    commit: -> 'COMMIT'

    rollback: (savepoint) ->
      if not savepoint
        return 'ROLLBACK'
      return 'ROLLBACK TO ' + savepoint

try
  factory(require('sqlite3'))
catch error
  console.log("Could not load sqlite3 wrapper:\n#{error.message}")
