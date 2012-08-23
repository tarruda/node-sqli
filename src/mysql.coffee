factory = (mysql) ->
  sqli = require('./sqli')
  sqli.register 'mysql',
    connect: (connectionString, cb) ->
      connection = mysql.createConnection(connectionString)
      connection.connect (err) ->
        if err then return cb(err, null)
        return cb(null, connection)

    close: (connection) -> connection.end()

    execute: (connection, sql, params, rowCb, endCb, errCb) ->
      q = connection.query(sql, params)
      q.on('result', rowCb).on('end', endCb).on('error', errCb)

    begin: (isolation) ->
      id = 'READ COMMITTED'
      if isolation isnt undefined
        switch isolation
          when sqli.READ_UNCOMMITTED
            id = 'READ UNCOMMITTED'
          when sqli.REPEATABLE_READ
            id = 'REPEATABLE READ'
          when sqli.SERIALIZABLE
            id = 'SERIALIZABLE'
      return ['SET TRANSACTION ISOLATION LEVEL ' + id, 'START TRANSACTION']

    save: (savepoint) -> 'SAVEPOINT ' + savepoint

    commit: -> 'COMMIT'

    rollback: (savepoint) ->
      if not savepoint then return 'ROLLBACK'
      return 'ROLLBACK TO SAVEPOINT ' + savepoint

try
  factory(require('mysql'))
catch error
  console.log("Could not load mysql wrapper:\n#{error.message}")
