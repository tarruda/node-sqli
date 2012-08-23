factory = (pg) ->
  sqli = require('./sqli')
  sqli.register 'postgres',
    connect: (connectionString, cb) ->
      client = new pg.Client(connectionString)
      client.connect (err) ->
        if (err) then return cb(err, null)
        return cb(null, client)

    close: (client) -> client.end()

    execute: (client, sql, params, rowCb, endCb, errCb) ->
      q = client.query(replaceQMarks(sql), params)
      q.on('row', rowCb).on('end', endCb).on('error', errCb)

    begin: (isolation) ->
      id = 'READ COMMITTED'
      switch isolation
        when sqli.READ_UNCOMMITTED
          id = 'READ UNCOMMITTED'
        when sqli.REPEATABLE_READ
          id = 'REPEATABLE READ'
        when sqli.SERIALIZABLE
          id = 'SERIALIZABLE'
      return 'START TRANSACTION ISOLATION LEVEL ' + id

    save: (savepoint) -> 'SAVEPOINT ' + savepoint

    commit: -> 'COMMIT'

    rollback: (savepoint) ->
      if not savepoint
        return 'ROLLBACK'
      return 'ROLLBACK TO SAVEPOINT ' + savepoint

try
  factory(require('pg'))
catch error
  console.log("Could not load postgres wrapper:\n#{error.message}")

###*
* Replaces question marks in the sql string by postgres prepared statement
* placeholders($1, $2...). Does minor parsing of the string in order to
* ignore question marks inside quotes.
*###
replaceQMarks = (sql) ->
  insideQuote = false
  parameterIndex = 1
  currentIndex = 0
  rv = []
  for c, i in sql
    if insideQuote
      if c is "'" then insideQuote = false
    else
      if c is '?'
        rv.push(sql.substring(currentIndex, i))
        rv.push('$' + parameterIndex)
        parameterIndex++
        currentIndex = i + 1
      else if c is "'" then insideQuote = true
  rv.push(sql.substring(currentIndex))
  return rv.join('')

exports.replaceQMarks = replaceQMarks
