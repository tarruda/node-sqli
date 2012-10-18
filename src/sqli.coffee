genericPool = require('generic-pool')
events = require('events')
{inspect} = require('util')

exports.READ_UNCOMMITTED = 0
exports.READ_COMMITTED = 1
exports.REPEATABLE_READ = 2
exports.SERIALIZABLE = 3

###*
* Main interface used to interact with database drivers.
*
* @param wrapper Wrapper to the real database driver.
*###
class Driver
  constructor: (@_wrapper) ->

  ###*
  * Starts connecting to the database and returns a promise for the
  * connection.
  *
  * @param {String} connectionString Connection string used to resolve the
  * database.
  * @return {ConnectionPromise} Object that can be used to interact with the
  * database.
  *###
  connect: (connectionString) ->
    resolver = new events.EventEmitter()
    @_wrapper.connect connectionString, (err, innerConnection) ->
      resolver.emit('resolve', err, innerConnection)
    closer = (connection) => wrapper.close(connection)
    return new ConnectionPromise(@_wrapper, resolver, closer, closer)

  ###*
  * Creates a pool to manage connections with the database.
  *
  * @param {String} connectionString Connection string used to resolve the
  * database.
  * @param {Number} max Maximum number of concurrent connections to the 
  * database.
  * @param {Number} timeout Maximum number of milliseconds a connection can be
  * idle before it is closed.
  * @return {ConnectionPool} Pool to manage connections to the database.
  *###
  createPool: (connectionString, max, timeout) ->
    return new ConnectionPool(@_wrapper, connectionString, max, timeout)

###*
* Represents a future connection which can be normally interacted with. All
* statements executed on this object will be sent to the 'real' connection as
* soon as it is available.
*
* @param wrapper The real driver wrapper object.
* @param resolver EventEmitter that will emit a 'resolve' event when the 
* connection is ready.
* @param {Function} releaser Will be called with the real connection to signal
* that the connection will no longer be used.
*###
class ConnectionPromise
  constructor: (@_wrapper, resolver, @_releaser, @_closer) ->
    @_queue = []
    @_connection = null
    @_error = null
    @_released = false
    @_pendingStmt = null
    @_readyCallback = null
    @_readyHandled = false
    @_errorCallback = null
    @_errorHandled = false
    @inTransaction = false
    resolver.once 'resolve', (err, conn) =>
      if err then @_error = err
      else
        @_connection = conn
        if (@_pendingStmt is null and @_queue[0])
          @_run(@_queue.shift())
      @_callbacks()

  _checkState: ->
    if @_released
      if @_error isnt null
        if typeof @_wrapper.getErrorMsg is 'function'
          reason = @_wrapper.getErrorMsg(@_error)
        else
          reason = @_error.toString()
        msg = "Connection is broken due to an error: #{reason}"
      else
        msg = 'Connection is no longer available'
      throw new Error(msg)
    else if @_error
      throw new Error "Connection is paused due to an error:\n" +
        @_error.message

  _callbacks: ->
    if typeof @_errorCallback is 'function'and
    @_error isnt null and not @_errorHandled
      @_errorCallback(@_error)
      @_errorHandled = true
    if typeof @_readyCallback is 'function' and
    @_connection isnt null and not @_readyHandled
      @_readyCallback()
      @_readyHandled = true

  _schedule: ->
    if @_queue[0]
      # clear stack before running the next statement
      next = @_queue.shift()
      process.nextTick =>
        @_run(next)


  _run: (stmt) ->
    # If the connection is released then return
    if @_released then return
    resolver = stmt[2]
    # If no 'resolver' as passed, assign a stub handler
    if not resolver instanceof events.EventEmitter
      resolver = emit: -> @ # stub emitter
    @_pendingStmt = stmt
    # Undefine query parameters to be safe
    if not stmt[1] then stmt[1] = undefined
    # Schedules the next statement but only if the current statement
    # is still pending(this may be invoked more times by the handlers
    # below.
    next = =>
      if @_pendingStmt is stmt
        @_pendingStmt = null
        if not @_error then @_schedule()
        @_callbacks()
    # Pass the statement to the real database driver and setup appropriate
    # handlers.
    @_wrapper.execute @_connection, stmt[0], stmt[1],
      (row) ->
        next()
        resolver.emit('row', row)
      , ->
        next()
        resolver.emit('end')
      , (err) =>
        if err and @_error is null
          msg =
            """
            Error happened when executing statement.
             Statement: '#{stmt[0]}'
             Arguments: '#{inspect(stmt[1])}'
             Cause: #{err.message}
            """
          @_error = new Error(msg)
        next()
        resolver.emit('error', @_error)

  # Runs the statement passed as paremeter, but only if
  # 1. There are no pending statements(if there are statements running
  #    and a statement was enqueued then it will eventually execute
  #    without needing to trigger it manually).
  # 2. Connection is available.
  # 3. The statement is the next in queue.
  _runIfNext: (stmt) ->
    shouldRun =
        @_pendingStmt is null and
        @_connection isnt null and
        @_queue[0] is stmt
    if shouldRun then @_run(@_queue.shift())

  _clearError: ->
    @_error = null
    @_errorHandled = false

  ###*
  * Schedule a SQL statement for execution.
  *
  * @param {String} sql Statement to be scheduled.
  * @param {Array} params Parameters that will be safely interpolated on the
  * SQL string.
  * @return {ResultPromise} Promise to the result of executing the statement.
  *###
  exec: (sql, params) ->
    if typeof sql isnt 'string'
      throw new Error('Pass SQL string as first argument')
    @_checkState()
    resolver = new events.EventEmitter()
    stmt = [sql, params, resolver]
    @_queue.push(stmt)
    @_runIfNext(stmt)
    return new ResultPromise(resolver)
  
  ###*
  * Resumes the execution of pending statements
  *###
  resume: (reset) ->
    if not @_error then throw new Error('Connection is not currently paused')
    @_clearError()
    if reset then @_queue = []
    else @_schedule()
  
  ###*
  * If the connection came from a pool it is returned, else the connection is
  * closed.
  *###
  release: ->
    if not @_released
      @_released = true
      @_releaser(@_connection)
  
  ###*
  * Closes the connection
  *###
  close: ->
    if not @_released
      @_released = true
      @_closer(@_connection)
  
  ###*
  * Starts a transaction with the specified isolation level. 
  * Acceptable values(The DBMS may not support all):
  * 0 - Read uncommited
  * 1 - Read commited (default)
  * 2 - Repeatable read
  * 3 - Serializable
  *
  * @param {Number} isolationLevel Isolation level.
  *###
  begin: (isolation) ->
    @inTransaction = true
    sql = @_wrapper.begin(isolation)
    rv = null
    if Array.isArray(sql)
      for s in sql
        res = @exec(s)
        if rv is null then rv = res
    else rv = @exec(sql)
    return rv
  
  ###*
  * Commits the current pending transaction. If no transaction is currently
  * pending, an error is thrown.
  *###
  commit: ->
    @inTransaction = false
    return @exec(@_wrapper.commit())
  
  ###*
  * Creates a savepoint that can be reverted to.
  *
  * @param {String} savepoint Savepoint identifier that can be passed to
  * 'rollback'.
  *###
  save: (savepoint) ->
    return @exec(@_wrapper.save(savepoint))
  
  ###*
  * Reverts the changes made by the currently pending transaction to the
  * specified savepoint. If no savepoint is specified, the rollback will
  * revert all changes since 'begin' was last called.
  *
  * @param {String} savepoint Savepoint to revert.
  *###
  rollback: (savepoint) ->
    if not savepoint then @inTransaction = false
    if @_error
      @_clearError()
      @_queue = []
    return @exec(@_wrapper.rollback(savepoint))
  
  ###*
  * Sets a handler to be executed when the connection is ready.
  *
  * @param {Function} cb Callback to be invoked when the connection is ready.
  *###
  ready: (cb) ->
    @_readyCallback = cb
    @_callbacks()
  
  ###*
  * Sets a handler to be executed if a error happens during connection or 
  * execution of a statement. If there's a pending error when the handler
  * is set, it will be executed imediately. When an error occurs, the 
  * connection can no longer be used. 
  *
  * @param {Function} cb Callback to be invoked if a error occurs.
  *###
  error: (cb) ->
    @_errorCallback = cb
    @_callbacks()


###*
* Manages/limits connections to the database.
*###
class ConnectionPool
  constructor: (@_wrapper, connectionString, max, timeout) ->
    if typeof max isnt 'number' then max = 10
    if typeof timeout isnt 'number' then timeout = 30000
    @_inner = genericPool.Pool
      create: (cb) => _wrapper.connect(connectionString, cb)
      destroy: (connection) => _wrapper.close(connection)
      max: max
      idleTimeoutMillis: timeout
    
  ###*
  * Starts resolving a connection to the database and returns a promise to it.
  *
  * @return {ConnectionPromise} Object that can be used to interact with the
  * database.
  *###
  get: ->
    destroyed = false
    released = false
    resolver = new events.EventEmitter()
    rv = new ConnectionPromise @_wrapper, resolver,
      (connection) =>
        if connection is null then released = true # release as soon as it is available
        else @_inner.release(connection)
      , (connection) =>
        if connection is null then destroyed = true # destroy as soon as it is available
        else @_inner.destroy(connection)
    @_inner.acquire (err, connection) =>
      if destroyed and connection then @_inner.destroy(connection)
      else if released and connection then @_inner.release(connection)
      else resolver.emit('resolve', err, connection)
    return rv


###*
* Represents the future result of a SQL statement. This object will be 
* 'consumed' when a callback passed to any of its result-consuming methods, so
* it can only be used to consume results once.
*
* @param resolver EventEmitter that will emit a 'resolve' event when
* the result is ready.
*###
class ResultPromise
  constructor: (resolver) ->
    @_buffer = []
    @_rowIdx = 0
    @_error = null
    @_callback = null
    @_then = null
    @_finished = false
    @_done = false
    resolver.on 'row', (row) =>
      if @_buffer is null then return
      @_buffer.push(row)
      @_flush()
    resolver.on 'end', =>
      @_finished = true
      @_flush()
    resolver.on 'error', (err) =>
      @_error = err
      @_finished = true
      @_flush()

  _flush: ->
    if typeof @_callback is 'function' and not @_error then @_callback()
    if typeof @_then is 'function' and @_finished then @_then()
  
  _consume: (cb) ->
    if typeof @_callback is 'function'
      throw new Error('Result callback already set')
    @_callback = cb
  
  ###*
  * Waits for all rows and pass them to the callback.
  *
  * @param {Function} cb Callback used to return all rows.
  *###
  all: (cb) ->
    @_consume =>
      if @_finished and @_buffer isnt null
        cb(@_buffer)
        @_buffer = null
    @_flush()
    return @
  
  ###*
  * Iterates through each result row as soon as possible, calling the
  * callback with the row/index as first/second arguments
  *
  * @param {Function} cb Callback used to return each row.
  *###
  each: (cb) ->
    @_consume =>
      if @_buffer isnt null
        while @_buffer.length
          cb(@_buffer.shift(), @_rowIdx)
          @_rowIdx++
    @_flush()
    return @
  
  ###*
  * Gets the first row in the result and invokes the callback with it.
  *
  * @param {Function} cb Callback used to return the first row.
  *###
  first: (cb) ->
    @_consume =>
      if @_buffer isnt null and @_buffer.length
        cb(@_buffer[0])
        @_buffer = null # discard the rest of the rows
      else if @_finished and @_error isnt null then cb(null)
    @_flush()
    return @
  
  ###*
  * Gets a single scalar value from the first row in the result and invokes 
  * the callback with it. Only call this when the result contains only one 
  * column.
  *
  * @param {Function} cb Callback used to return the scalar.
  *###
  scalar: (cb) ->
    return @first (firstRow) ->
      for column of firstRow
        cb(firstRow[column])
        break

  ###*
  * Executes a handler when the rows are fully processed
  *
  * @param {Function} cb Handler to execute.
  *###
  then: (cb) ->
    if @_then isnt null then throw new Error("'then' callback already set")
    @_then = =>
      if not @_done
        @_done = true
        cb(@_error)
    @_flush()
    return @


drivers = {}

exports.register = (name, wrapper) ->
  drivers[name] = new Driver(wrapper)

exports.getDriver = (name) ->
  return drivers[name]

# Register database-specific wrappers
require('./sqlite')
require('./postgres')
require('./mysql')
