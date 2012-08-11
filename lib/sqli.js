var genericPool = require('generic-pool')
, events = require('events')
, inspect = require('util').inspect;
exports.READ_UNCOMMITTED = 0
exports.READ_COMMITTED = 1
exports.REPEATABLE_READ = 2
exports.SERIALIZABLE = 3;

/**
 * Main interface used to interact with database drivers.
 *
 * @param wrapper Wrapper to the real database driver.
 */
function Driver(wrapper) { 
  this._wrapper = wrapper;
}

/**
* Starts connecting to the database and returns a promise for the
* connection.
*
* @param {String} connectionString Connection string used to resolve the
* database.
* @return {ConnectionPromise} Object that can be used to interact with the
* database.
*/
Driver.prototype.connect =  function(connectionString) {
  var wrapper = this._wrapper
  , resolver = new events.EventEmitter();
  wrapper.connect(connectionString, function(err, innerConnection) {
    resolver.emit('resolve', err, innerConnection);
  }); 
  function closer(connection) { wrapper.close(connection); }
  return new ConnectionPromise(wrapper, resolver, closer, closer);
};

/**
* Creates a pool to manage connections with the database.
*
* @param {String} connectionString Connection string used to resolve the
* database.
* @param {Number} max Maximum number of concurrent connections to the 
* database.
* @param {Number} timeout Maximum number of milliseconds a connection can be
* idle before it is closed.
* @return {ConnectionPool} Pool to manage connections to the database.
*/
Driver.prototype.createPool = function(connectionString, max, timeout) {
  return new ConnectionPool(this._wrapper, connectionString, max, timeout);
};

/**
 * Represents a future connection which can be normally interacted with. All
 * statements executed on this object will be sent to the 'real' connection as
 * soon as it is available.
 *
 * @param wrapper The real driver wrapper object.
 * @param resolver EventEmitter that will emit a 'resolve' event when the 
 * connection is ready.
 * @param {Function} releaser Will be called with the real connection to signal
 * that the connection will no longer be used.
 */
function ConnectionPromise(wrapper, resolver, releaser, closer) {
  var self = this;
  this._wrapper = wrapper;
  this._resolver = resolver;
  this._releaser = releaser;
  this._closer = closer;
  this._queue = [];
  this._connection = null;
  this._error = null;
  this._released = false;
  this._pendingStmt = null;
  this._readyCallback = null;
  this._readyHandled = false;
  this._errorCallback = null;
  this._errorHandled = false;
  this.inTransaction = false;
  resolver.once('resolve', function(err, conn) {
    var queue = self._queue;
    if (err) self._error = err;
    else {
      self._connection = conn;
      if (self._pendingStmt === null && queue[0]) {
        self._run(queue.shift());
      }
    }
    self._callbacks();
  });
}

ConnectionPromise.prototype._checkState = function() {
  if (this._released) {
    var msg;
    if (this._error !== null) {
      var reason;
      if (typeof this._wrapper.getErrorMsg === 'function')
        reason = this._wrapper.getErrorMsg(this._error);
      else
        reason = this._error.toString();
      msg = 'Connection is broken due to an error: ' + reason;
    }
    else 
      msg = 'Connection is no longer available';
    throw new Error(msg);
  } else if (this._error) {
    throw new Error('Connection is paused due to an error:\n' + 
        this._error.message);
  }
};

ConnectionPromise.prototype._callbacks = function() {
  if (typeof this._errorCallback === 'function' && this._error !== null && 
      !this._errorHandled) {
    this._errorCallback(this._error);
    this._errorHandled = true;
  }
  if (typeof this._readyCallback === 'function' && this._connection !== null && 
      !this._readyHandled) {
    this._readyCallback();
    this._readyHandled = true;
  }
};

ConnectionPromise.prototype._schedule = function() {
  var self = this;
  if (this._queue[0]) {
    // clear stack before running the next statement
    var next = this._queue.shift();
    process.nextTick(function() {
      self._run(next);
    });
  }
};

ConnectionPromise.prototype._run = function(stmt) {
  if (this._released) return; //
  var self = this;
  var resolver = stmt[2];
  if (!(resolver instanceof events.EventEmitter)) 
    resolver = {emit: function() {}}; // stub emitter 
  this._pendingStmt = stmt;
  if (!stmt[1]) stmt[1] = undefined;
  function next() {
    if (self._pendingStmt === stmt) {
      self._pendingStmt = null;
      if (!self._error) self._schedule();
      self._callbacks();
    }
  }
  this._wrapper.execute(this._connection, stmt[0], stmt[1], 
    function(row) {
      next();
      resolver.emit('row', row);
    },
    function() {
      next();
      resolver.emit('end');
    },
    function(err) {
      if (err && self._error === null) {
        var msg = [
          'Error happened when executing statement.',
          " Statement: '" + stmt[0] + "'",
          " Arguments: '" + inspect(stmt[1]) + "'",
          ' Cause: ' + err.message
        ].join('\n');
        self._error = new Error(msg);
      }
      next();
      resolver.emit('error', self._error);
    }
  );
};

ConnectionPromise.prototype._runIfNext = function(stmt) {
  var shouldRun = 
      this._pendingStmt === null && 
      this._connection !== null && 
      this._queue[0] === stmt;
  if (shouldRun) this._run(this._queue.shift());
};

/**
* Schedule a SQL statement for execution.
*
* @param {String} sql Statement to be scheduled.
* @param {Array} params Parameters that will be safely interpolated on the
* SQL string.
* @return {ResultPromise} Promise to the result of executing the statement.
*/
ConnectionPromise.prototype.exec = function(sql, params) {
  if (typeof sql !== 'string') throw new Error('Pass SQL string as first argument');
  this._checkState();
  if (!params)
    params = undefined;
  var resolver = new events.EventEmitter()
  , stmt = [sql, params, resolver];
  this._queue.push(stmt);
  this._runIfNext(stmt);
  return new ResultPromise(resolver);
};

ConnectionPromise.prototype._clearError = function() {
  this._error = null;
  this._errorHandled = false;
};

/**
 * Resumes the execution of pending statements
 */
ConnectionPromise.prototype.resume = function(reset) {
  if (!this._error) throw new Error('Connection is not currently paused');
  this._clearError();
  if (reset) this._queue = [];
  else this._schedule();
};

/**
* If the connection came from a pool it is returned, else the connection is
* closed.
*/
ConnectionPromise.prototype.release = function() {
  if (!this._released) {
    this._released = true;
    this._releaser(this._connection);
  }
};

/**
* Closes the connection
*/
ConnectionPromise.prototype.close = function() {
  if (!this._released) {
    this._released = true;
    this._closer(this._connection);
  }
};

/**
* Starts a transaction with the specified isolation level. 
* Acceptable values(The DBMS may not support all):
* 0 - Read uncommited
* 1 - Read commited (default)
* 2 - Repeatable read
* 3 - Serializable
*
* @param {Number} isolationLevel Isolation level.
*/
ConnectionPromise.prototype.begin = function(isolation) {
  this.inTransaction = true;
  var sql = this._wrapper.begin(isolation);
  var rv = null;
  if (Array.isArray(sql)) {
    for (var i = 0; i < sql.length; i++) {
      var res = this.exec(sql[i]);
      if (rv === null) rv = res;
    }
  } else rv = this.exec(sql);
  return rv;
};

/**
* Commits the current pending transaction. If no transaction is currently
* pending, an error is thrown.
*/
ConnectionPromise.prototype.commit = function() {
  this.inTransaction = false;
  return this.exec(this._wrapper.commit());
};

/**
* Creates a savepoint that can be reverted to.
*
* @param {String} savepoint Savepoint identifier that can be passed to
* 'rollback'.
*/
ConnectionPromise.prototype.save = function(savepoint) {
  return this.exec(this._wrapper.save(savepoint));
};

/**
* Reverts the changes made by the currently pending transaction to the
* specified savepoint. If no savepoint is specified, the rollback will
* revert all changes since 'begin' was last called.
*
* @param {String} savepoint Savepoint to revert.
*/
ConnectionPromise.prototype.rollback = function(savepoint) {
  if (!savepoint) this.inTransaction = false;
  if (this._error) {
    this._clearError();
    this._queue = [];
  }
  return this.exec(this._wrapper.rollback(savepoint));
};

/**
* Sets a handler to be executed when the connection is ready.
*
* @param {Function} cb Callback to be invoked when the connection is ready.
*/
ConnectionPromise.prototype.ready = function(cb) {
  this._readyCallback = cb;
  this._callbacks();
};

/**
* Sets a handler to be executed if a error happens during connection or 
* execution of a statement. If there's a pending error when the handler
* is set, it will be executed imediately. When an error occurs, the 
* connection can no longer be used. 
*
* @param {Function} cb Callback to be invoked if a error occurs.
*/
ConnectionPromise.prototype.error = function(cb) {
  this._errorCallback = cb;
  this._callbacks();
};

/**
* Manages/limits connections to the database.
*/
function ConnectionPool(wrapper, connectionString, max, timeout) {
  if (typeof max !== "number")
    max = 10;
  if (typeof timeout !== "number")
    timeout = 30000;
  this._inner = genericPool.Pool({
    create: function(cb) { wrapper.connect(connectionString, cb); },
    destroy: function(connection) { wrapper.close(connection); },
    max: max,
    idleTimeoutMillis: timeout
  });
  this._wrapper = wrapper;
}

/**
* Starts resolving a connection to the database and returns a promise to it.
*
* @return {ConnectionPromise} Object that can be used to interact with the
* database.
*/
ConnectionPool.prototype.get = function() {
  var self = this 
  , destroyed = false
  , released = false
  , resolver = new events.EventEmitter()
  , rv = new ConnectionPromise(this._wrapper, resolver, 
    function(connection) {
      if (connection === null) released = true; // release as soon as it is available
      else self._inner.release(connection);
    }, 
    function(connection) { 
      if (connection === null) destroyed = true; // destroy as soon as it is available
      else self._inner.destroy(connection); 
    });
  this._inner.acquire(function(err, connection) {
    if (destroyed && connection) self._inner.destroy(connection);
    else if (released && connection) self._inner.release(connection);
    else resolver.emit('resolve', err, connection);
  });
  return rv;
};

/**
 * Represents the future result of a SQL statement. This object will be 
 * 'consumed' when a callback passed to any of its result-consuming methods, so
 * it can only be used to consume results once.
 *
 * @param resolver EventEmitter that will emit a 'resolve' event when
 * the result is ready.
 */
function ResultPromise(resolver) {
  var self = this;
  this._buffer = [];
  this._rowIdx = 0;
  this._error = null;
  this._callback = null;
  this._then = null;
  this._finished = false;
  this._done = false;

  resolver.on('row', function(row) {
    if (self._buffer === null) return;
    self._buffer.push(row);
    self._flush();
  });
  resolver.on('end', function() {
    self._finished = true;
    self._flush();
  });
  resolver.on('error', function(err) {
    self._error = err;
    self._finished = true;
    self._flush();
  });
}

ResultPromise.prototype._flush = function() {
  if (typeof this._callback === 'function') this._callback();
  if (typeof this._then === 'function' && this._finished) this._then();
};

ResultPromise.prototype._consume = function(cb) {
  if (typeof this._callback === 'function') 
    throw new Error('Result callback already set');
  this._callback = cb;
};

/**
* Waits for all rows and pass them to the callback.
*
* @param {Function} cb Callback used to return all rows.
*/
ResultPromise.prototype.all = function(cb) {
  var self = this;
  this._consume(function() {
    if (self._finished && self._buffer !== null) {
      cb(self._buffer);
      self._buffer = null;
    }
  });
  this._flush();
  return this;
};

/**
* Iterates through each result row as soon as possible, calling the
* callback with the row/index as first/second arguments
*
* @param {Function} cb Callback used to return each row.
*/
ResultPromise.prototype.each = function(cb) {
  var self = this;
  this._consume(function() {
    if (self._buffer !== null)
      for (var i = 0;i < self._buffer.length;i++) {
        cb(self._buffer.shift(), self._rowIdx);
        self._rowIdx++;
      }
  });
  this._flush();
  return this;
};

/**
* Gets the first row in the result and invokes the callback with it.
*
* @param {Function} cb Callback used to return the first row.
*/
ResultPromise.prototype.first = function(cb) {
  var self = this;
  this._consume(function() {
    if (self._buffer !== null && self._buffer.length > 0) {
      cb(self._buffer[0]);
      self._buffer = null; // discard the rest of the rows
    } else if (self._finished && self._error !== null) {
      cb(null);
    }
  });
  this._flush();
  return this;
};
/**
* Gets a single scalar value from the first row in the result and invokes 
* the callback with it. Only call this when the result contains only one 
* column.
*
* @param {Function} cb Callback used to return the scalar.
*/
ResultPromise.prototype.scalar = function(cb) {
  return this.first(function(firstRow) {
    for (var k in firstRow) {
      cb(firstRow[k]);
      break;
    }
  });
};
/**
* Executes a handler when the rows are fully processed
*
* @param {Function} cb Handler to execute.
*/
ResultPromise.prototype.then = function(cb) {
  var self = this;
  if (this._then !== null) throw new Error('\'then\' callback already set');
  this._then = function() {
    if (!self._done) {
      self._done = true;
      cb(self._error);
    }
  };
  this._flush();
  return this;
};

var drivers = {};

exports.register = function (name, wrapper) {
  drivers[name] = new Driver(wrapper);
};
exports.getDriver = function(name) {
  return drivers[name];
};

// Register database-specific wrappers
require('./sqlite');
require('./postgres');
require('./mysql');
