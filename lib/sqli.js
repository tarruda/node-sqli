var genericPool = require('generic-pool')
, events = require('events');
exports.READ_UNCOMMITED = 0
exports.READ_COMMITTED = 1
exports.REPEATABLE_READ = 2
exports.SERIALIZABLE = 3;

/**
 * Main interface used to interact with database drivers.
 *
 * @param wrapper Wrapper to the real database driver.
 */
function Driver(wrapper) {
  var self = this;

  /**
   * Starts connecting to the database and returns a promise for the
   * connection.
   *
   * @param {String} connectionString Connection string used to resolve the
   * database.
   * @return {ConnectionPromise} Object that can be used to interact with the
   * database.
   */
  this.connect =  function(connectionString) {
    var resolver = new events.EventEmitter();
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
  this.createPool = function(connectionString, max, timeout) {
    return new ConnectionPool(wrapper, connectionString, max, timeout);
  };
}

/**
 * Represents a future connection which can be normally interacted with. All
 * statements executed on this object will be sent to the 'real' connection as
 * soon as it is available. This al
 *
 * @param wrapper The real driver wrapper object.
 * @param resolver EventEmitter that will emit a 'resolve' event when the 
 * connection is ready.
 * @param {Function} releaser Will be called with the real connection to signal
 * that the connection will no longer be used.
 */
function ConnectionPromise(wrapper, resolver, releaser, closer) {
  var self = this
  , queue = []
  , connection = null
  , error = null
  , released = false
  , pendingStmt = null
  , readyCallback = null
  , readyHandled = false
  , errorCallback = null
  , errorHandled = false;
  resolver.once('resolve', function(err, conn) {
    if (err) error = err;
    else {
      connection = conn;
      if (pendingStmt === null && queue[0]) {
        var next = queue.shift();
        run(next);
      }
    }
    callbacks();
  });

  function checkState() {
    if (released) {
      var msg;
      if (error !== null) {
        var reason;
        if (typeof wrapper.getErrorMsg === 'function')
          reason = wrapper.getErrorMsg(error);
        else
          reason = error.toString();
        msg = 'Connection is broken due to an error: ' + reason;
      }
      else 
        msg = 'Connection is no longer available';
      throw new Error(msg);
    }
  }

  function callbacks() {
    if (typeof errorCallback === 'function' && error !== null && !errorHandled) {
      errorCallback(error);
      errorHandled = true;
    }
    if (typeof readyCallback === 'function' && connection !== null && !readyHandled) {
      readyCallback();
      readyHandled = true;
    }
  }

  function schedule() {
    if (queue[0]) {
      // clear stack before running the next statement
      var next = queue.shift();
      process.nextTick(function() {
        run(next);
      });
    }
  }

  function run(stmt) {
    var resolver = stmt[2];
    if (!(resolver instanceof events.EventEmitter)) 
      resolver = {emit: function() {}}; // stub emitter 
    if (error) {
      resolver.emit('error', error);
      schedule();
      return;
    }
    pendingStmt = stmt;
    if (!stmt[1]) stmt[1] = undefined;
    function next() {
      if (pendingStmt === stmt) {
        pendingStmt = null;
        schedule();
        if (error) self.close();
        callbacks();
      }
    }
    wrapper.execute(connection, stmt[0], stmt[1], 
      function(row) {
        resolver.emit('row', row);
        next();
      },
      function() {
        resolver.emit('end');
        next();
      },
      function(err) {
        if (err && error === null) error = err;
        resolver.emit('error', error);
        next();
      }
    );
  }

  function runIfNext(stmt) {
    if (pendingStmt === null && connection !== null && queue[0] === stmt)
      run(queue.shift());
  }

  /**
   * Schedule a SQL statement for execution.
   *
   * @param {String} sql Statement to be scheduled.
   * @param {Array} params Parameters that will be safely interpolated on the
   * SQL string.
   * @return {ResultPromise} Promise to the result of executing the statement.
   */
  this.execute = function(sql, params) {
    checkState();
    if (!params)
      params = undefined;
    var resolver = new events.EventEmitter()
    , stmt = [sql, params, resolver];
    queue.push(stmt);
    runIfNext(stmt);
    return new ResultPromise(resolver);
  }

  /**
   * If the connection came from a pool it is returned, else the connection is
   * closed.
   */
  this.release = function() {
    if (!released) {
      releaser(connection);
      released = true;
    }
  }

  /**
   * Closes the connection
   */
  this.close = function() {
    if (!released) {
      closer(connection);
      released = true;
    }
  } 

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
  this.begin = function(isolation) {
    if (!queue)
      return;
    var sql = wrapper.begin(isolation);
    if (Array.isArray(sql))
      for (var i = 0; i < sql.length; i++) {
        var stmt = [sql[i], null, null];
        queue.push(stmt);
        runIfNext(stmt);
     }
    else {
      var stmt = [sql, null, null];
      queue.push(stmt);
      runIfNext(stmt);
    }
  };

  /**
   * Commits the current pending transaction. If no transaction is currently
   * pending, an error is thrown.
   */
  this.commit = function() {
    var stmt = [wrapper.commit(), null, null];
    if (queue) {
      queue.push(stmt);
      runIfNext(stmt);
    }
  };

  /**
   * Creates a savepoint that can be reverted to.
   *
   * @param {String} savepoint Savepoint identifier that can be passed to
   * 'rollback'.
   */
  this.save = function(savepoint) {
    var stmt = [wrapper.save(savepoint), null, null];
    if (queue) {
      queue.push(stmt);
      runIfNext(stmt);
    }
  };

  /**
   * Reverts the changes made by the currently pending transaction to the
   * specified savepoint. If no savepoint is specified, the rollback will
   * revert all changes since 'begin' was last called.
   *
   * @param {String} savepoint Savepoint to revert.
   */
  this.rollback = function(savepoint) {
    var stmt = [wrapper.rollback(savepoint), null, null];
    if (queue) {
      queue.push(stmt);
      runIfNext(stmt);
    }
  };

  /**
   * Sets a handler to be executed when the connection is ready.
   *
   * @param {Function} cb Callback to be invoked when the connection is ready.
   */
  this.ready = function(cb) {
    readyCallback = cb;
    callbacks();
  };

  /**
   * Sets a handler to be executed if a error happens during connection or 
   * execution of a statement. If there's a pending error when the handler
   * is set, it will be executed imediately. When an error occurs, the 
   * connection can no longer be used. 
   *
   * @param {Function} cb Callback to be invoked if a error occurs.
   */
  this.error = function(cb) {
    errorCallback = cb;
    callbacks();
  };
}

/**
 * Manages/limits connections to the database.
 */
function ConnectionPool(wrapper, connectionString, max, timeout) {
  if (typeof max !== "number")
    max = 10;
  if (typeof timeout !== "number")
    timeout = 30000;
  var inner = genericPool.Pool({
    create: function(cb) { wrapper.connect(connectionString, cb); },
    destroy: function(connection) {
      wrapper.close(connection);
    },
    max: max,
    idleTimeoutMillis: timeout
  });

  /**
   * Starts resolving a connection to the database and returns a promise to it.
   *
   * @return {ConnectionPromise} Object that can be used to interact with the
   * database.
   */
  this.get = function() {
    var destroyed = false
    , released = false
    , resolver = new events.EventEmitter()
    , rv = new ConnectionPromise(wrapper, resolver, 
    function(connection) {
      if (connection === null) released = true; // release as soon as it is available
      else inner.release(connection);
    }, 
    function(connection) { 
      if (connection === null) destroyed = true; // destroy as soon as it is available
      else inner.destroy(connection); 
    });
    inner.acquire(function(err, connection) {
      if (destroyed && connection) inner.destroy(connection);
      else if (released && connection) inner.release(connection);
      else resolver.emit('resolve', err, connection);
    });
    return rv;
  }
}

/**
 * Represents the future result of a SQL statement. This object will be 
 * 'consumed' when a callback passed to any of its result-consuming methods, so
 * it can only be used to consume results once.
 *
 * @param resolver EventEmitter that will emit a 'resolve' event when
 * the result is ready.
 */
function ResultPromise(resolver) {
  var self = this
  , buffer = []
  , rowIdx = 0
  , error = null
  , callback = null
  , then = null
  , finished = false
  , discard = false
  , done = false;

  function flush() {
    if (typeof callback === 'function') callback();
    if (typeof then === 'function' && finished) then();
  }

  resolver.on('row', function(row) {
    if (buffer === null) return;
    buffer.push(row);
    flush();
  });
  resolver.on('end', function() {
    finished = true;
    flush();
  });
  resolver.on('error', function(err) {
    error = err;
    finished = true;
    flush();
  });

  function consume(cb) {
    if (typeof callback === 'function') throw new Error('Result callback already set');
    callback = cb;
  }

  /**
   * Waits for all rows and pass them to the callback.
   *
   * @param {Function} cb Callback used to return all rows.
   */
  this.all = function(cb) {
    consume(function() {
      if (finished && buffer !== null) {
        cb(buffer);
        buffer = null;
      }
    });
    flush();
    return self;
  };

  /**
   * Iterates through each result row as soon as it is available, calling the
   * callback with the row/index as first/second arguments
   *
   * @param {Function} cb Callback used to return each row.
   */
  this.each = function(cb) {
    consume(function() {
      if (buffer !== null)
        for (var i = 0;i < buffer.length;i++) {
          cb(buffer.shift(), rowIdx);
          rowIdx++;
        }
    });
    flush();
    return self;
  };

  /**
   * Gets the first row in the result and invokes the callback with it.
   *
   * @param {Function} cb Callback used to return the first row.
   */
  this.first = function(cb) {
    consume(function() {
      if (buffer !== null && buffer.length > 0) {
        cb(buffer[0]);
        buffer = null; // discard the rest of the rows
      } else if (finished && error !== null) {
        cb(null);
      }
    });
    flush();
    return self;
  };

  /**
   * Gets a single scalar value from the first row in the result and invokes 
   * the callback with it. Only call this when the result contains only one 
   * column.
   *
   * @param {Function} cb Callback used to return the scalar.
   */
  this.scalar = function(cb) {
    return self.first(function(firstRow) {
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
  this.then = function(cb) {
    if (then !== null) throw new Error('\'then\' callback already set');
    then = function() {
      if (!done) {
        done = true;
        cb(error);
      }
    };
    flush();
    return self;
  }
}

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
