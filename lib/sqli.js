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
  , errorCallback = null;
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
    if (typeof errorCallback === 'function' && error !== null) {
      errorCallback(error);
      error = null; // Only handle each error once
    }
    if (typeof readyCallback === 'function' && connection !== null) {
      readyCallback();
      readyCallback = null; // Only execute this once
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
    if (error && resolver instanceof events.EventEmitter) {
      resolver.emit('resolve', error);
      schedule();
    }
    checkState();
    pendingStmt = stmt;
    if (connection !== null) {
      if (!stmt[1])
        stmt[1] = undefined;
      var buffer = []
      , finished = false
      , rCb = null
      , eCb = null;
      var cursor = {
        each: function(rowCb, endCb) {
          if (rCb === null) rCb = rowCb;
          if (eCb === null) eCb = endCb;
          flushBuffer();
        }
      };
      function flushBuffer() {
        if (typeof rCb == 'function')
          for (var i = 0;i < buffer.length;i++) rCb(buffer.shift());       
        if (finished && typeof eCb === 'function') eCb();
      }
      function processStmt() {
        if (pendingStmt === stmt) {
          pendingStmt = null;
          if (!error) schedule();
          else self.close();
          if (resolver instanceof events.EventEmitter)
            resolver.emit('resolve', error, cursor);
          callbacks();
        }
      }
      wrapper.execute(connection, stmt[0], stmt[1], 
        function(row) {
          buffer.push(row);
          processStmt();
          flushBuffer();
        },
        function() {
          if (finished) return;
          finished = true;
          processStmt();
          flushBuffer();
        },
        function(err) {
          if (finished) return;
          error = err;
          finished = true;
        }
      );
    }
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
    var resolver = new events.EventEmitter();
    var rv = new ConnectionPromise(wrapper, resolver, function(connection) {
      inner.release(connection);
    }, function(connection) { inner.destroy(connection); });
    inner.acquire(function(err, connection) {
      resolver.emit('resolve', err, connection);
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
  , cursor = null
  , error = null
  , resolved = false
  , released = false
  , callback = null
  , afterCallback = null;
  resolver.once('resolve', function(err, curs) {
    if (err) error = err;
    else cursor = curs;
    resolved = true;
    callbacks();
  });

  function consume() {
    if (released) throw new Error('This result has been consumed');
    released = true;
  }

  function callbacks() {
    if (typeof callback === 'function' && cursor !== null && error === null) {
      callback();
    }
    if (typeof afterCallback === 'function' && resolved) {
      afterCallback(error);
      afterCallback = null; // only run this once
    }
  }

  function all(cb) {
    var buffer = [];
    cursor.each(function(row) {
      buffer.push(row);
    }, function() {
      cb(buffer);
    });
  }

  function each(cb, endCb) {
    var idx = 0;
    cursor.each(function(row) {
      if (idx !== -1) {
        var result = cb(row, idx);
        if (result === false) {
          idx = -1;
          return false;
        }
        idx++;
      } else return false;
    }, endCb);
  }

  function first(cb) {
    var done = false;
    cursor.each(function(row) {
      if (!done) {
        cb(row);
        done = true;
      } else return false;
    });
  }

  function scalar(cb) {
    var done = false;
    cursor.each(function(row) {
      if (!done) {
        for (var k in row) {
          cb(row[k]);
          break;
        }
        done = true;
      } else return false;
    });
  }

  /**
   * Buffers all rows and pass it to the callback.
   *
   * @param {Function} cb Callback used to return all rows.
   */
  this.all = function(cb) {
    consume();
    callback = function() { all(cb); };
    callbacks();
    return self;
  };

  /**
   * Iterates through each result row as soon as it is available, calling the
   * callback with the row as argument.
   *
   * @param {Function} cb Callback used to return each row.
   */
  this.each = function(cb, endCb) {
    consume();
    callback = function() { each(cb, endCb); };
    callbacks();
    return self;
  };

  /**
   * Gets the first row in the result and invokes the callback with it.
   *
   * @param {Function} cb Callback used to return the first row.
   */
  this.first = function(cb) {
    consume();
    callback = function() { first(cb); };
    callbacks();
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
    consume();
    callback = function() { scalar(cb); };
    callbacks();
    return self;
  };

  /**
   * Executes a handler when the the result is resolved
   *
   * @param {Function} cb Handler to execute.
   */
  this.then = function(cb) {
    afterCallback = cb;
    callbacks();
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
