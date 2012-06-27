var genericPool = require('generic-pool')
, events = require('events');


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
    return new ConnectionPromise(wrapper, resolver, function(connection) {
      if (typeof wrapper.close === 'function')
        wrapper.close(connection);
    });
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
    return new ConnectionPool(self, connectionString, max, timeout);
  };
}

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
function ConnectionPromise(wrapper, resolver, releaser) {
  var self = this
  , queue = []
  , connection = null
  , lastError = null
  , released = false
  , pendingStmt = false
  , readyCallback = null
  , errorCallback = null;
  resolver.once('resolve', function(err, conn) {
    if (err) lastError = err;
    else connection = conn;
    schedule();
    callbacks();
  });

  function callbacks() {
    if (typeof errorCallback === 'function' && lastError !== null) {
      errorCallback(lastError);
      lastError = null; // Only handle each error once
    }
    if (typeof readyCallback === 'function' && connection !== null) {
      readyCallback();
      readyCallback = null; // Only execute this once
    }
  }

  function schedule() {
    process.nextTick(function() {
      if (pendingStmt)
        schedule();
      else if (connection !== null && queue[0] !== undefined)
        run(queue.shift());
    });
  }

  function run(stmt) {
    pendingStmt = true;
    if (connection !== null) {
      wrapper.execute(connection, stmt[0], stmt[1], function(err, innerCursor) {
        pendingStmt = false;
        schedule();
        if (err) lastError = err;
        var resolver = stmt[2];
        resolver.emit('resolve', err, innerCursor);
      });
    }
    callbacks();
  }

  /**
   * Enqueue a SQL statement for execution.
   *
   * @param {String} sql Statement to be sent.
   * @param {Array} params Parameters that will be safely interpolated on the
   * SQL string.
   * @return {ResultPromise} Promise to the result of executing the statement.
   */
  this.execute = function(sql, params) {
    if (released) throw new Error('Connection has already been released');
    if (!params)
      params = [];
    var resolver = new events.EventEmitter()
    , stmt = [sql, params, resolver];
    queue.push(stmt);
    schedule(); 
    return new ResultPromise(resolver);
  }

  /**
   * Sends a 'release' signal to the connection manager(Can no longer be used
   * after this).
   */
  this.release = function() {
    releaser(connection);
    released = true;
  }

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
   * execution of a statement. The handler will be executed at most once per
   * error. If there's a pending error when the handler is set, it will be 
   * executed imediately.
   *
   * @param {Function} cb Callback to be invoked if a error occurs.
   */
  this.fail = function(cb) {
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
      if (typeof wrapper.close === 'function')
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
    inner.acquire(function(err, connection) {
      resolver.emit('resolve', err, connection);
    });
    return new ConnectionPromise(wrapper, resolver, function(connection) {
      inner.release(connection);
    });
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
  , errorCallback = null
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
    if (typeof errorCallback === 'function' && error !== null) {
      errorCallback(error);
      error = null; // only handle the error once
    }
    if (typeof afterCallback === 'function' && resolved) {
      afterCallback();
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

  function each(cb) {
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
    });
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
  this.each = function(cb) {
    consume();
    callback = function() { each(cb); };
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

  /**
   * Sets a handler to be executed if a error happens during the execution of
   * this statement.
   *
   * @param {Function} cb Callback to be invoked if a error occurs.
   */
  this.fail = function(cb) {
    errorCallback = cb;
    callbacks();
    return self;
  };
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
