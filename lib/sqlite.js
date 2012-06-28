function factory(sqlite3) {
  var sqli = require('./sqli');
  sqli.register('sqlite', {
    connect: function(filename, cb) {
      var db = new sqlite3.Database(filename, function(err) {
        if (err) return cb(err, null);
        cb(null, db);
      });
    },
    close: function(db) {
      db.close();
    },
    execute: function(db, sql, params, cb) {
      db.all(sql, params, function(err, rows) {
        if (err) cb(err);
        else cb(null, {
          each: function(rowCb, endCb) {
            for (var i = 0; i < rows.length; i++)
              rowCb(rows[i]);
            if (typeof endCb === 'function')
              endCb();
          }
        });
      });
    },
    begin: function(isolation) {
      switch (isolation) {
        case sqli.READ_UNCOMMITTED:
          return 'PRAGMA read_uncommitted = true; BEGIN TRANSACTION';
        case sqli.REPEATABLE_READ:
          return 'BEGIN IMMEDIATE TRANSACTION';
        case sqli.SERIALIZABLE:
          return 'BEGIN EXCLUSIVE TRANSACTION';
        default:
          return 'BEGIN TRANSACTION';
      }
    },
    save: function(savepoint) {
      return 'SAVEPOINT ' + savepoint;
    },
    commit: function() {
      return 'COMMIT';
    },
    rollback: function(savepoint) {
      if (!savepoint)
        return 'ROLLBACK';
      return 'ROLLBACK TO ' + savepoint;
    }
  });
};

try {
  factory(require('sqlite3'));
} catch (error) {
  console.log('Could not load sqlite3 wrapper ' + error.message);
}
