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
    execute: function(db, sql, params, rowCb, endCb, errCb) {
      db.each(sql, params, function(err, row) {
        if (err) errCb(err);
        else rowCb(row);
      }, function(err, count) {
        endCb(err);
      });
    },
    begin: function(isolation) {
      switch (isolation) {
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
