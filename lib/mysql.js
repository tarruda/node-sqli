function factory(mysql) {
  var sqli = require('./sqli')
  sqli.register('mysql', {
    connect: function(connectionString, cb) {
      var connection = mysql.createConnection(connectionString);
      connection.connect(function(err) {
        if (err) return cb(err, null);
        return cb(null, connection);
      });
    },
    close: function(connection) {
      connection.end();
    },
    execute: function(connection, sql, params, rowCb, endCb, errCb) {
      var q = connection.query(sql, params);
      q.on('result', rowCb).on('end', endCb).on('error', errCb);
    },
    begin: function(isolation) {
      var id = 'READ COMMITTED';
      if (isolation !== undefined) {
        switch (isolation) {
          case sqli.READ_UNCOMMITTED:
            id = 'READ UNCOMMITTED';
            break;
          case sqli.REPEATABLE_READ:
            id = 'REPEATABLE READ';
            break;
          case sqli.SERIALIZABLE:
            id = 'SERIALIZABLE';
            break;
        }
      }
      return ['SET TRANSACTION ISOLATION LEVEL ' + id, 'START TRANSACTION'];
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
      return 'ROLLBACK TO SAVEPOINT ' + savepoint;
    }
  });
};

try {
  factory(require('mysql'));
} catch (error) {
  console.log('Could not load mysql wrapper ' + error.message);
}
