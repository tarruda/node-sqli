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
    execute: function(connection, sql, params, cb) {
      connection.query(sql, params, function(err, results) {
        if (err) cb(err, null);
        else cb(null, {
          each: function(rowCb, endCb) {
            for (var i = 0; i < results.length; i++)
              rowCb(results[i]);
            if (typeof endCb === 'function')
              endCb();
          }
        });
      });
    },
  });
};

try {
  factory(require('mysql'));
} catch (error) {
  console.log('Could not load mysql wrapper ' + error.message);
}
