function factory(pg) {
  var sqli = require('./sqli');
  sqli.register('postgres', {
    connect: function(connectionString, cb) {
      var client = new pg.Client(connectionString);
      client.connect(function(err) {
        if (err) return cb(err, null);
        return cb(null, client);
      });
    },
    close: function(client) {
      client.end();
    },
    execute: function(client, sql, params, rowCb, endCb, errCb) {
      var q = client.query(replaceQMarks(sql), params);
      q.on('row', rowCb).on('end', endCb).on('error', errCb);
    },
    begin: function(isolation) {
      var id = 'READ COMMITTED';
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
      return 'START TRANSACTION ISOLATION LEVEL ' + id;
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
  factory(require('pg'));
} catch (error) {
  console.log('Could not load postgres wrapper ' + error.message);
}

/**
 * Replaces question marks in the sql string by postgres prepared statement
 * placeholders($1, $2...). Does minor parsing of the string in order to
 * ignore question marks inside quotes.
 */
function replaceQMarks(sql) {
  var insideQuote = false
  , parameterIndex = 1
  , currentIndex = 0
  , rv = [];
  for (var i = 0; i < sql.length; i++) 
    if (insideQuote) {
      if (sql[i] === "'")
        insideQuote = false;
    } else { 
      if (sql[i] === '?') {
        rv.push(sql.substring(currentIndex, i));
        rv.push('$' + parameterIndex);
        parameterIndex++;
        currentIndex = i + 1;
      }
      else if (sql[i] === "'")
        insideQuote = true;
    }
  rv.push(sql.substring(currentIndex));
  return rv.join('');
}

exports.replaceQMarks = replaceQMarks;
