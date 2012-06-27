function factory(pg) {
  var sqli = require('./sqli')
  , sqlCache = {};
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
    execute: function(client, sql, params, cb) {
      if (sqlCache[sql] === undefined)
        sqlCache[sql] = replaceQMarks(sql);
      client.query(sqlCache[sql], params, function(err, result) {
        if (err) cb(err, null);
        else cb(null, {
          each: function(rowCb, endCb) {
            for (var i = 0; i < result.rows.length; i++)
              rowCb(result.rows[i]);
            if (typeof endCb === 'function')
              endCb();
          }
        });
      });
    },
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
