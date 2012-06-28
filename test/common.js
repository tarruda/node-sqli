var assert = require('assert');

exports.createSuite = function(pool, specificOptions, specificTestsFactory) {
  var options = {
    blobType: 'BLOB',
    stringType: 'TEXT'
  }, conn = null;
  var tests = {
    'error makes connection unusable': function(done) {
      conn.execute('Invalid SQL');
      conn.fail(function(err) { 
        try {
          conn.execute('SELECT 1'); // nothing could go wrong here
        } catch(err) {
          done();
        }
      });
    },
    'inserting strings': function(done) {
      conn.execute('INSERT INTO test (id, stringcol) VALUES(?, ?)', 
          [1, 'String1']);
      conn.execute("INSERT INTO test (id, stringcol) VALUES(2, 'String2')");
      conn.execute('SELECT * FROM test').all(function(rows) {
        assert.equal(2, rows.length);
        assert.equal(rows[0].id, 1);
        assert.equal(rows[0].stringcol, 'String1');
        assert.equal(rows[1].id, 2);
        assert.equal(rows[1].stringcol, 'String2');
        done();
      });
    },
    'deleting': function(done) {
      conn.execute('INSERT INTO test (id) VALUES(?)', [1]);
      conn.execute('INSERT INTO test (id) VALUES(?)', [2]);
      conn.execute('SELECT COUNT(*) FROM test').scalar(function(value) {
        assert.equal(value, 2);
      });
      conn.execute('DELETE FROM test');
      conn.execute('SELECT COUNT(*) FROM test').scalar(function(value) {
        assert.equal(value, 0);
        done();
      });
    },
    'deleting with condition': function(done) {
      conn.execute('INSERT INTO test (id) VALUES(?)', [1]);
      conn.execute('INSERT INTO test (id) VALUES(?)', [2]);
      conn.execute('SELECT COUNT(*) FROM test').scalar(function(value) {
        assert.equal(value, 2);
      });
      conn.execute('DELETE FROM test WHERE id = 2');
      conn.execute('SELECT COUNT(*) FROM test').scalar(function(value) {
        assert.equal(value, 1);
        done();
      });
    }
  };
  if (specificOptions) {
    for (var key in specificOptions)
      options[key] = specificOptions[key];
  }
  if (typeof specificTestsFactory === 'function') {
    var specificTests = specificTestsFactory(conn);
    if (specificTests)
      for (var key in specificTests) 
        tests[key] = specificTests[key];
  }
  suite('Common -', function() {
    setup(function() {
      conn = pool.get();
      conn.execute('DROP TABLE IF EXISTS test'); 
      conn.execute(
        'CREATE TABLE test (' +
          ' id INTEGER PRIMARY KEY,' +
          ' blobcol ' + options.blobType + ',' +
          ' stringcol ' + options.stringType +
      ')');
    });
    teardown(function() {
      conn.release();
    });
    for (var key in tests)
      test(key, tests[key]);
  });
}
