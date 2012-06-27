var assert = require('assert');

exports.createSuite = function(conn, specificOptions, specificTestsFactory) {
  var options = {
    blobType: 'BLOB',
    stringType: 'TEXT'
  };
  var tests = {
    'errors': function(done) {
      var expected = 2;
      conn.execute('Invalid SQL').fail(function(err) {
        decrement();
      });
      conn.execute('INSERT INTO missing VALUES(1, 2)').fail(function(err) {
        decrement();
      });
      function decrement() {
        expected--;
        if (expected === 0)
          done();
      }
    },
    'inserting strings': function(done) {
      conn.execute('INSERT INTO common_tests (id, stringCol) VALUES(?, ?)', 
          [1, 'String1']);
      conn.execute("INSERT INTO common_tests (id, stringCol) VALUES(2, 'String2')");
      conn.execute('SELECT * FROM common_tests').all(function(rows) {
        assert.equal(2, rows.length);
        assert.equal(rows[0].id, 1);
        assert.equal(rows[0].stringCol, 'String1');
        assert.equal(rows[1].id, 2);
        assert.equal(rows[1].stringCol, 'String2');
        done();
      });
    },
    'deleting': function(done) {
      conn.execute('INSERT INTO common_tests (id) VALUES(?)', [1]);
      conn.execute('INSERT INTO common_tests (id) VALUES(?)', [2]);
      conn.execute('SELECT COUNT(*) FROM common_tests').scalar(function(value) {
        assert.equal(value, 2);
      });
      conn.execute('DELETE FROM common_tests');
      conn.execute('SELECT COUNT(*) FROM common_tests').scalar(function(value) {
        assert.equal(value, 0);
        done();
      });
    },
    'deleting with condition': function(done) {
      conn.execute('INSERT INTO common_tests (id) VALUES(?)', [1]);
      conn.execute('INSERT INTO common_tests (id) VALUES(?)', [2]);
      conn.execute('SELECT COUNT(*) FROM common_tests').scalar(function(value) {
        assert.equal(value, 2);
      });
      conn.execute('DELETE FROM common_tests WHERE id = 2');
      conn.execute('SELECT COUNT(*) FROM common_tests').scalar(function(value) {
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
    setup(function(done) {
      conn.execute('DROP TABLE IF EXISTS common_tests');
      conn.execute(
        'CREATE TABLE common_tests (' +
          ' id INTEGER PRIMARY KEY,' +
          ' blobCol ' + options.blobType + ',' +
          ' stringCol ' + options.stringType +
      ')').then(done);
    });
    for (var key in tests)
      test(key, tests[key]);
  });
}
