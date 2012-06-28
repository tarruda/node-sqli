var postgres = require('../lib/sqli').getDriver('postgres')
, replaceQMarks = require('../lib/postgres').replaceQMarks
, createSuite = require('./common').createSuite
, assert = require('assert');

suite('Postgres -', function() {
  test('Question marks', function() {
    assert.equal(replaceQMarks(
    'select * from stub where col1 = ? and col2 = ?'),
    'select * from stub where col1 = $1 and col2 = $2');
  });
  test('Question marks inside quotes', function() {
    assert.equal(replaceQMarks(
    "select * from stub where col1 = ? and col2='qmark?qmark' and col3 = ?"),
    "select * from stub where col1 = $1 and col2='qmark?qmark' and col3 = $2");
  });
  test('Ending with other characters', function() {
    assert.equal(replaceQMarks(
      'INSERT INTO common_tests (id, stringCol) VALUES(?, ?)'),
      'INSERT INTO common_tests (id, stringCol) VALUES($1, $2)');
  });
});

if (postgres) {
  var connStr = 'tcp://postgres:123@localhost/postgres';
  // only test if it is possible to connect
  var tmp = postgres.connect(connStr).ready(function() {
    createSuite(postgres.createPool(connStr, 1), {
      blobType: 'bytea'
    });
  });
}
