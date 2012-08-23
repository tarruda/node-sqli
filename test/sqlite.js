var sqlite = require('../src/sqli').getDriver('sqlite');
var createSuite = require('./common').createSuite;

if (sqlite) {
  var connStr = ':memory:';
  createSuite(sqlite.createPool(connStr, 1));
}
