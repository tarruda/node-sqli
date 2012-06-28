var sqlite = require('../lib/sqli').getDriver('sqlite')
, createSuite = require('./common').createSuite;

if (sqlite) {
  var connStr = ':memory:';
  // only test if it is possible to connect
  var tmp = sqlite.connect(connStr).ready(function() {
    createSuite(sqlite.createPool(connStr, 1));
  });
}
