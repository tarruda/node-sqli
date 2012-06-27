var sqlite = require('../lib/sqli').getDriver('sqlite')
, createSuite = require('./common').createSuite;

if (sqlite) {
  createSuite(sqlite.connect(':memory:'));
}
