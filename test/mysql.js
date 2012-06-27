var mysql = require('../lib/sqli').getDriver('mysql')
, createSuite = require('./common').createSuite;

var connection = { 
  socketPath: '/var/run/mysqld/mysqld.sock',
  database: 'test'
};

if (mysql) {
  createSuite(mysql.connect(connection));
}
