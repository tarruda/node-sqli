var mysql = require('../src/sqli').getDriver('mysql')
, createSuite = require('./common').createSuite;


if (mysql) {
  var connStr = { 
    socketPath: '/var/run/mysqld/mysqld.sock',
    database: 'test'
  };
  createSuite(mysql.createPool(connStr, 1));
}
