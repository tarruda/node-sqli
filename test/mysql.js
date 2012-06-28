var mysql = require('../lib/sqli').getDriver('mysql')
, createSuite = require('./common').createSuite;


if (mysql) {
  var connStr = { 
    socketPath: '/var/run/mysqld/mysqld.sock',
    database: 'test'
  };
  // only test if it is possible to connect
  var tmp = mysql.connect(connStr).ready(function() {
    createSuite(mysql.createPool(connStr, 1));
  });
}
