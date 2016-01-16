var async = require('async');
var fs = require('fs');

module.exports = function(conn, dbname, structSQL) {
	var routine = [
		function(mainCB) {
			async.waterfall([
				function(asyncCB) {
					conn.query('DROP DATABASE IF EXISTS ??', [dbname], function(err, result) {
						if (err) {
							asyncCB(err);
						} else {
							asyncCB(null, conn);
						}
					})
				},
				function(conn, asyncCB) {
					conn.query('CREATE DATABASE IF NOT EXISTS ??', [dbname], function(err, result) {
						if (err) {
							asyncCB(err);
						} else {
							asyncCB(null, conn, result);
						}
					})
				},
				function(conn, result, asyncCB) {
					conn.query('USE ??', [dbname], function(err, result) {
						if (err) {
							asyncCB(err);
						} else {
							asyncCB(null, conn, result);

						}
					})
				},
				function(conn, result, asyncCB) {
					conn.query(structSQL, function(err, result) {
						if (err) {
							asyncCB(err);
						} else {
							asyncCB(null, conn, result);

						}
					})
				},
			], function(err, result) {
				if (err) {
					mainCB(err);
				} else {
					mainCB(null, result);
				}
			});
		},
		function(conn, mainCB) { // Select appropraite db
			conn.query('USE ??', [dbname], function(err, result) {
				if (err) {
					mainCB(err);
				} else {
					mainCB(null, conn);
				}
			})
		}
	]

	return {
		make: function(data) {
			data.forEach(function(block) {
				for (var table in block) {
					block[table].forEach(function(item) {
						routine.push(function(conn, cb) {
							for (var key in item.input) {
								if (typeof item.input[key] == 'function') {
									item.input[key] = item.input[key](data);
								}
							}
							conn.insert(table, item.input, function(err, result) {
								if (err) {
									cb(err);
								} else {
									item.result = {
										insertId: result.insertId
									}
									cb(null, conn);
								}
							})
						})
					})
				}
			});
		},

		run: function(cb) {
			async.waterfall(routine, function(err, result) {
				if (err) {
					cb(err, null);
				} else {
					cb(null, result);
				}
			});
		}
	}
}
