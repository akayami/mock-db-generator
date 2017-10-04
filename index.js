var async = require('async');
var fs = require('fs');

module.exports = function(conn, dbname, structSQL) {

	var tasks = [
		function(asyncCB) {
			asyncCB(null, conn, asyncCB);
		},
	];

	structSQL.forEach(function(statement) {
		if(statement.trim().length) {
			tasks.push(function(conn, result, asyncCB) {
				conn.query(this.sql, function(err, result) {
					if (err) {
						err.query = this.sql;
						asyncCB(err);
					} else {
						asyncCB(null, conn, result);
					}
				});
			}.bind({sql: statement}))
		}
	});

	tasks.push(function(conn, result, asyncCB) {
		conn.init(function(err, result) {
			asyncCB(err, conn, result);
		});
	})

	var routine = [
		function(mainCB) {
			async.waterfall(tasks, function(err, result) {
				if (err) {
					mainCB(err);
				} else {
					mainCB(null, result);
				}
			});
		},
	]

	return {
		make: function(data) {
			var map = {};
			data.forEach(function(block, blockIndex) {
				for (var table in block) {
					if(!map[table]) {
						map[table] = [];
					}
					map[table].push(blockIndex);

					block[table].forEach(function(item) {
						routine.push(function(conn, cb) {
							for (var key in item) {
								if (typeof item[key] == 'function') {
									//item[key] = item[key](data, {});
									item[key] = item[key](data, {
										get: function(table, index, block) {
											if(!this.map[table]) {
												throw new Error('Table not available: ' + table);
											}
											if(block) {
												var blockIndex = this.map[table].indexOf(block);
											} else {
												var blockIndex = 0;
											}
											if(blockIndex === -1) {
												throw new Error(`Table ${table} not available in block: ` + block);
											}
											return this.data[this.map[table][blockIndex]][table][index];
										}.bind({data: data, map: map}),
										self: function() {
											return this.item;
										}.bind({item: item})
									});
								}
							}
							conn.insert(table, item, function(err, result) {
								if (err) {
									cb({context: table, item: item, error: err});
								} else {
									item._insertId = result.insertId;
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
