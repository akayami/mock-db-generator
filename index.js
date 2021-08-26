const async = require('async');
const fs = require('fs');

module.exports = function (conn, structSQL) {

	/**
	 * Prepares the tasks array starting with an empty task to setup passing of conn
	 * @type {((function(*=): void)|*)[]}
	 */
	const structCreationTasks = [
		function (asyncCB) {
			asyncCB(null, conn, asyncCB);
		},
	];

	/**
	 * Adding statements from structSQL
	 */
	structSQL.forEach(function (statement) {
		if (statement.trim().length) {
			structCreationTasks.push((conn, result, asyncCB) => {
				conn.query(statement, function (err, result) {
					if (err) {
						err.query = statement;
						asyncCB(err);
					} else {
						asyncCB(null, conn, result);
					}
				});
			});
		}
	});

	/**
	 * Initializes DB after creation. Useful for pgsql
	 */
	structCreationTasks.push(function (conn, result, asyncCB) {
		conn.init(function (err, result) {
			asyncCB(err, conn, result);
		});
	});

	/**
	 * Main Routine - First executes DB creation part, and it follows seeding the db with data.
	 */
	const dbCompleteRoutine = [
		function (mainCB) {
			async.waterfall(structCreationTasks, function (err, result) {
				if (err) {
					mainCB(err);
				} else {
					mainCB(null, result);
				}
			});
		},
	];

	return {
		make: function (data) {
			const map = {};
			data.forEach(function (block, blockIndex) {
				for (const table in block) {
					if (!map[table]) {
						map[table] = [];
					}
					map[table].push(blockIndex);

					block[table].forEach(function (item) {
						dbCompleteRoutine.push(function (conn, cb) {
							for (const key in item) {
								if (typeof item[key] == 'function') {
									//item[key] = item[key](data, {});
									item[key] = item[key](data, {
										get: function (table, index, block) {
											let blockIndex;
											if (!this.map[table]) {
												throw new Error('Table not available: ' + table);
											}
											if (block) {
												blockIndex = this.map[table].indexOf(block);
											} else {
												blockIndex = 0;
											}
											if (blockIndex === -1) {
												throw new Error(`Table ${table} not available in block: ` + block);
											}
											return this.data[this.map[table][blockIndex]][table][index];
										}.bind({data: data, map: map}),
										self: function () {
											return this.item;
										}.bind({item: item})
									});
								}
							}
							conn.insert(table, item, function (err, result) {
								if (err) {
									cb({context: table, item: item, error: err});
								} else {
									item._insertId = result.insertId;
									cb(null, conn);
								}
							});
						});
					});
				}
			});
		},

		run: function (cb) {
			async.waterfall(dbCompleteRoutine, function (err, result) {
				if (err) {
					cb(err, null);
				} else {
					cb(null, result);
				}
			});
		},
		getTasks: () => {
			return structCreationTasks;
		}

	};
};
