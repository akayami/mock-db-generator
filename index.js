var generator = require('./lib/engine.js');
var fs = require('fs');

module.exports = function(options) {
	return {
		create: function() {
			options.conn.connect(function(err) {
				if (err) {
					console.error('error connecting: ' + err.stack);
					return;
				}
				fs.readFile(options.structureFile, function(err, sql) {
					if (err) {
						console.error('error connecting: ' + err.stack);
						return;
					}
					var gen = generator(options.conn, options.schemaName, sql.toString());
					var data = require(options.dataFile);
					gen.make(data);
					gen.run(function(err, result) {
						if (err) {
							console.log(err);
							console.log(err.stack);
							connection.close();
							return;
						}
						options.conn.query('DROP DATABASE IF EXISTS ??', [options.schemaName], function(err, result) {
							console.log('Test Data Cleaned Up')
							console.log('Done');
							options.conn.close();
						})
					});
				});
			});
		}
	}
}
