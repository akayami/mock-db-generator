var generator = require('./lib/engine.js');
var fs = require('fs');

module.exports = function(options) {
	return {
		create: function(cb) {
			fs.readFile(options.structureFile, function(err, sql) {
				if (err) {
					console.error('error connecting: ' + err.stack);
					return;
				}
				var gen = generator(options.conn, options.schemaName, sql.toString());
				var data = require(options.dataFile);
				gen.make(data);
				gen.run(function(err, result) {
					cb(err);
				});
			});
		}
	}
}
