const should = require('chai').should();
const expect = require('chai').expect;
const async = require('async');
const merge = require('merge');

describe('Tests', () => {

	const cluster = require('../../db-cluster-mysql');

	let struct = require('fs').readFileSync(require('path').resolve(__dirname, './data/struct.sql'));
	struct = String(struct).split(';');

	const database = 'mock-db-test-' + process.pid;

	const dbconf = {
		host: 'localhost',
		user: 'root'
		//database: 'mock-db-test-' + process.pid
	};
	const maintenacePool = cluster.getPool(require('mysql'), dbconf);
	const pool = cluster.getPool(require('mysql'), merge(true, dbconf, {database: database}));


	it('Needs to generate a proper object', () => {
		const output = require('../index')({}, struct);
		expect(output).to.be.an('Object');
		expect(output).to.have.property('make').to.be.a('function');
		expect(output).to.have.property('run').to.be.a('function');
	});


	it('Needs to create the db', done => {
		pool.getConnection((err, conn) => {
			if (err) return done(err);
			const generator = require('../index')(conn, struct);
			generator.make(require('./data/file1.js')({}));
			generator.run((err, res) => {
				expect(err).to.be.null;
				expect(res).not.to.be.null;
				conn.query('SHOW TABLES', (err, res) => {
					expect(err).to.be.null;
					expect(res.length).equal(2);
					conn.query('select * from test', (err, res) => {
						expect(err).to.be.null;
						expect(res.length).equal(2);
						expect(res.rows()[0].id).equal(1);
						expect(res.rows()[0].address).equal('address1');
						expect(res.rows()[1].id).equal(2);
						expect(res.rows()[1].address).equal('address2');
						done(err);
					});
				});
			});
		});
	});

	beforeEach((done) => {
		maintenacePool.getConnection((err, conn) => {
			if (err) return done(err);

			const tasks = [
				function (asyncCB) {
					asyncCB(null, conn);
				},
				function (conn, asyncCB) {
					conn.query('DROP DATABASE IF EXISTS ??', [database], function (err, result) {
						if (err) {
							asyncCB(err);
						} else {
							asyncCB(null, conn);
						}
					});
				},
				function (conn, asyncCB) {
					conn.query('CREATE DATABASE ??', [database], function (err, result) {
						if (err) {
							asyncCB(err);
						} else {
							asyncCB(null, conn, result);
						}
					});
				}
			];

			async.waterfall(tasks, function (err, result) {
				if (err) return done(err);
				done();
			});
		});
	});

	afterEach((done) => {
		maintenacePool.getConnection((err, conn) => {
			if (err) return done(err);

			const tasks = [
				function (asyncCB) {
					asyncCB(null, conn);
				},
				function (conn, asyncCB) {
					conn.query('DROP DATABASE IF EXISTS ??', [database], function (err, result) {
						if (err) {
							asyncCB(err);
						} else {
							asyncCB(null, conn);
						}
					});
				},
			];
			async.waterfall(tasks, function (err, result) {
				if (err) return done(err);
				done()
			});
		});
	});

	after((done) => {
		maintenacePool.end((e) => {
			if(e) return done(e);
			pool.end(done);
		});
	});
});