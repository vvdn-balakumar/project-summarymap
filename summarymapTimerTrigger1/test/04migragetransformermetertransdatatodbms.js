var expect = require("chai").expect;
var moment = require("moment");

var objInputModel = require('../model/transformertransactionmodeldao.js');
var objSetup = require('../test/setup/setup.js');
var objConfig = require('../config.js');
var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objTransformerTransModel = require('../model/sqltables/transformertransactionmodel.js');
var objMeterTransModel = require('../model/sqltables/metertransactionmodel.js');
var objMigrateAllTransactionsData = require('../controllers/migratetransactiontordbms.js');
var objMigrateAllMeterTransactionsData = require('../controllers/migratemetertransactiontordbms.js');

describe("Meter and transaction data migration to RDBMS test cases", function () {
	var intNumRecords = 96;
	before(function(done) {
		objConfig.numberofDaysTransMeterTransaction = 2;
		objSetup.cleanupData(function() {
			objSetup.initData(done);
		});
	});

	this.timeout(objConfig.testcasetimeout);
	it("validate whether number of record inserted for transformer matches the expected count", function (done) {
		var context = console;
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				objInputModel.updateTransformerTransactionDataToRDBMS(context, function(err, data) {
					objdaoimpl.findAll("transformertransactions", objTransformerTransModel.objTransformer, objTransformerTransModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(1 * intNumRecords * 2);
							done();
						});
				});
			});
		});
	});

	it("validate whether number of record inserted for transformer matches the expected count through controller", function (done) {
		var context = console;
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				objMigrateAllTransactionsData.migrateTransactionsToRdbms(context, function(err, obj) {
					objdaoimpl.findAll("transformertransactions", objTransformerTransModel.objTransformer, objTransformerTransModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(1 * intNumRecords * 2);
							done();
						});
				});
			});
		});
	});
	

	it("validate whether number of record inserted for meter matches the expected count", function (done) {
		var context = console;
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				objMigrateAllMeterTransactionsData.migrateTransactionsToRdbms(context, function(err, obj) {
					objdaoimpl.findAll("metertransactions", objMeterTransModel.objMeter, objMeterTransModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(1 * intNumRecords * 4);
							done();
						});
				});
			});
		});		
	});	

	after(function(done){
		objSetup.cleanupData(done);
	})
});