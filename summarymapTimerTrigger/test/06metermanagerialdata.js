var expect = require("chai").expect;
var moment = require("moment");

var objMeterManagerialData = require('../controllers/metermanagerialdatacontroller.js');
var objInputModel = require('../model/metermanagerialdatadao.js');
var objSetup = require('../test/setup/setup.js');
var objConfig = require('../config.js');
var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objManagerialDataModel = require('../model/sqltables/managerialdatamodel.js');

describe("Meter managerial data test cases", function () {

	before(function(done) {
		objSetup.cleanupData(function() {
			objSetup.initData(done);
		});
	});

	this.timeout(objConfig.testcasetimeout);
	it("validate whether number of record inserted matches the expected count", function (done) {
		var context = console;
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				objInputModel.updateMeterManagerialDataToRDBMS(context, function(err, data) {
					objdaoimpl.findAll("managerialdata", objManagerialDataModel.objTableColumns, objManagerialDataModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(5);
							done();									
						});
				});
			});
		});		
	});

	it("validate whether number of record inserted matches the expected count through controller", function (done) {
		var context = console;
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				objMeterManagerialData.updateMeterManagerialDataToRDBMS(context, function(err, obj) {
					objdaoimpl.findAll("managerialdata", objManagerialDataModel.objTableColumns, objManagerialDataModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(5);
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