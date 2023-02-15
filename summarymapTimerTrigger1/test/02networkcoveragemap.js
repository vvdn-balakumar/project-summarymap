var expect = require("chai").expect;
var moment = require("moment");
var objNetworkCoverageMap = require('../controllers/networkcoveragemapcontroller.js');
var objInputModel = require('../model/networkcoveragemapinput.js');
var objOutputModel = require('../model/networkcoveragemapoutput.js');

var objSetup = require('../test/setup/setup.js');
var objConfig = require('../config.js');
var objNetworkCoverageMapModel = require('../model/sqltables/networkcoveragemodel.js');
var objdaoimpl = require('../dao/mysqldaoimpl.js');

describe("Network coverage map data test cases", function () {
	before(function(done) {
		objSetup.cleanupData(function() {
			objSetup.initData(done);
		});
	});

	this.timeout(objConfig.testcasetimeout);
	it("validate whether number of record inserted matches the expected count", function (done) {
		var context = console;
		objInputModel.getAllNetworkCoverageMapRelatedDetails(function(err, result) {
			expect(result).not.to.equal(null);
            objOutputModel.postAllNetworkCoverageMapRelatedDetails(result,function(err, data){
				expect(err).to.equal(null);
				objdaoimpl.findAll("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage, objNetworkCoverageMapModel.objTableProps,
					null,
					null,
					function(err, results) {
						//TODO: to calculate based on timestamp
						//expect(results.length).to.equal(720);
						done();
					});
			});
		});
	});

	it("validate whether number of record inserted matches the expected count through controller", function (done) {
		var context = console;

		objNetworkCoverageMap.getNetworkCoverageMap(context, function (err, obj) {
			expect(err).to.equal(null);
			objdaoimpl.findAll("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage, objNetworkCoverageMapModel.objTableProps,
				null,
				null,
				function(err, results) {
					//TODO: to calculate based on timestamp
					// expect(results.length).to.equal(720);
					done();
				});
		});

	});

	it("validate with wrong mongodb host", function (done) {
		var oldURL = objConfig.inputdatasource.url;
		objConfig.inputdatasource.url = 'mongodb://' + "wronglocalhost" + ':' + objConfig.inputdatasource.port + '/' +
										objConfig.inputdatasource.database + '?authSource=' + objConfig.inputdatasource.database;

		var context = console;
		objInputModel.getAllNetworkCoverageMapRelatedDetails(function(err, result) {
			expect(err).to.equal(null);
			expect(result).not.to.equal(null);
			objConfig.inputdatasource.url = oldURL;
			done();
		});
	});

	after(function(done){
	    objSetup.cleanupData(done);
	})
});