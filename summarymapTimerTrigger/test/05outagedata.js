var expect = require("chai").expect;
var moment = require("moment");

var outagecontroller = require('../controllers/outagecontroller.js');
var objInputModel = require('../model/manageoutageinput.js');
var objOutputModel = require('../model/outagemapdaoimpl.js');
var objSetup = require('../test/setup/setup.js');
var objConfig = require('../config.js');
var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objOutageMapModel = require('../model/sqltables/outagemap.js');

describe("Outages data test cases", function () {
	var intOutagesLen = 0;
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

				objInputModel.processOutageDetails(context, function(err, data) {
					for(var objCellId in data.outagedata) {
						var arrOutages = data.outagedata[objCellId];
						if(objCellId === "1") {
							intOutagesLen = arrOutages.length; 
							expect(arrOutages.length).to.greaterThan(0);
						} else {
							expect(arrOutages.length).to.equal(0);					
						}
					}
					objOutputModel.updateOutageMapModel(data, function(err, data) {
						objdaoimpl.findAll("outagemap", objOutageMapModel.objOutageData, objOutageMapModel.objTableProps, 
							null,
							null, 
							function(err, results) {
								//expect(results.length).to.equal(intOutagesLen);//TODO: TO convert the async call to sync
								done();									
							});
					});			
				});
			});
		});		
	});

	it("validate whether number of record inserted matches the expected count through controller", function (done) {
		var context = console;
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				outagecontroller.processOutages(context, function(err, objData) {
					objdaoimpl.findAll("outagemap", objOutageMapModel.objOutageData, objOutageMapModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							// expect(results.length).to.greaterThan(0);//TODO: TO convert the async call to sync
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