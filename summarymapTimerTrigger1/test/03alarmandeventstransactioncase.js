var expect = require("chai").expect;
var moment = require("moment");
var objMigrateAllAlarmsandEventsData = require('../controllers/migratealarmsandeventstordbms.js');
var objAlarmsandeventsModel = require('../model/alarmsandeventsmodeldao.js');
var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objSetup = require('../test/setup/setup.js');
var objAlarmAndEvents = require('../model/sqltables/alarmseventstransformer.js');
var objAlarmAndEventsLatest = require('../model/sqltables/alarmseventstransformerlatest.js');
var objConfig = require('../config.js');

describe("Alarms and Events data migration to RDBMS test cases", function () {
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
				objAlarmsandeventsModel.updateAlarmsandEventsDataToRDBMS(context, function(err, status) {
					objdaoimpl.findAll("alarmseventstransformer", objAlarmAndEvents.objTransformerevents, objAlarmAndEvents.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(576);
							objdaoimpl.findAll("alarmseventstransformerlatest", objAlarmAndEventsLatest.objTransformerevents, objAlarmAndEventsLatest.objTableProps, 
								null,
								null, 
								function(err, results) {
										expect(results.length).to.equal(144);
										done();									
								});
						});
				})
			});
		});		
	});

	it("validate whether number of record inserted matches the expected count through controller", function (done) {
		var context = console;
		objSetup.cleanupData(function() {
			objSetup.initData(function() {		
				objMigrateAllAlarmsandEventsData.migrateAlarmsandEventsToRdbms(context, function(err, obj) {
					objdaoimpl.findAll("alarmseventstransformer", objAlarmAndEvents.objTransformerevents, objAlarmAndEvents.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(576);
							objdaoimpl.findAll("alarmseventstransformerlatest", objAlarmAndEventsLatest.objTransformerevents, objAlarmAndEventsLatest.objTableProps, 
								null,
								null, 
								function(err, results) {
										expect(results.length).to.equal(144);
										done();									
								});
						});
				})
			});
		});
	});	

	after(function(done){
	    objSetup.cleanupData(done);
	})
});