var expect = require("chai").expect;
var moment = require("moment");

var objSummaryMap = require('../controllers/summarymapcontroller.js');
var objInputModel = require('../model/managesummarymapinput.js');
var objOutputModel = require('../model/managesummarymapoutput.js');
var objSetup = require('../test/setup/setup.js');
var objConfig = require('../config.js');
var objProcessTransaction = require('../test/setup/input/processtransactiondata.js');
var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objSummaryMapModel = require('../model/sqltables/summarymapmodel.js');
var objLatestTransactionsModel = require('../model/sqltables/latesttransactionmodel.js');

describe("Summary map data test cases", function () {

	before(function(done) {
		objConfig.numberofDaysSM = 2;
		objSetup.cleanupData(function() {
			objSetup.initData(done);
		});
	});

	this.timeout(objConfig.testcasetimeout);
	it("validate aggregation of input data", function (done) {
		var objDate = objProcessTransaction.objDate;
		objDate.setUTCMinutes(0, 0 ,0);
		var context = console;
		objInputModel.getAllSummaryMapRelatedDetails(context, function(err, data) {
			expect(data).not.to.equal(null);			
			expect(Object.keys(data.meterdata)).to.deep.equal([ '1', '2']);
			expect(Object.keys(data.transformerdata)).to.deep.equal([ '1', '2' ]);	
			expect(data.transformerdata[1].managerialdata.Transformer_CellID).to.equal("1");
			expect(data.transformerdata[2].managerialdata.Transformer_CellID).to.equal("2");
			for(var meterData in data.meterdata) {
				var arrMeterKeys = JSON.parse(JSON.stringify(objProcessTransaction.objTransMeterMap[meterData]));
				arrMeterKeys.push("TransformerData")
				expect(data.meterdata[meterData]).to.have.all.keys(arrMeterKeys);
				
				for(var i = 0; i < arrMeterKeys.length; i++) {
					var strMeterKey = arrMeterKeys[i];
					var objData = strMeterKey === 'TransformerData' ? data.meterdata[meterData][strMeterKey]
								: data.meterdata[meterData][strMeterKey]['TranData'] ;
					var objCurrentDate = new Date();
					objCurrentDate.setTime(objDate.getTime());
					var objStartDate = new Date();
					objStartDate.setTime(objDate.getTime());
					objStartDate.setUTCDate(objStartDate.getDate() - objProcessTransaction.intNumberOfDays);

					if(strMeterKey !== 'TransformerData') {
						var objManagerialData = data.meterdata[meterData][strMeterKey]['managerialdata'];
						expect(objManagerialData.Meter_DeviceID).to.equal(strMeterKey);
					}					
					var index = 0;
					while (objStartDate < objCurrentDate) {
						var objStartDateMoment = moment.utc(objStartDate, 'DD-MM-YYYY HH:mm:ss');                     
						var strKeyGenerated = objStartDateMoment.format('YYYY-MM-DD_HH');
						expect(objData[strKeyGenerated]).not.to.equal(null);
						validateTransactionData(index, objData, strMeterKey, strKeyGenerated);
						objStartDate.setMinutes(objStartDate.getMinutes() + objProcessTransaction.intIntervalMins);        
						index++;      
					}
				}				
			}
			done();
		});
	});

	it("validate aggregation of output data", function (done) {
		var objDate = objProcessTransaction.objDate;
		objDate.setUTCMinutes(0, 0 ,0);
		var context = console;
		objInputModel.getAllSummaryMapRelatedDetails(context, function(err, data) {
			expect(data).not.to.equal(null);			
            objOutputModel.postAllSummaryMapRelatedDetails(data, function(err, data) {
				expect(err).to.equal(null);
				objdaoimpl.findAll("summarymap", objSummaryMapModel.objSummaryMap, objSummaryMapModel.objTableProps, 
					null,
					null, 
					function(err, results) {
						expect(results.length).to.equal(objConfig.numberofDaysSM * 98);
						objdaoimpl.findAll("latesttransactions", objLatestTransactionsModel.objLatestTrans, objLatestTransactionsModel.objTableProps, 
							null,
							null, 
							function(err, results) {
								expect(results.length).to.equal(4);
									done();									
							});
					});				
			});
		});
	});

	it("validate aggregation of input and output of data for no record", function (done) {
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				var objDate = objProcessTransaction.objDate;
				objDate.setUTCMinutes(0, 0 ,0);
				var context = console;
				var actualNumberOfDay = objConfig.numberofDaysSM;
				objConfig.numberofDaysSM = 0;
				objInputModel.getAllSummaryMapRelatedDetails(context, function(err, data) {
					expect(data).not.to.equal(null);			
					objOutputModel.postAllSummaryMapRelatedDetails(data, function(err, data) {
						expect(err).to.equal(null);
						objdaoimpl.findAll("summarymap", objSummaryMapModel.objSummaryMap, objSummaryMapModel.objTableProps, 
							null,
							null, 
							function(err, results) {
								expect(results.length).to.equal(4);
								objdaoimpl.findAll("latesttransactions", objLatestTransactionsModel.objLatestTrans, objLatestTransactionsModel.objTableProps, 
									null,
									null, 
									function(err, results) {
										expect(results.length).to.equal(0);
										objConfig.numberofDaysSM = actualNumberOfDay;
										done();									
									});
							});				
					});
				});
			});
		});
	});	

	it("validate aggregation of input and output of data for no record and meter data", function (done) {
		objSetup.cleanupData(function() {
			var objDate = objProcessTransaction.objDate;
			objDate.setUTCMinutes(0, 0 ,0);
			var context = console;
			var actualNumberOfDay = objConfig.numberofDaysSM;
			objConfig.numberofDaysSM = 0;
			objInputModel.getAllSummaryMapRelatedDetails(context, function(err, data) {
				expect(data).not.to.equal(null);			
				objOutputModel.postAllSummaryMapRelatedDetails(data, function(err, data) {
					expect(err).to.equal(null);
					objdaoimpl.findAll("summarymap", objSummaryMapModel.objSummaryMap, objSummaryMapModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(0);
							objdaoimpl.findAll("latesttransactions", objLatestTransactionsModel.objLatestTrans, objLatestTransactionsModel.objTableProps, 
								null,
								null, 
								function(err, results) {
									expect(results.length).to.equal(0);
									objConfig.numberofDaysSM = actualNumberOfDay;
									done();									
								});
						});				
				});
			});
		});
	});		

	it("validate with wrong mongodb host", function (done) {
		objSetup.cleanupData(function() {
			objSetup.initData(function() {
				var oldURL = objConfig.inputdatasource.url; 
				objConfig.inputdatasource.url = 'mongodb://' + "wronglocalhost" + ':' + objConfig.inputdatasource.port + '/' +  
												objConfig.inputdatasource.database + '?authSource=' + objConfig.inputdatasource.database;
				var context = console;		
				objInputModel.getAllSummaryMapRelatedDetails(context, function(err, data) {
					expect(err).not.to.equal(null);
					expect(data).to.equal(null);
					objConfig.inputdatasource.url = oldURL;
					done();	
				});
			});
		});		
	});

	it("validate data through summarymap controller", function (done) {
		var objDate = objProcessTransaction.objDate;
		objDate.setUTCMinutes(0, 0 ,0);
		var context = console;
		objSummaryMap.getSummaryMap(console, function(err, obj) {
			objdaoimpl.findAll("summarymap", objSummaryMapModel.objSummaryMap, objSummaryMapModel.objTableProps, 
				null,
				null, 
				function(err, results) {
					expect(results.length).to.equal(objConfig.numberofDaysSM * 98);
					objdaoimpl.findAll("latesttransactions", objLatestTransactionsModel.objLatestTrans, objLatestTransactionsModel.objTableProps, 
						null,
						null, 
						function(err, results) {
							expect(results.length).to.equal(4);
							done();									
						});
				});		
		});   

	});						

	function validateTransactionData(index, objData, strMeterKey, strKeyGenerated) {
		var diffValToCheck = index > 3 ? 4 : 3;	
		var cumulativeValToCheck = index > 3 ? 8 : 6;
		var nontechlossValToCheck = index > 3 ? -1.6 : -1.2;		
		if(strMeterKey === 'TransformerData') {
			validateObjectWithValue(objData[strKeyGenerated], objConfig.transfomer_sm_arrLastValKeys, 4);
			validateObjectWithValue(objData[strKeyGenerated], objConfig.transfomer_sm_arrAverageKeys, ((1+2+3+4)/ 4));
			validateObjectWithValue(objData[strKeyGenerated], objConfig.transfomer_sm_arrDifffernceSumKeys,diffValToCheck);
			validateObjectWithValue(objData[strKeyGenerated], objConfig.transfomer_sm_arrSumKeys,(1+2+3+4));							
			expect(objData[strKeyGenerated].AllMeter_ActiveReceivedCumulativeRate_Total).to.equal(cumulativeValToCheck);
			expect(objData[strKeyGenerated].Non_Technical_Loss).to.equal(nontechlossValToCheck);							  
		} else {
			validateObjectWithValue(objData[strKeyGenerated], objConfig.meter_sm_arrLastValKeys, 4);
			validateObjectWithValue(objData[strKeyGenerated], objConfig.meter_sm_arrAverageKeys, ((1+2+3+4)/ 4));
			validateObjectWithValue(objData[strKeyGenerated], objConfig.meter_sm_arrDifffernceSumKeys, diffValToCheck);
			validateObjectWithValue(objData[strKeyGenerated], objConfig.meter_sm_arrSumKeys, (1+2+3+4));																					  
		}		
	}

	function validateObjectWithValue(objData, arrInputKeys, valToCheck) {
		for(var i = 0; i < arrInputKeys.length; i++) {
			if(arrInputKeys[i] === 'Meter_NetworkResponseRate') {
				expect(objData[arrInputKeys[i]]).to.equal(1);				
				continue;
			}
			expect(objData[arrInputKeys[i]]).to.equal(valToCheck);
		}
	}	

	after(function(done){
		objSetup.cleanupData(done);
	})
});