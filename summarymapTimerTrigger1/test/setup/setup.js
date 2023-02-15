var async = require('async');
var objdaoimpl = require('../../dao/mongodaoimpl.js');
var arrTransformerData = require('./input/transformerdata.js');
var arrHypersproutData = require('./input/hypersproutdata.js');
var arrCircuitData = require('./input/circuitdata.js');
var arrMeterData = require('./input/meterdata.js');
var objProcessTransactionData = require('./input/processtransactiondata.js');
var objProcessAlarmAndEventsData = require('./input/processalarmdata.js');
var objmysqldaoimpl = require('../../dao/mysqldaoimpl.js');
var objSummaryMapModel = require('../../model/sqltables/summarymapmodel.js');
var objLatestTransactionsModel = require('../../model/sqltables/latesttransactionmodel.js');
var objNetworkCoverageMapModel = require('../../model/sqltables/networkcoveragemodel.js');
var objAlarmAndEvents = require('../../model/sqltables/alarmseventstransformer.js');
var objAlarmAndEventsLatest = require('../../model/sqltables/alarmseventstransformerlatest.js');
var objTransformerTransModel = require('../../model/sqltables/transformertransactionmodel.js');
var objMeterTransModel = require('../../model/sqltables/metertransactionmodel.js');
var objOutageMapModel = require('../../model/sqltables/outagemap.js');
var objManagerialDataModel = require('../../model/sqltables/managerialdatamodel.js');

function init(done){
    cleanupData(function() {
        initData(done);
    })
};

function initData(done){
    var arrTransactionData = objProcessTransactionData.getTransactionData();
    var arrAlarmAndEventsData = objProcessAlarmAndEventsData.getTransactionData();    
    async.waterfall([
            function(innercallback) {
                insertData("DELTA_Transformer", arrTransformerData.data, innercallback);
            },
            function(objInput, innercallback) {
                insertData("DELTA_Hypersprouts", arrHypersproutData.data, innercallback);
            },  
            function(objInput, innercallback) {
                insertData("DELTA_Meters", arrMeterData.data, innercallback);
            },
            function(objInput, innercallback) {
                insertData("DELTA_Circuit", arrCircuitData.data, innercallback);
            }, 			
            function(objInput, innercallback) {
                insertData("DELTA_Transaction_Data", arrTransactionData, innercallback);
            },
            function(objInput, innercallback) {
                insertData("DELTA_AlarmsAndEvents", arrAlarmAndEventsData, innercallback);
            }               
        ], function(err, results) {
            if(err) {
                console.log(err);
            }
            objdaoimpl.closeConnection();
            done();
        }
    );
};

function cleanupInputData(done){
    var arrTransactionData = objProcessTransactionData.getTransactionData();
    deleteAllDocs(["DELTA_Transformer", "DELTA_Hypersprouts", 
            "DELTA_Meters", "DELTA_Circuit", "DELTA_Transaction_Data", "DELTA_AlarmsAndEvents"], done);
};

function cleanupOutputData(done){
    objmysqldaoimpl.truncateEntries("summarymap", objSummaryMapModel.objSummaryMap, objSummaryMapModel.objTableProps, 
        {}, 
        function(err, results) {
            objmysqldaoimpl.truncateEntries("latesttransactions", objLatestTransactionsModel.objLatestTrans, objLatestTransactionsModel.objTableProps, 
                {}, 
                function(err, results) {
				    objmysqldaoimpl.truncateEntries("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage, objNetworkCoverageMapModel.objTableProps, 
                    {}, 
                    function(err, results) {
                        objmysqldaoimpl.truncateEntries("alarmseventstransformer", objAlarmAndEvents.objTransformerevents, objAlarmAndEvents.objTableProps, 
                            {}, 
                            function(err, results) {
                                objmysqldaoimpl.truncateEntries("alarmseventstransformerlatest", objAlarmAndEventsLatest.objTransformerevents, objAlarmAndEventsLatest.objTableProps, 
                                    {}, 
                                    function(err, results) {
                                    objmysqldaoimpl.truncateEntries("transformertransactions", objTransformerTransModel.objTransformer, objTransformerTransModel.objTableProps, 
                                        {}, 
                                        function(err, results) {
                                            objmysqldaoimpl.truncateEntries("metertransactions", objMeterTransModel.objMeter, objMeterTransModel.objTableProps, 
                                                {}, 
                                                function(err, results) {
                                                    objmysqldaoimpl.truncateEntries("outagemap", objOutageMapModel.objOutageData, objOutageMapModel.objTableProps, 
                                                        {}, 
                                                        function(err, results) {
                                                            objmysqldaoimpl.truncateEntries("managerialdata", objManagerialDataModel.objTableColumns, objManagerialDataModel.objTableProps, 
                                                                {}, 
                                                                function(err, results) {
                                                                    done();									
                                                                });
                                                        });
                                                });
                                        });
                                    });								
                            });
                    });									
                });
        });	
};

function cleanupData(done) {
    cleanupInputData(function() {
        cleanupOutputData(done);
    });    
}

function insertData(strCollectionName, objInput, callback) {
    objdaoimpl.insertDoc(strCollectionName, objInput, function(err, r) {
        callback(err, r);
    })        
}

function deleteAllDocs(arrCollectionNames, callback) {
    var callbackIndex = 0;
    for(var i = 0; i < arrCollectionNames.length; i++) {
        objdaoimpl.deleteAllDocs(arrCollectionNames[i], function(err, r) {
            callbackIndex++;
            if(callbackIndex === arrCollectionNames.length) {
                objdaoimpl.closeConnection();
                callback(err, r);
            }
        });
    }        
}

module.exports = {
    initData : initData,
    cleanupData : cleanupData
}