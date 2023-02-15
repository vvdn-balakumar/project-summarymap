var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var moment = require("moment");
var objmysqldaoimpl = require('../dao/mysqldaoimpl.js');
var objTransformTransModel = require('../model/sqltables/transformertransactionmodel.js');
var objMeterTransModel = require('../model/sqltables/metertransactionmodel.js');
var objCalculations = require('../util/calculations.js');
var objjsFunctions = require('../util/jsfunctions.js');
var objConfig = require('../config.js');

var intnumofdays = objConfig.numberofDaysTransMeterTransaction;

var logger = console;
var intTransIndexCount = 0;
var intMeterIndexCount = 0;
/**
    * @description - Code to update transformer transaction data 
    * @param context - console
    * @return callback
    */
function updateTransformerTransactionDataToRDBMS(context, callback) {
    logger = context;
    intnumofdays = objConfig.numberofDaysTransMeterTransaction;
    async.parallel({
        transformerdata: function (callback) {
            updateAllTransformerData(callback);
        }
    }, function (err, results) {
        objdaoimpl.closeConnection();
        callback(err, results);
    });
}
/**
    * @description - Code to update meter transaction data 
    * @param context - console
    * @return callback
    */
function updateMeterTransactionDataToRDBMS(context, callback) {
    logger = context;
    intnumofdays = objConfig.numberofDaysTransMeterTransaction;
    async.parallel({
        meterdata: function (callback) {
            updateAllMeterData(callback);
        }
    }, function (err, results) {
        objdaoimpl.closeConnection();
        callback(err, results);
    });
}
/**
    * @description - Code to update all transformer data 
    * @param Nil
    * @return callback
    */
function updateAllTransformerData(callback) {
    objmysqldaoimpl.synctable("objTransformer", objTransformTransModel.objTransformer,
        objTransformTransModel.objTableProps, function (err) {
            if (err) {
                logger.log(err);
            }
            intTransIndexCount = 0;
            var cc = false;
            objdaoimpl.getCursorFromCollectionSorted("DELTA_Transaction_Data", ['DBTimestamp'], [{ $gt: new Date(Date.now() - (intnumofdays * 24 * 60 * 60 * 1000)) }], null, { 'result.meters': 0 }, function (err, arrTransformerTransData) {
                if (arrTransformerTransData) {
                    arrTransformerTransData.stream()
                        .on('data', function (meterTranItem) {
                            try {
                                var objTranData = meterTranItem;
                                var arrFilter = [];
                                var strCellIdVal;
                                if (objTranData.result) {
                                    strCellIdVal = objTranData.result.CellID;
                                    if (objConfig.cellIdToSkip.indexOf(strCellIdVal) === -1) {
                                        objjsFunctions.assignValuesFrmObject(objTranData.result.Transformer, objTranData.result, '', arrFilter, true, false);
                                    } else {
                                        objjsFunctions.assignValuesFrmObject(objTranData.result.Transformer, objTranData.result, '', arrFilter, true, true);
                                    }
                                    delete objTranData.result.Transformer;
                                    delete objTranData.result.meters;
                                    objTranData.result.DBTimestamp = objTranData.DBTimestamp;
                                    objTranData.result.uniqueIdVal = objTranData._id.toString();
                                }
                                intTransIndexCount++;
                                objmysqldaoimpl.insertData("objTransformer", objTransformTransModel.objTransformer,
                                    objTransformTransModel.objTableProps,
                                    objTranData.result, transformerTransactionInsertResponse);
                            } catch (err) {
                                logger.log(err);
                            }
                        })
                        .on('error', function (err) {
                            if (!cc) {
                                callback(err, null);
                                cc = true;
                            }
                            logger.log('errr:', err);
                        })
                        .on('end', function () {
                            arrTransformerTransData.close();
                            logger.log('end:');
                            checkAndRedirect(function () {
                                if (!cc) {
                                    callback();
                                    cc = true;
                                }
                            });
                        });
                }
            });
        });
    /**
        * @description - Code to update transformer transaction data  
        * @param Nil
        * @return callback
        */
    function transformerTransactionInsertResponse(err) {
        if (err) {
            logger.log("Error", err);
        }
        intTransIndexCount--;
        checkAndRedirect(callback);
    }

}
/**
      * @description - Code to check index count and redirect
      * @param Nil
      * @return callback
      */
function checkAndRedirect(callback) {
    if (intTransIndexCount === 0) {
        intTransIndexCount = -1;
        callback(null, true);
    }
}
/**
      * @description - Code to cupdate all meter data
      * @param Nil
      * @return callback
      */
function updateAllMeterData(callback) {
    objmysqldaoimpl.synctable("metertransactions", objMeterTransModel.objMeter,
        objMeterTransModel.objTableProps, function (err) {
            if (err) {
                logger.log(err);
            }
            var cc = false;
            intMeterIndexCount = 0;
            objdaoimpl.getCursorFromCollectionSorted("DELTA_Transaction_Data", ['DBTimestamp'], [{ $gt: new Date(Date.now() - (intnumofdays * 24 * 60 * 60 * 1000)) }], null, { 'result.Transformer': 0 }, function (err, arrMeterTransData) {
                if (arrMeterTransData) {
                    arrMeterTransData.stream()
                        .on('data', function (meterTranItem) {
                            try {
                                var objMeterData = meterTranItem;
                                var arrMeterResultData = [];
                                if (objMeterData.result) {
                                    arrMeterResultData = objMeterData.result.meters;
                                }
                                var arrFilter = [];

                                if (arrMeterResultData && Array.isArray(arrMeterResultData)) {
                                    for (var i = 0; i < arrMeterResultData.length; i++) {
                                        var objDataToInsert = {};
                                        objDataToInsert.result = {};
                                        objjsFunctions.assignValuesFrmObject(objMeterData.result, objDataToInsert.result);
                                        var meterResultTranItem = arrMeterResultData[i];
                                        objjsFunctions.assignValuesFrmObject(meterResultTranItem, objDataToInsert.result, '', arrFilter, true, false);
                                        objDataToInsert.result.DBTimestamp = objMeterData.DBTimestamp;
                                        objDataToInsert.result.uniqueIdVal = objMeterData._id.toString();

                                        if (objDataToInsert.result) {
                                            delete objDataToInsert.result.Transformer;
                                            delete objDataToInsert.result.meters;

                                            objDataToInsert.result.Line1InstVoltage = objDataToInsert.result.Line1InstVoltage ? objDataToInsert.result.Line1InstVoltage.toFixed(7) : 0;
                                            objDataToInsert.result.Line1InstCurrent = objDataToInsert.result.Line1InstCurrent ? objDataToInsert.result.Line1InstCurrent.toFixed(7) : 0;
                                            objDataToInsert.result.Line1Frequency = objDataToInsert.result.Line1Frequency ? objDataToInsert.result.Line1Frequency.toFixed(7) : 0;
                                            objDataToInsert.result.Line1PowerFactor = objDataToInsert.result.Line1PowerFactor ? objDataToInsert.result.Line1PowerFactor.toFixed(7) : 0;
                                            objDataToInsert.result.ActiveReceivedCumulativeRate1 = objDataToInsert.result.ActiveReceivedCumulativeRate1 ? objDataToInsert.result.ActiveReceivedCumulativeRate1.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveReceivedCumulativeRate2 = objDataToInsert.result.ActiveReceivedCumulativeRate2 ? objDataToInsert.result.ActiveReceivedCumulativeRate2.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveReceivedCumulativeRate3 = objDataToInsert.result.ActiveReceivedCumulativeRate3 ? objDataToInsert.result.ActiveReceivedCumulativeRate3.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveReceivedCumulativeRate4 = objDataToInsert.result.ActiveReceivedCumulativeRate4 ? objDataToInsert.result.ActiveReceivedCumulativeRate4.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveReceivedCumulativeRate_Total = objDataToInsert.result.ActiveReceivedCumulativeRate_Total ? objDataToInsert.result.ActiveReceivedCumulativeRate_Total.toFixed(6) : 0;

                                            objDataToInsert.result.ActiveDeliveredCumulativeRate1 = objDataToInsert.result.ActiveDeliveredCumulativeRate1 ? objDataToInsert.result.ActiveDeliveredCumulativeRate1.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveDeliveredCumulativeRate2 = objDataToInsert.result.ActiveDeliveredCumulativeRate2 ? objDataToInsert.result.ActiveDeliveredCumulativeRate2.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveDeliveredCumulativeRate3 = objDataToInsert.result.ActiveDeliveredCumulativeRate3 ? objDataToInsert.result.ActiveDeliveredCumulativeRate3.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveDeliveredCumulativeRate4 = objDataToInsert.result.ActiveDeliveredCumulativeRate4 ? objDataToInsert.result.ActiveDeliveredCumulativeRate4.toFixed(6) : 0;
                                            objDataToInsert.result.ActiveDeliveredCumulativeRate_Total = objDataToInsert.result.ActiveDeliveredCumulativeRate_Total ? objDataToInsert.result.ActiveDeliveredCumulativeRate_Total.toFixed(6) : 0;
                                            objDataToInsert.result.Apparent_m_Total = objDataToInsert.result.Apparent_m_Total ? objDataToInsert.result.Apparent_m_Total.toFixed(3) : 0;

                                            objDataToInsert.result.Line0InstCurrent = objDataToInsert.result.Line0InstCurrent ? objDataToInsert.result.Line0InstCurrent.toFixed(7) : 0;
                                            objDataToInsert.result.Line2InstVoltage = objDataToInsert.result.Line2InstVoltage ? objDataToInsert.result.Line2InstVoltage.toFixed(7) : 0;
                                            objDataToInsert.result.Line2InstCurrent = objDataToInsert.result.Line2InstCurrent ? objDataToInsert.result.Line2InstCurrent.toFixed(7) : 0;
                                            objDataToInsert.result.Line2Frequency = objDataToInsert.result.Line2Frequency ? objDataToInsert.result.Line2Frequency.toFixed(7) : 0;
                                            objDataToInsert.result.Line2PowerFactor = objDataToInsert.result.Line2PowerFactor ? objDataToInsert.result.Line2PowerFactor.toFixed(7) : 0;
                                            objDataToInsert.result.Line3InstVoltage = objDataToInsert.result.Line3InstVoltage ? objDataToInsert.result.Line3InstVoltage.toFixed(7) : 0;
                                            objDataToInsert.result.Line3InstCurrent = objDataToInsert.result.Line3InstCurrent ? objDataToInsert.result.Line3InstCurrent.toFixed(7) : 0;
                                            objDataToInsert.result.Line3Frequency = objDataToInsert.result.Line3Frequency ? objDataToInsert.result.Line3Frequency.toFixed(7) : 0;
                                            objDataToInsert.result.Line3PowerFactor = objDataToInsert.result.Line3PowerFactor ? objDataToInsert.result.Line3PowerFactor.toFixed(7) : 0;

                                        }
                                        intMeterIndexCount++;
                                        objmysqldaoimpl.insertData("metertransactions", objMeterTransModel.objMeter,
                                            objMeterTransModel.objTableProps,
                                            objDataToInsert.result, meterTransactionInsertResponse);
                                    }
                                }
                            } catch (err) {
                                logger.log(err);
                            }
                        })
                        .on('error', function (err) {
                            if (!cc) {
                                callback(err, null);
                                cc = true;
                            }
                        })
                        .on('end', function () {
                            arrMeterTransData.close();
                            logger.log('end:');
                            checkAndRedirectMeter(function () {
                                if (!cc) {
                                    callback();
                                    cc = true;
                                }
                            });
                        });
                }

            });
        });
    /**
          * @description - Code to check meter transaction insert response
          * @param err
          * @return callback
          */
    function meterTransactionInsertResponse(err) {
        if (err) {
            logger.log("Error", err);
        }
        intMeterIndexCount--;
        checkAndRedirectMeter(callback);
    }
}
/**
        * @description - Code to check index count and redirect
        * @param Nil
        * @return callback
        */
function checkAndRedirectMeter(callback) {
    if (intMeterIndexCount === 0) {
        intMeterIndexCount = -1;
        callback(null, true);
    }
}

module.exports = {
    updateTransformerTransactionDataToRDBMS: updateTransformerTransactionDataToRDBMS,
    updateMeterTransactionDataToRDBMS: updateMeterTransactionDataToRDBMS
};