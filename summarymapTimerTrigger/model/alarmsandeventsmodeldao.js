var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var moment = require("moment");
var objmysqldaoimpl = require('../dao/mysqldaoimpl.js');
var objManagerialDatadaoimpl = require('../model/managerialdatadaoimpl.js');
var objtransformereventsmodel = require('../model/sqltables/alarmseventstransformer.js');
var objtransformereventslatestmodel = require('../model/sqltables/alarmseventstransformerlatest.js');
var objjsFunctions = require('../util/jsfunctions.js');
var objConfig = require('../config.js');

var intnumofdays = 2;

var logger = console;
var countval;
var totalNoOfRecords;
var insertCallbackCount;
var objTimer;
var objTimerInterval;
var objLatestHSData = {};
var objLatestMeterData = {};
/**
 * @description - Code to update Alarms and events
 * @param context - console
 * @param {Respose to be returned} callback
 * @return - callback
 */
function updateAlarmsandEventsDataToRDBMS(context, callback) {
    logger = context;
    objManagerialDatadaoimpl.getManagerialData(function (managerialDataerr, data) {
        async.parallel({
            transformerdata: function (callback) {
                updateAllTransformerData(data, callback);
            }
        }, function (err, results) {
            callback(err, results);
        });
    });
}
/**
 * @description - Code to update Transformer Data
 * @param managerialData - managerial data
 * @param {Respose to be returned} callback
 * @return - callback
 */
function updateAllTransformerData(managerialData, callback) {
    objmysqldaoimpl.truncateEntries("transformerevents", objtransformereventsmodel.objTransformerevents,
        objtransformereventsmodel.objTableProps, {}, function (err) {
            if (err) {
                logger.log(err);
            }
            countval = 0;
            totalNoOfRecords = 0;
            insertCallbackCount = 0;
            objmysqldaoimpl.synctable("alarmseventstransformerlatest", objtransformereventslatestmodel.objTransformerevents,
                objtransformereventslatestmodel.objTableProps, function (err) {
                    if (err) {
                        logger.log(err);
                    }
                    var cc = false;
                    objdaoimpl.getCursorFromCollection("DELTA_AlarmsAndEvents", ['DBTimestamp'], [{ $gt: new Date(Date.now() - (intnumofdays * 24 * 60 * 60 * 1000)) }], function (err, arrTransformerTransData) {
                        if (arrTransformerTransData) {
                            arrTransformerTransData.stream()
                                .on('data', function (meterTranItem) {
                                    processAlarmTransformerData(meterTranItem, managerialData);
                                    interruptWhnDefinedCountReached(arrTransformerTransData);
                                    countval++;
                                })
                                .on('error', function (err) {
                                    if (!cc) {
                                        callback(err, null);
                                        cc = true;
                                    }
                                    logger.log('errr:', err);
                                })
                                .on('end', function () {
                                    logger.log('end:');
                                    checkAndCallCallbackAfterFinish(function () {
                                        if (!cc) {
                                            callback();
                                            cc = true;
                                        }
                                    });
                                });
                        }
                    });
                });
        });
}

/**
 * @description - Code to call function after time interval
 * @param {Respose to be returned} callback
 * @return - callback
 */
function checkAndCallCallbackAfterFinish(callback) {
    objTimerInterval = setInterval(function () {
        if (insertCallbackCount >= totalNoOfRecords) {
            insertCallbackCount = 0;
            totalNoOfRecords = 0;
            clearTimeout(objTimer);
            clearInterval(objTimerInterval);
            updateMeterAlarmLatestData();
            updateHypersproutAlarmLatestData();
            checkAndCallCallbackAfterLatestDataFinish(callback);
        }
    }, 1000);
}
/**
 * @description - Code to call function after  time interval
 * @param {Respose to be returned} callback
 * @return - callback
 */
function checkAndCallCallbackAfterLatestDataFinish(callback) {
    objTimerInterval = setInterval(function () {
        if (insertCallbackCount >= totalNoOfRecords) {
            clearTimeout(objTimer);
            clearInterval(objTimerInterval);
            callback(null, true);
        }
    }, 1000);
}
/**
 * @description - Code to update MEter latest alarms and events
 * @return - Nil
 */
function updateMeterAlarmLatestData() {
    for (var objMeterDataKey in objLatestMeterData) {
        if (objLatestMeterData.hasOwnProperty(objMeterDataKey)) {
            var objMeterDetails = objLatestMeterData[objMeterDataKey];
            for (var i = 0; i < objConfig.arrMeterAlarmKey.length; i++) {
                var objDataToInsert = {};
                updateCommonKeysForLatestData(objMeterDetails, objDataToInsert);
                objDataToInsert.Meter_DeviceID = objMeterDetails.Meter_DeviceID;
                objDataToInsert.Phase = objMeterDetails.Meter_Phase;
                objDataToInsert.Status = objMeterDetails.Meter_Status;
                objDataToInsert.AlarmsType = objConfig.arrMeterAlarmKey[i];
                objDataToInsert.AlarmsValue = objMeterDetails[objConfig.arrMeterAlarmKey[i]] ? 1 : 0;
                objDataToInsert.DBTimestamp = objMeterDetails.DBTimestamp;
                insertRecordToAlarmAndEventTablesLatest(objDataToInsert);
            }
        }
    }
}
/**
 * @description - Code to update Hypersprout latest alarms and events
 * @return - Nil
 */
function updateHypersproutAlarmLatestData() {
    for (var objHSDataKey in objLatestHSData) {
        if (objLatestHSData.hasOwnProperty(objHSDataKey)) {
            var objHSDetails = objLatestHSData[objHSDataKey];
            for (var i = 0; i < objConfig.arrHypersproutAlarmKey.length; i++) {
                var objDataToInsert = {};
                updateCommonKeysForLatestData(objHSDetails, objDataToInsert);
                objDataToInsert.Phase = objHSDetails.Phase;
                objDataToInsert.Status = objHSDetails.StatusTransformer;
                objDataToInsert.AlarmsType = objConfig.arrHypersproutAlarmKey[i];
                objDataToInsert.AlarmsValue = objHSDetails[objConfig.arrHypersproutAlarmKey[i]] ? 1 : 0;
                objDataToInsert.DBTimestamp = objHSDetails.DBTimestamp;
                insertRecordToAlarmAndEventTablesLatest(objDataToInsert);
            }
        }
    }
}
/**
 * @description - Code to update common keys used for  latest data
 * @param objMeterDetails - meter details
 * @param objDataToInsert - data to be inserted
 * @return - Nil
 */
function updateCommonKeysForLatestData(objMeterDetails, objDataToInsert) {
    objDataToInsert.CellID = objMeterDetails.CellID;
    objDataToInsert.MeterSerialNumber = objMeterDetails.MeterSerialNumber;
    objDataToInsert.CircuitID = objMeterDetails.CircuitID;
    objDataToInsert.HypersproutID = objMeterDetails.HypersproutID;
    objDataToInsert.TransformerID = objMeterDetails.TransformerID;
}

/**
 * @description - code to call function when certain count is reached
 * @param arrTransformerTransData - transformer transaction data
 * @return - Nil
 */
function interruptWhnDefinedCountReached(arrTransformerTransData) {
    if (countval % 200 === 0) {
        arrTransformerTransData.pause();
        objTimer = setTimeout(function () {
            arrTransformerTransData.resume();
        }, 500);
    }
}
/**
 * @description - code to process  alarms transformer data
 * @param meterTranItem - meter transaction item
 * @param managerialData - managerial data
 * @return - Nil
 */
function processAlarmTransformerData(meterTranItem, managerialData) {
    try {
        var objTranData = meterTranItem;
        var objToUpdate;
        if (objTranData.result && objTranData.result.meters) {
            for (var i = 0; i < objTranData.result.meters.length; i++) {
                var objMeterData = objTranData.result.meters[i];
                objToUpdate = updateValuesToInsert(objTranData, objMeterData);
                updateCircuitAndTrasformerData(managerialData, objToUpdate);
                objToUpdate.MeterSerialNumber = managerialData && managerialData.meterobj ? managerialData.meterobj[objToUpdate.Meter_DeviceID].MeterSerialNumber : null;
                objLatestMeterData[objToUpdate.Meter_DeviceID] = objToUpdate;
                insertRecordToAlarmAndEventTables(objToUpdate);
            }
            return;
        }
        if (objTranData.result) {
            objToUpdate = updateValuesToInsert(objTranData, null);
            updateCircuitAndTrasformerData(managerialData, objToUpdate);
            objToUpdate.MeterSerialNumber = 'HyperSprout Alarm';
            objLatestHSData[objToUpdate.CellID] = objToUpdate;
            insertRecordToAlarmAndEventTables(objToUpdate);
            return;
        }
    } catch (err) {
        logger.log(err);
    }
}
/**
 * @description - code to update value to be inserted
 * @param objTranData -  transaction item
 * @param objMeterData - meter data
 * @return - Nil
 */
function updateValuesToInsert(objTranData, objMeterData) {
    var objToUpdate = {};
    objjsFunctions.assignValuesFrmObject(objTranData.result, objToUpdate, '', [], true, false);
    objjsFunctions.assignValuesFrmObject(objTranData, objToUpdate, '', ["DBTimestamp"], false, false);
    delete objToUpdate.Transformer;
    delete objToUpdate.meters;
    if (objMeterData) {
        objjsFunctions.assignValuesFrmObject(objMeterData, objToUpdate, 'Meter_', [], true, false);
        objToUpdate.Meter_ReadTimestamp = objToUpdate.Meter_ReadTimestamp ? moment(objToUpdate.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
    }
    objjsFunctions.assignValuesFrmObject(objTranData.result.Transformer, objToUpdate, '', [], true, false);
    objToUpdate.ReadTimestamp = objToUpdate.ReadTimestamp ? moment(objToUpdate.ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
    objToUpdate.DBTimestamp = objToUpdate.DBTimestamp ? moment(objToUpdate.DBTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
    return objToUpdate;
}
/**
 * @description - code to update circuit and transformer data
 * @param managerialData -  managerial data
 * @param objToUpdate - data to be updated
 * @return - Nil
 */
function updateCircuitAndTrasformerData(managerialData, objToUpdate) {
    if (managerialData && managerialData.transformerobj && managerialData.transformerobj[objToUpdate.CellID]) {
        objToUpdate.CircuitID = managerialData.transformerobj[objToUpdate.CellID].CircuitID;
        objToUpdate.TransformerID = managerialData.transformerobj[objToUpdate.CellID].TransformerSerialNumber;
        objToUpdate.HypersproutID = managerialData.transformerobj[objToUpdate.CellID].HypersproutSerialNumber;
    }
}
/**
 * @description - code to insert alarms data to alarmsandevent table
 * @param objDataToInsert -  data to be inserted
 * @return - Nil
 */
function insertRecordToAlarmAndEventTables(objDataToInsert) {
    objmysqldaoimpl.insertData("alarmseventstransformer", objtransformereventsmodel.objTransformerevents,
        objtransformereventsmodel.objTableProps,
        objDataToInsert, insertDataCallback);
    totalNoOfRecords += 1;
}
/**
 * @description - code to insert latest alarms data to latestalarmsandevent table
 * @param objDataToInsert -  data to be inserted
 * @return - Nil
 */
function insertRecordToAlarmAndEventTablesLatest(objDataToInsert) {
    objmysqldaoimpl.insertData("alarmseventstransformerlatest", objtransformereventslatestmodel.objTransformerevents,
        objtransformereventslatestmodel.objTableProps,
        objDataToInsert, insertDataCallback);
    totalNoOfRecords += 1;
}
/**
 * @description - code to insert data
 * @param - error
 * @return - Nil
 */
function insertDataCallback(err) {
    if (err) {
        logger.log("Error", err);
    }
    insertCallbackCount++;
}

module.exports = {
    updateAlarmsandEventsDataToRDBMS: updateAlarmsandEventsDataToRDBMS
};