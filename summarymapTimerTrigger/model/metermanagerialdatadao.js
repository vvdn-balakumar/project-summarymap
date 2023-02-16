var objManagerialDatadaoimpl = require('../model/managerialdatadaoimpl.js');
var objManagerialDataModel = require('../model/sqltables/managerialdatamodel.js');
var objmysqldaoimpl = require('../dao/mysqldaoimpl.js');
var forEach = require('async-foreach').forEach;

var logger = console;
/**
 * @description - Code to update meter managerial data
 * @param context - console
 * @return callback
 */
function updateMeterManagerialDataToRDBMS(context, callback) {
    logger = context;
    objManagerialDatadaoimpl.getManagerialData(function (err, data) {
        processDataByMeter(data, callback);
    });
}
/**
 * @description - Code to process data by meter id
 * @param objManagerialData - managerial data
 * @return callback
 */
function processDataByMeter(objManagerialData, callback) {
    objmysqldaoimpl.truncateEntries("managerialdata", objManagerialDataModel.objTableColumns,
        objManagerialDataModel.objTableProps, {}, function (err) {
            if (err) {
                logger.log(err);
            }
            if (objManagerialData.meterobj) {
                var arrMeterKeys = Object.keys(objManagerialData.meterobj);
                var intSyncIndex = 0;
                forEach(arrMeterKeys, function (strMeterId, index, arrMeterObj) {
                    try {
                        var done = this.async();
                        var objMeterData = objManagerialData.meterobj[strMeterId];
                        var objTransformerData = objManagerialData.transformerobj[objMeterData.HypersproutID];
                        objTransformerData = objTransformerData ? objTransformerData : {};
                        var objDataCollection = {};
                        objDataCollection[index] = getDefinedObject(strMeterId, objMeterData.TransformerID, objMeterData, objTransformerData);
                        insertDataToManagerialData(objDataCollection[index], function (err, bUpdateStatus) {
                            if (err) {
                                logger.log(err, bUpdateStatus);
                            }
                            delete objDataCollection[intSyncIndex];
                            if (intSyncIndex === (arrMeterObj.length - 1)) {
                                return processDataByTransformer(objManagerialData, callback);
                            }
                            intSyncIndex++;
                        });
                        setTimeout(function () {
                            done();
                        }, 5);
                    } catch (err) {
                        logger.log(err);
                    }
                });
            }
        });
}
/**
 * @description - Code to process data by transformer id
 * @param objManagerialData - managerial data
 * @return callback
 */
function processDataByTransformer(objManagerialData, callback) {
    var arrTransformerKeys = objManagerialData.arrtransformeridstoprocess;

    if (arrTransformerKeys && arrTransformerKeys.length > 0) {
        var intSyncIndex = 0;
        forEach(arrTransformerKeys, function (strTransId, index, arrTransObj) {
            try {
                var done = this.async();
                var objMeterData = {};
                var objTransformerData = objManagerialData.transformerobj[strTransId];
                var objProcessedManagerialData = getDefinedObject(null, strTransId, objMeterData, objTransformerData);
                insertDataToManagerialData(objProcessedManagerialData, function (err, bUpdateStatus) {
                    if (err) {
                        logger.log(err, bUpdateStatus);
                    }
                    if (intSyncIndex === (arrTransObj.length - 1)) {
                        callback(null, true);
                    }
                    intSyncIndex++;
                });

                setTimeout(function () {
                    done();
                }, 5);
            } catch (err) {
                logger.log(err);
            }
        });
    } else {
        callback(null, true);
    }
}
/**
 * @description - Code to insert data to managerial table
 * @param objProcessedManagerialData - managerial data
 * @return callback
 */
function insertDataToManagerialData(objProcessedManagerialData, callback) {
    objmysqldaoimpl.insertData("managerialdata", objManagerialDataModel.objTableColumns,
        objManagerialDataModel.objTableProps,
        objProcessedManagerialData, function (err, objTransformerTransData) {
            callback(err, objTransformerTransData);
        });
}
/**
 * @description - Code to get object with proper data defined
 * @param strMeterId - meter id
 * @param strTransformerId - transformer id
 * @param objMeterData - meter data
 * @param objTransformerData - transformer data
 * @return callback
 */
function getDefinedObject(strMeterId, strTransformerId, objMeterData, objTransformerData) {
    var objToReturn = {};
    objToReturn.CircuitID = objTransformerData.CircuitID;
    objToReturn.TransformerID = strTransformerId;
    objToReturn.HypersproutID = objTransformerData.HypersproutID;
    objToReturn.TransformerSerialNumber = objTransformerData.TransformerSerialNumber;
    objToReturn.HypersproutSerialNumber = objTransformerData.HypersproutSerialNumber;
    objToReturn.MeterID = strMeterId;
    objToReturn.MeterSerialNumber = objMeterData.MeterSerialNumber;
    objToReturn.MeterStatus = objMeterData.Status;
    return objToReturn;
}

module.exports = {
    updateMeterManagerialDataToRDBMS: updateMeterManagerialDataToRDBMS
};