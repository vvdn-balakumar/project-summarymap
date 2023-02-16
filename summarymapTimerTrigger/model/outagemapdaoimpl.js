var objdaoimpl = require('../dao/mysqldaoimpl.js');
var moment = require("moment");
var objOutageMapModel = require('../model/sqltables/outagemap.js');
var forEach = require('async-foreach').forEach;
var logger = console;

var DEFAULT_START_DATE = new Date(1970, 0, 1, 0, 0, 0);
/**
* @description - Code to update outage map
* @param objInputData - object data
* @param context - console
* @return callback
*/
function updateOutageMapModel(objInputData, callback, context) {
    logger = context ? context : logger;
    try {
        objdaoimpl.truncateEntries("outagemap", objOutageMapModel.objOutageData,
            objOutageMapModel.objTableProps,
            { Status: 'No Outage' },
            function (err) {
                if (err) {
                    logger.log(err);
                }
                var objManagerialData = objInputData.managerialData;
                var objOutageData = objInputData.outagedata;
                var objTransformersTransactionData = objInputData.transformertransationdata;
                if (objManagerialData.meterobj) {
                    var arrMeterKeys = Object.keys(objManagerialData.meterobj);
                    forEach(arrMeterKeys, function (strMeterId, index, arrMeterObj) {
                        try {
                            var done = this.async();
                            var objMeterData = objManagerialData.meterobj[strMeterId];
                            processoutagedata(objManagerialData, objOutageData, objTransformersTransactionData, objMeterData);
                            if (index === (arrMeterObj.length - 1)) {
                                processTransformerBasedData(objManagerialData, objOutageData, objTransformersTransactionData, callback);
                            }
                            setTimeout(function () {
                                done();
                            }, 5);
                        } catch (err) {
                            logger.log(err);
                        }
                    });
                }
            });
    } catch (err) {
        logger.log(err);
        callback(err, null);
    }
}
/**
* @description - Code to process transformer data
* @param objManagerialData - managerial data
* @param objOutageData - outage data
* @param objTransformersTransactionData - transformer transaction data
* @return callback
*/
function processTransformerBasedData(objManagerialData, objOutageData, objTransformersTransactionData, callback) {
    var arrTransformerKeys = objManagerialData.arrtransformeridstoprocess;

    if (arrTransformerKeys && arrTransformerKeys.length > 0) {
        forEach(arrTransformerKeys, function (strTransId, index, arrTransObj) {
            try {
                var done = this.async();
                var objMeterData = {};
                objMeterData.TransformerID = strTransId;
                processoutagedata(objManagerialData, objOutageData, objTransformersTransactionData, objMeterData);

                if (index === (arrTransObj.length - 1)) {
                    callback(null, 'success');
                }
                setTimeout(function () {
                    done();
                }, 5);
            } catch (err) {
                logger.log(err);
            }
        });
    } else {
        callback(null, 'success');
    }

}
/**
* @description - Code to process outage data
* @param objManagerialData - managerial data
* @param objOutageData - outage data
* @param objTransformersTransactionData - transformer transaction data
* @param objMeterData - meter data
* @return Nil
*/
function processoutagedata(objManagerialData, objOutageData, objTransformersTransactionData, objMeterData) {
    if (objMeterData.TransformerID) {
        var objTransformerData = objManagerialData.transformerobj[objMeterData.TransformerID];
        objTransformerData = objTransformerData ? objTransformerData : {};
        var objCircuitData = objManagerialData.circuitobj[objTransformerData.CircuitID];
        objCircuitData = objCircuitData ? objCircuitData : {};
        var objTransformerTransactionData = objTransformersTransactionData[objMeterData.TransformerID];
        objTransformerTransactionData = objTransformerTransactionData ? objTransformerTransactionData : {};

        var objTransformerOutages = objOutageData[objMeterData.TransformerID];
        var objectToInsert;
        if (objTransformerOutages && objTransformerOutages.length > 0) {

            for (var i = 0; i < objTransformerOutages.length; i++) {
                objectToInsert = getUpdateObject(objCircuitData, objTransformerData, objMeterData, objTransformerTransactionData);
                objectToInsert["Start Time"] = objTransformerOutages[i].startTime;
                objectToInsert["Unique Start Time"] = objectToInsert["Start Time"] ? objectToInsert["Start Time"] : objectToInsert["Unique Start Time"];
                objectToInsert["End Time"] = objTransformerOutages[i].endTime;
                objectToInsert.Duration = objTransformerOutages[i].duration;
                objectToInsert.Status = objTransformerOutages[i].endTime ? objTransformerOutages[i].status : 'Ongoing';

                objdaoimpl.insertData("outagemap", objOutageMapModel.objOutageData,
                    objOutageMapModel.objTableProps,
                    objectToInsert, insertOutageResponse);
                objdaoimpl.findAll("outagemap", objOutageMapModel.objOutageData, objOutageMapModel.objTableProps,
                    null,
                    {
                        'Transformer ID': objectToInsert["Transformer ID"],
                        'Meter ID': objectToInsert["Meter ID"]
                    },
                    outagefindAllResponse);
            }
        } else {
            objectToInsert = getUpdateObject(objCircuitData, objTransformerData, objMeterData, objTransformerTransactionData);
            objdaoimpl.findAll("outagemap", objOutageMapModel.objOutageData, objOutageMapModel.objTableProps, null,
                {
                    'Transformer ID': objectToInsert["Transformer ID"],
                    'Meter ID': objectToInsert["Meter ID"]
                },
                function (err, results) {
                    if (results.length === 0 || (results.length === 1 && results[0].Status === 'No Outage')) {
                        objdaoimpl.insertData("outagemap", objOutageMapModel.objOutageData,
                            objOutageMapModel.objTableProps,
                            objectToInsert, insertOutageResponse);
                    } else {
                        updateTransValuesToAllRows(results, objectToInsert);
                    }
                });
        }
    }

    /**
    * @description - Code to get reponse after insert
    * @param err
    * @return Nil
    */
    function insertOutageResponse(err) {
        if (err) {
            logger.log("outage data Error", err);
        }
    }
    /**
       * @description - Code to get all reponse after insert
       * @param err
       * @param results - results
       * @return Nil
       */
    function outagefindAllResponse(err, results) {
        updateTransValuesToAllRows(results, objectToInsert);
    }
}
/**
    * @description - Code to update all transaction value to row
    * @param objectToInsert - data
    * @param results - results
    * @return Nil
    */

function updateTransValuesToAllRows(results, objectToInsert) {
    for (var i = 0; i < results.length; i++) {
        var objPopulatedResult = updateTransactionData(objectToInsert);
        objdaoimpl.updateData("outagemap", objOutageMapModel.objOutageData,
            objOutageMapModel.objTableProps,
            objPopulatedResult,
            { 'Transformer ID': objectToInsert["Transformer ID"] },
            outageUpdateDataResponse);
    }
    function outageUpdateDataResponse(err) {
        if (err) {
            logger.log("Outage data update Error", err);
        }
    }
}
/**
    * @description - Code to create object with defined data
    * @param objCircuitData - circuit data
    * @param objTransformerData - transformer data
    * @param objMeterData - meter data
    * @param objTransformerTransactionData - transformer transaction data
    * @return Nil
    */
function getUpdateObject(objCircuitData, objTransformerData, objMeterData, objTransformerTransactionData) {
    var objectToInsert = {};
    objectToInsert["Start Time"] = null;
    objectToInsert["Unique Start Time"] = DEFAULT_START_DATE;
    objectToInsert["End Time"] = null;
    objectToInsert.Duration = '';
    objectToInsert.Status = 'No Outage';
    objectToInsert["Circuit ID"] = objCircuitData.CircuitID;
    objectToInsert["Circuit Latitude"] = objCircuitData.CircuitLatitude ? parseFloat(objCircuitData.CircuitLatitude).toFixed(7) : 0;
    objectToInsert["Circuit Longitude"] = objCircuitData.CircuitLongitude ? parseFloat(objCircuitData.CircuitLongitude).toFixed(7) : 0;
    objectToInsert["Transformer ID"] = objTransformerData.TransformerSerialNumber;
    objectToInsert["Hypersprout ID"] = objTransformerData.HypersproutSerialNumber;
    objectToInsert["Transformer Latitude"] = objTransformerData.TransformerLatitude ? parseFloat(objTransformerData.TransformerLatitude).toFixed(7) : 0;
    objectToInsert["Transformer Longitude"] = objTransformerData.TransformerLongitude ? parseFloat(objTransformerData.TransformerLongitude).toFixed(7) : 0;
    objectToInsert["Meter ID"] = objMeterData.MeterSerialNumber ? objMeterData.MeterSerialNumber : 0;
    objectToInsert["Meter Latitude"] = objMeterData.MeterLatitude ? parseFloat(objMeterData.MeterLatitude).toFixed(7) : 500.0000000;
    objectToInsert["Meter Longitude"] = objMeterData.MeterLongitude ? parseFloat(objMeterData.MeterLongitude).toFixed(7) : 500.0000000;
    objectToInsert["Circuit Power Flow Out"] = objCircuitData.ActiveReceivedCumulativeRate_Total;
    objectToInsert["Transformer Power Flow Out"] = objTransformerTransactionData.ActiveReceivedCumulativeRate_Total;
    objectToInsert["Top Oil Temperature"] = objTransformerTransactionData.TopTemperature ? objTransformerTransactionData.TopTemperature.toFixed(7) : null;
    objectToInsert["Bottom Oil Temperature"] = objTransformerTransactionData.BottomTemperature ? objTransformerTransactionData.BottomTemperature.toFixed(7) : null;
    objectToInsert["Total KVA"] = objTransformerTransactionData.Apparent_m_Total;
    return objectToInsert;
}

function updateTransactionData(objInput) {
    var objOutput = {};
    objOutput["Circuit Power Flow Out"] = objInput["Circuit Power Flow Out"];
    objOutput["Transformer Power Flow Out"] = objInput["Transformer Power Flow Out"];
    objOutput["Top Oil Temperature"] = objInput["Top Oil Temperature"];
    objOutput["Bottom Oil Temperature"] = objInput["Bottom Oil Temperature"];
    objOutput["Total KVA"] = objInput["Total KVA"];
    return objOutput;
}

module.exports = {
    updateOutageMapModel: updateOutageMapModel
};