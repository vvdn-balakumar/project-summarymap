var objdaoimpl = require('../dao/mongodaoimpl.js');
var objjsFunctions = require('../util/jsfunctions.js');
var objCalculations = require('../util/calculations.js');
var objManagerialDatadaoimpl = require('../model/managerialdatadaoimpl.js');
var async = require('async');
var moment = require("moment");
var forEach = require('async-foreach').forEach;

var intnumofdays = 2;
var objOutageData = {};
var objTransformerTransactionData = {};
var logger = console;
/**
 * @description - Code to process outage data
 * @param context - console
 * @param {Respose to be returned} callback
 * @return - callback
 */
function processOutageDetails(context, callback) {
    logger = context;
    async.parallel({
        outagedata: function (callback) {
            processOutageDetailsFromAlarmEvents(callback);
        },
        managerialData: function (callback) {
            objManagerialDatadaoimpl.getManagerialData(callback);
        },
        transformertransationdata: function (callback) {
            getAllSummaryMapRelatedDetailsTransformer(callback);
        },
    }, function (err, results) {
        if (err) {
            callback(err, null);
            objdaoimpl.closeConnection();
        } else {
            circuitLevelGroupingCalc(results, function (err, results) {
                callback(err, results);
                objdaoimpl.closeConnection();
            });
        }

    });
}

/**
 * @description - Code to do grouping at circuit level
 * @param objResults - result
 * @param {Respose to be returned} callback
 * @return - callback
 */
function circuitLevelGroupingCalc(objResults, callback) {
    var arrTransformerKeys = Object.keys(objResults.managerialData.transformerobj);
    forEach(arrTransformerKeys, function (strTransformerId, index, arrayOfTransformerKeys) {
        try {
            var done = this.async();

            if (objResults.transformertransationdata[strTransformerId]) {
                var strCircuitId = objResults.managerialData.transformerobj[strTransformerId].CircuitID;
                if (objResults.managerialData.circuitobj &&
                    objResults.managerialData.circuitobj[strCircuitId]) {
                    getConcatenatedCumulativeValue(strCircuitId, strTransformerId, objResults);
                }
            }
            if (index === (arrayOfTransformerKeys.length - 1)) {
                callback(null, objResults);
            }
            setTimeout(function () {
                done();
            }, 5);
        } catch (err) {
            callback(err, null);
        }
    });
}

/**
 * @description - Code to process outage data from alarms and events table
 * @param {Respose to be returned} callback
 * @return - callback
 */
function processOutageDetailsFromAlarmEvents(callback) {
    getAlarmAndEvents(function (err, data) {
        try {
            if (err) {
                callback(err, null);
            }
            if (data) {
                callback(null, data);
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}

/**
 * @description - Code to get alarms and events from DELTA_AlarmsAndEvents
 * @param {Respose to be returned} callback
 * @return - callback
 */
function getAlarmAndEvents(callback) {
    objOutageData = {};
    objdaoimpl.getCursorFromCollectionSorted("DELTA_AlarmsAndEvents", ['DBTimestamp'], [{ $gt: new Date(Date.now() - (intnumofdays * 24 * 60 * 60 * 1000)) }], { "DBTimestamp": 1 }, {}, function (err, arrAlarmAndEvents) {
        if (arrAlarmAndEvents) {
            arrAlarmAndEvents.stream()
                .on('data', function (objAlarmAndEventItem) {
                    processAlarmAndEventItem(objAlarmAndEventItem);
                })
                .on('error', function (err) {
                    logger.log(err);
                    arrAlarmAndEvents.close();
                })
                .on('end', function () {
                    callback(null, objOutageData);
                    arrAlarmAndEvents.close();
                });

        } else {
            callback("no alarms", null);
        }

    });
}

/**
 * @description - Code to process alarms and events
 * @param objAlarmAndEventItem - alarms and event item
 * @return Nil
 */
function processAlarmAndEventItem(objAlarmAndEventItem) {
    try {
        var strCellID = objAlarmAndEventItem.result.CellID;
        var objDbTimeStamp = objAlarmAndEventItem.DBTimestamp;
        var objTransformerEvent = objAlarmAndEventItem.result.Transformer;
        objOutageData[strCellID] = (objOutageData[strCellID]) ? objOutageData[strCellID] : [];
        var intOutageDataLen = objOutageData[strCellID].length;
        var intOutageDataIndex = intOutageDataLen - 1;
        updateOutageData(strCellID, objDbTimeStamp, objTransformerEvent, intOutageDataIndex, intOutageDataLen, objOutageData);
    } catch (err) {
        logger.log(err);
    }
}

/**
 * @description - Code to update outage data status
 * @param strCellID - Cell id
 * @param objDbTimeStamp - DBTimestamp
 * @param objTransformerEvent - tranformer event
 * @param intOutageDataIndex - data index
 * @param intOutageDataLen - data length
 * @param objOutageData - outage data
 * @return Nil
 */
function updateOutageData(strCellID, objDbTimeStamp, objTransformerEvent, intOutageDataIndex, intOutageDataLen, objOutageData) {
    var objLatestDataOfTransformer = intOutageDataLen > 0 ? objOutageData[strCellID][intOutageDataIndex] : {};
    objLatestDataOfTransformer = objLatestDataOfTransformer.status === 'Resolved' ? {} : objLatestDataOfTransformer;
    updateOutageDataDetails(strCellID, objDbTimeStamp, objTransformerEvent, intOutageDataIndex, intOutageDataLen, objOutageData, objLatestDataOfTransformer);
}
/**
 * @description - Code to update outage start time end end time
 * @param strCellID - Cell id
 * @param objDbTimeStamp - DBTimestamp
 * @param objTransformerEvent - tranformer event
 * @param intOutageDataIndex - data index
 * @param intOutageDataLen - data length
 * @param objOutageData - outage data
 * @return Nil
 */
function updateOutageDataDetails(strCellID, objDbTimeStamp, objTransformerEvent, intOutageDataIndex, intOutageDataLen, objOutageData, objLatestDataOfTransformer) {
    objDbTimeStamp = moment.utc(objDbTimeStamp, 'YYYY-MM-DD HH:mm:ss').toDate();
    if (objTransformerEvent && objDbTimeStamp) {
        if (objTransformerEvent.PowerFailure === 1) {
            objLatestDataOfTransformer.startTime = (!objLatestDataOfTransformer.startTime) ? objDbTimeStamp : objLatestDataOfTransformer.startTime;
            objLatestDataOfTransformer.endTime = null;
            objOutageData[strCellID].push(objLatestDataOfTransformer);
            return;
        }

        if (objTransformerEvent.PowerFailure === 0 &&
            (objLatestDataOfTransformer.startTime && !objLatestDataOfTransformer.endTime)) {
            objLatestDataOfTransformer.status = 'Resolved';
            objLatestDataOfTransformer.endTime = objDbTimeStamp;
            var intMilliSeconds = moment(objLatestDataOfTransformer.endTime).diff(moment(objLatestDataOfTransformer.startTime));
            objLatestDataOfTransformer.duration = convertMillisecondsToHours(intMilliSeconds);
            if (intOutageDataLen > 0) {
                objOutageData[strCellID][intOutageDataIndex] = objLatestDataOfTransformer;
            }
        }
    }
}
/**
 * @description - Code to get summary map related from DELTA_Transaction_Data
 * @param {Respose to be returned} callback
 * @return - callback
 */
function getAllSummaryMapRelatedDetailsTransformer(callback) {
    objdaoimpl.getCursorFromCollection("DELTA_Transaction_Data", ['DBTimestamp'], [{ $gt: new Date(Date.now() - (1 * 24 * 60 * 60 * 1000)) }], function (err, arrTransformerTransData) {
        objTransformerTransactionData = {};
        if (arrTransformerTransData) {
            arrTransformerTransData.stream()
                .on('data', processTransformerTransResponse)
                .on('error', function (err) {
                    callback(err, null);
                })
                .on('end', function () {
                    var arrNoProccessKey = ['StatusTransformer'];
                    var arrTimestpampKeys = ['ReadTimestamp'];
                    var arrAverageKeys = [];
                    var arrSumKeys = [];
                    var arrLastValKeys = ["TopTemperature", "BottomTemperature"];
                    var arrDifffernceSumKeys = ["ActiveReceivedCumulativeRate_Total", "Apparent_m_Total"];
                    objCalculations.processDataFor(objTransformerTransactionData, arrNoProccessKey, arrAverageKeys, arrSumKeys, arrDifffernceSumKeys, arrTimestpampKeys, arrLastValKeys);
                    callback(null, objTransformerTransactionData);
                    arrTransformerTransData.close();
                });
        }

    });
}
/**
 * @description - Code to process transformer transaction
 * @param transformerTransItem - transformer transaction data
 * @return - Nil
 */
function processTransformerTransResponse(transformerTransItem) {
    try {

        var objTransData = transformerTransItem;
        var arrTransformerResultData = [];
        var strCellIdVal = null;
        if (objTransData.result) {
            arrTransformerResultData = objTransData.result.Transformer;
            strCellIdVal = objTransData.result.CellID;
        }
        var objDBTimestampVal = objTransData.DBTimestamp;

        var transformerResultTranItem = arrTransformerResultData;
        var objRowData = {};

        var arrFilter = ["CellID", "TopTemperature", "StatusTransformer",
            "BottomTemperature", "ActiveReceivedCumulativeRate_Total", "Apparent_m_Total", "ReadTimestamp"];
        objjsFunctions.assignValuesFrmObject(transformerResultTranItem, objRowData, "", arrFilter, false, false);
        objRowData.MeterDBTimestampVal = new Date(objDBTimestampVal);

        if (objRowData && objRowData.StatusTransformer === 'Connected') {
            objTransformerTransactionData[strCellIdVal] = objTransformerTransactionData[strCellIdVal] ? objTransformerTransactionData[strCellIdVal] : [];
            objTransformerTransactionData[strCellIdVal].push(objRowData);
        }
    } catch (err) {
        logger.log(err);
    }
}
/**
 * @description - Code to get concatenated cumulative value
 * @param strCircuitId - Circuit Id
 * @param strTransformerId - transformer id
 * @param objResults - results
 * @return - Nil
 */
function getConcatenatedCumulativeValue(strCircuitId, strTransformerId, objResults) {
    if (objResults.managerialData.circuitobj[strCircuitId].ActiveReceivedCumulativeRate_Total) {
        objResults.managerialData.circuitobj[strCircuitId].ActiveReceivedCumulativeRate_Total +=
            objResults.transformertransationdata[strTransformerId].ActiveReceivedCumulativeRate_Total;
    } else {
        objResults.managerialData.circuitobj[strCircuitId].ActiveReceivedCumulativeRate_Total =
            objResults.transformertransationdata[strTransformerId].ActiveReceivedCumulativeRate_Total;
    }
}
/**
 * @description - Code to convert milliseconds to hour
 * @param ms - millisecond value
 * @return - Nil
 */
function convertMillisecondsToHours(ms) {
    var hours = Math.floor(ms / 3600000);
    var minutes = Math.floor((ms % 3600000) / 60000);
    var seconds = Math.floor(((ms % 360000) % 60000) / 1000);
    return hours + ":" + minutes + ":" + seconds;
}

module.exports = {
    processOutageDetails: processOutageDetails
};