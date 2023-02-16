var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objSummaryMapModel = require('../model/sqltables/summarymapmodel.js');
var objManagerialDatadaoimpl = require('./managerialdatadaoimpl.js');
var objLatestTransactionModel = require('../model/sqltables/latesttransactionmodel.js');
var objSummaryMapModelDAO = require('../model/summarymapdaoimpl.js');
var objConfig = require('../config.js');
var objjsFunctions = require('../util/jsfunctions.js');
var dbConMysql = require('../dao/mysqlconnector.js');
var moment = require("moment");
var forEach = require('async-foreach').forEach;
var fs = require('fs');
var path = require('path');
const json2csv = require('json2csv').parse;
const dateTime = new Date().toLocaleString().slice(-24).replace(/\D/g, '').slice(0, 14);
var connection = dbConMysql.getDbconnection;
var filePath_summary = process.env.csvPath + "/csv-summarymap" + dateTime + ".csv";
var filePath_latest = process.env.csvPath + "/csv-latesttransaction" + dateTime + ".csv";
var smstat = 0;
// var filePath_summary = path.join(__dirname, "../../../", "project-datamart", "summarymap", "public", "csv-summarymap" + dateTime + ".csv");
// var filePath_latest = path.join(__dirname, "../../../", "project-datamart", "summarymap", "public", "csv-latesttransaction" + dateTime + ".csv");
// var filePath_summary = "E:/Delta/project-datamart/summarymap/public/csv-summarymap-" + dateTime + ".csv";
// var filePath_latest = "E:/Delta/project-datamart/summarymap/public/csv-latesttransaction-" + dateTime + ".csv";
const fields = [
    `MeterID`, `Meter_DeviceID`, `MeterLatitude`, `MeterLongitude`, `HypersproutID`, `Meter_ReadTimestamp`, `TransformerID`, `Transformer_CellID`, `TransformerLatitude`, `TransformerLongitude`, `Transformer_ReadTimestamp`, `CircuitLatitude`, `CircuitLongitude`, `CircuitID`, `Meter_Line1InstVoltage`, `Meter_Line1InstCurrent`, `Meter_Line1Frequency`, `Meter_Apparent_m_Total`, `Meter_ActiveReceivedCumulativeRate_Total`, `Meter_ActiveDeliveredCumulativeRate_Total`, `MeterApparentReceivedCumulativeRate_Total`, `Transformer_Line1Voltage`, `Transformer_Line2Voltage`, `Transformer_Line3Voltage`, `Transformer_Line1Current`, `Transformer_Line2Current`, `Transformer_Line3Current`, `Transformer_AmbientTemperarture`, `Transformer_TopTemperature`, `Transformer_BottomTemperature`, `Transformer_TransformerOilLevel`, `Transformer_Apparent_m_Total`, `Transformer_ActiveReceivedCumulativeRate_Total`, `Transformer_ActiveDeliveredCumulativeRate_Total`, `DateTime`, `Hours`, `Circuit ID`, `Circuit_Latitude`, `Circuit_Longitude`, `Transformer ID`, `Hypersprout ID`, `Transformer_Latitude`, `Transformer_Longitude`, `Meter ID`, `Meter_Latitude`, `Meter_Longitude`, `Transformer_active_energy_received`, `Transformer_active_energy_delivered`, `Meter_active_energy_received`, `Meter_active_energy_delivered`, `Top_Temperature`, `Bottom_Temperature`, `ambient_temparature`, `Energy_Apparent_Absolute`, `Date`, `TransformerActiveReceivedCumulativeRate_Total`, `TransformerActiveDeliveredCumulativeRate_Total`, `MeterActiveReceivedCumulativeRate_Total`, `MeterActiveDeliveredCumulativeRate_Total`, `NetworkResponceRate`, `TopTemperature`, `BottomTemperature`, `AmbientTemperarture`, `Apparent_m_Total`, `Circuit_Id`, `Tranformer_Id`, `Hypersprout_ID`, `Meter_Id`, `Non_technichal_Loss`, `ActualMeter_ReadTimestamp`, `ActualTransformer_ReadTimestamp`, `SolarPanel`, `EVMeter`, `Meter_Line2InstVoltage`, `Meter_Line3InstVoltage`, `Meter_Line2InstCurrent`, `Meter_Line3InstCurrent`, `Meter_Line2Frequency`, `Meter_Line3Frequency`, `Meter_Phase`, `Meter_Line1PowerFactor`, `Meter_Line2PowerFactor`, `Meter_Line3PowerFactor`, `Transformer_Phase`, `Transformer_Line1PhaseAngle`, `Transformer_Line2PhaseAngle`, `Transformer_Line3PhaseAngle`, `Transformer_BatteryVoltage`, `Transformer_BatteryStatus`, `TransformerRating`, `IsHyperHub`, `createdAt`, `updatedAt`
];
const latest_fields = [
    `MeterID`, `Meter_DeviceID`, `MeterLatitude`, `MeterLongitude`, `HypersproutID`, `Meter_ReadTimestamp`, `TransformerID`, `Transformer_CellID`, `TransformerLatitude`, `TransformerLongitude`, `Transformer_ReadTimestamp`, `CircuitLatitude`, `CircuitLongitude`, `CircuitID`, `Meter_Line1InstVoltage`, `Meter_Line1InstCurrent`, `Meter_Line1Frequency`, `Meter_Apparent_m_Total`, `Meter_ActiveReceivedCumulativeRate_Total`, `Meter_ActiveDeliveredCumulativeRate_Total`, `Transformer_Line1Voltage`, `Transformer_Line2Voltage`, `Transformer_Line3Voltage`, `Transformer_Line1Current`, `Transformer_Line2Current`, `Transformer_Line3Current`, `Transformer_AmbientTemperarture`, `Transformer_TopTemperature`, `Transformer_BottomTemperature`, `Transformer_TransformerOilLevel`, `Transformer_Apparent_m_Total`, `Transformer_ActiveReceivedCumulativeRate_Total`, `Transformer_ActiveDeliveredCumulativeRate_Total`, `Meter_Line2InstVoltage`, `Meter_Line3InstVoltage`, `Meter_Line2InstCurrent`, `Meter_Line3InstCurrent`, `Meter_Line2Frequency`, `Meter_Line3Frequency`, `Meter_Phase`, `Meter_Line1PowerFactor`, `Meter_Line2PowerFactor`, `Meter_Line3PowerFactor`, `Transformer_Phase`, `Transformer_Line1PhaseAngle`, `Transformer_Line2PhaseAngle`, `Transformer_Line3PhaseAngle`, `Transformer_BatteryVoltage`, `Transformer_BatteryStatus`, `createdAt`, `updatedAt`
]
var loopIndex = 0;
var loopCount = 0;
var summaryArray = [];
var latestArray = [];
var logger = console;
var arrDateToCompare = [];
var arrAvailableMeters = [];
var objMeterHSMap = {};

/**
 * @description - Code to post all summary map related data
 * @param objData -  data
 * @return callback
 */
function postAllSummaryMapRelatedDetails(context, objData, callback) {
    try {
        logger = context
        arrDateToCompare = [];
        objMeterHSMap = {};
        summaryArray = [];
        latestArray = [];
        arrAvailableMeters = [];
        objManagerialDatadaoimpl.getManagerialData(function (err, objManagerialdata) {
            if (err) {
                logger.log(err);
            }
            arrAvailableMeters = objManagerialdata ? Object.keys(objManagerialdata.meterobj) : [];
            populateMeterManagerialInfo(objManagerialdata);
            var objCurrentDate = new Date();
            var objStartDate = new Date();
            populateRequiredDates(objStartDate, objCurrentDate);
            loopIndex = 0;
            loopCount = 0;
            objdaoimpl.synctable("summarymap", objSummaryMapModel.objSummaryMap,
                objSummaryMapModel.objTableProps, function (err) {
                    if (err) {
                        logger.log(err);
                    }
                    objdaoimpl.synctable("latesttransactions", objLatestTransactionModel.objLatestTrans,
                        objLatestTransactionModel.objTableProps, function (err) {
                            if (err) {
                                logger.log(err);
                            }
                            processAllAvailData(context, objData, callback);
                        });
                });
        });
    } catch (err) {
        logger.error(err);
    }
}

/**
 * @description - Code to populate all meter managerial data
 * @param objManagerialdata -  data
 * @return Nil
 */
function populateMeterManagerialInfo(objManagerialdata) {
    var intArrAvailableMetersLen = arrAvailableMeters.length;
    var strMeterId;
    var strTransformerId;
    var strCircuitId;
    while (intArrAvailableMetersLen--) {
        strMeterId = arrAvailableMeters[intArrAvailableMetersLen];
        strTransformerId = objManagerialdata.meterobj[strMeterId].TransformerID;
        strCircuitId = "";
        objMeterHSMap[strMeterId] = {};
        objjsFunctions.assignValuesFrmObject(objManagerialdata.meterobj[strMeterId], objMeterHSMap[strMeterId], "", [], true, false);
        objMeterHSMap[strMeterId].Meter_DeviceID = strMeterId;
        objMeterHSMap[strMeterId].TransformerID = strTransformerId;

        if (objManagerialdata.transformerobj[strTransformerId]) {
            objjsFunctions.assignValuesFrmObject(objManagerialdata.transformerobj[strTransformerId], objMeterHSMap[strMeterId], "", [], true, false);
            objMeterHSMap[strMeterId].Transformer_CellID = objManagerialdata.transformerobj[strTransformerId].HypersproutID;
            strCircuitId = objMeterHSMap[strMeterId].CircuitID = objManagerialdata.transformerobj[strTransformerId].CircuitID;
        }

        if (objManagerialdata.circuitobj[strCircuitId]) {
            objjsFunctions.assignValuesFrmObject(objManagerialdata.circuitobj[strCircuitId], objMeterHSMap[strMeterId], "", [], true, false);
        }

    }
}
/**
 * @description - Code to populate non reporting meter transaction data
 * @param objData -  data
 * @return Nil
 */
function populateNonReportingMeterTransData(objData, callback) {
    var intArrAvailableMetersLen = arrAvailableMeters.length;
    var oldLoopCount = loopCount;
    while (intArrAvailableMetersLen--) {
        var strMeterId = arrAvailableMeters[intArrAvailableMetersLen];
        arrAvailableMeters.splice(intArrAvailableMetersLen, 1);
        var strHypersproutId = objMeterHSMap[strMeterId].Transformer_CellID;
        objData.meterdata[strHypersproutId] = objData.meterdata[strHypersproutId] ? objData.meterdata[strHypersproutId] : {};
        objData.meterdata[strHypersproutId][strMeterId] = objData.meterdata[strHypersproutId][strMeterId] ? objData.meterdata[strHypersproutId][strMeterId] : {};
        objData.meterdata[strHypersproutId][strMeterId].TranData = objData.meterdata[strHypersproutId][strMeterId].TranData ? objData.meterdata[strHypersproutId][strMeterId].TranData : {};
        objData.meterdata[strHypersproutId][strMeterId].managerialdata = objMeterHSMap[strMeterId];
        processTransactionRecords(arrDateToCompare, objData, strHypersproutId, strMeterId, callback);
    }
    if (oldLoopCount === loopCount) {
        callback(null, true);
    }
}
/**
 * @description - Code to populate required dates
 * @param objStartDate -  start data
 * @param objCurrentDate - today date
 * @return Nil
 */
function populateRequiredDates(objStartDate, objCurrentDate) {
    objStartDate.setUTCHours(objStartDate.getUTCHours() - 2);
    //objStartDate.setUTCDate(objStartDate.getUTCDate() - objConfig.numberofDaysSM);
    objStartDate.setUTCMinutes(0);
    objStartDate.setUTCSeconds(0);
    while (objStartDate < objCurrentDate) {
        var objRequiredDateFormat = moment.utc(objStartDate, 'DD-MM-YYYY HH:mm:ss');
        var strDateFormat = objRequiredDateFormat.format('YYYY-MM-DD_HH');
        arrDateToCompare.push(strDateFormat);
        objStartDate.setUTCMinutes(objStartDate.getUTCMinutes() + 60);
    }
}
MeterHSloop = async (arrHSKeys, objData, callback) => {
    // logger.log('---Inside new async function');
    return new Promise((resolve, reject) => {
        forEach(arrHSKeys, async function (strHSId) {
            if (objData.meterdata.hasOwnProperty(strHSId)) {
                console.log("yes we have meter property======>"+strHSId);
                //var done = this.async();
                await loopMeterData(objData, strHSId, callback);
                // setTimeout(function () {
                //     done();
                // }, 100);
            }
        });
        resolve(true);
    });
}

/**
 * @description - Code to process all available data
 * @param objData -   data
 * @return callback
 */
function processAllAvailData(context, objData, callback) {
    logger = context;

    var arrHSKeys = Object.keys(objData.meterdata);
    //no of HS objects from the meter data 3 [ '16', '1246', '1188' ]

    console.log("ArrHSKey === > " + arrHSKeys.length)
    if (arrHSKeys.length === 0) {
        populateNonReportingMeterTransData(objData, callback);
    }
     console.log('This is the array hs keys============>')
     console.log(arrHSKeys);
    MeterHSloop(arrHSKeys, objData, callback).then((value) => {
        logger.log("this is end", summaryArray.length, latestArray.length);
        //getting the details in summary array and latest array
        //  callcsvUploadandprocess().then(val);
        if (summaryArray.length > 0 || latestArray.length > 0) {
            callcsvUploadandprocess(smstat).then((smstat) => {
                logger.log("in status--->" + smstat)
                if (smstat == 1) {
                    callback(null, true);
                } else {
                    callback("err", true);
                }
            })
        } else {
            logger.log("No data available for processing--->")
            callback(null, true);
        }


    });
}
var callcsvUploadandprocess = async function (smstat) {
    return new Promise((resolve, reject) => {
        dbConMysql.pool.getConnection(function (err, connection) {
            if (summaryArray.length > 0) {
                logger.log('in 1');
                try {
                    csv = json2csv(summaryArray, { fields });
                } catch (err) {
                    reject(err)
                    logger.log(err);
                }
                fs.appendFile(filePath_summary, csv, function (err) {
                    if (err) {
                        reject(err)
                        logger.log(err);
                    } else {
                        var sql = "LOAD DATA LOCAL INFILE'" + filePath_summary + "' REPLACE INTO TABLE summarymap FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (`MeterID` ,`Meter_DeviceID` ,`MeterLatitude` ,`MeterLongitude` ,`HypersproutID` ,`Meter_ReadTimestamp` ,`TransformerID` ,`Transformer_CellID` ,`TransformerLatitude` ,`TransformerLongitude` ,`Transformer_ReadTimestamp` ,`CircuitLatitude` ,`CircuitLongitude` ,`CircuitID` ,`Meter_Line1InstVoltage` ,`Meter_Line1InstCurrent` ,`Meter_Line1Frequency` ,`Meter_Apparent_m_Total` ,`Meter_ActiveReceivedCumulativeRate_Total` ,`Meter_ActiveDeliveredCumulativeRate_Total`,`MeterApparentReceivedCumulativeRate_Total`, `Transformer_Line1Voltage`,`Transformer_Line2Voltage`,`Transformer_Line3Voltage`,`Transformer_Line1Current` ,`Transformer_Line2Current` ,`Transformer_Line3Current` ,`Transformer_AmbientTemperarture` ,`Transformer_TopTemperature` ,`Transformer_BottomTemperature` ,`Transformer_TransformerOilLevel` ,`Transformer_Apparent_m_Total` ,`Transformer_ActiveReceivedCumulativeRate_Total` ,`Transformer_ActiveDeliveredCumulativeRate_Total` ,`DateTime` ,`Hours` ,`Circuit ID` ,`Circuit_Latitude` ,`Circuit_Longitude` ,`Transformer ID` ,`Hypersprout ID` ,`Transformer_Latitude` ,`Transformer_Longitude` ,`Meter ID` ,`Meter_Latitude` ,`Meter_Longitude` ,`Transformer_active_energy_received` ,`Transformer_active_energy_delivered` ,`Meter_active_energy_received` ,`Meter_active_energy_delivered` ,`Top_Temperature` ,`Bottom_Temperature` ,`ambient_temparature` ,`Energy_Apparent_Absolute` ,`Date` ,`TransformerActiveReceivedCumulativeRate_Total` ,`TransformerActiveDeliveredCumulativeRate_Total` ,`MeterActiveReceivedCumulativeRate_Total` ,`MeterActiveDeliveredCumulativeRate_Total`,`NetworkResponceRate`,`TopTemperature` ,`BottomTemperature` ,`AmbientTemperarture` ,`Apparent_m_Total` ,`Circuit_Id` ,`Tranformer_Id` ,`Hypersprout_ID` ,`Meter_Id` ,`Non_technichal_Loss` ,`ActualMeter_ReadTimestamp` ,`ActualTransformer_ReadTimestamp` ,`SolarPanel` ,`EVMeter` ,`Meter_Line2InstVoltage` ,`Meter_Line3InstVoltage` ,`Meter_Line2InstCurrent` ,`Meter_Line3InstCurrent` ,`Meter_Line2Frequency` ,`Meter_Line3Frequency` ,`Meter_Phase` ,`Meter_Line1PowerFactor` ,`Meter_Line2PowerFactor` ,`Meter_Line3PowerFactor` ,`Transformer_Phase` ,`Transformer_Line1PhaseAngle` ,`Transformer_Line2PhaseAngle` ,`Transformer_Line3PhaseAngle` ,`Transformer_BatteryVoltage` ,`Transformer_BatteryStatus` ,`TransformerRating` ,`IsHyperHub` ,`createdAt` ,`updatedAt` ) set id = NULL;"
                        connection.query(sql, function (err, resultsummary) {
                            if (err) {
                                reject(err)
                                logger.log(err);
                            }
                            if (resultsummary) {
                                logger.log("inside summary insertion");
                                if (latestArray.length > 0) {
                                    logger.log('in latest array process start')
                                    try {
                                        csv = json2csv(latestArray, { latest_fields });
                                    } catch (err) {
                                        logger.log(err);
                                    }
                                    fs.appendFile(filePath_latest, csv, function (err) {
                                        if (err) {
                                            logger.log(err);
                                        } else {
                                            logger.log("mysql insertion array latest");
                                            var sql = "LOAD DATA LOCAL INFILE'" + filePath_latest + "' REPLACE INTO TABLE latesttransactions FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (`MeterID`, `Meter_DeviceID`, `MeterLatitude`, `MeterLongitude`, `HypersproutID`, `Meter_ReadTimestamp`, `TransformerID`, `Transformer_CellID`, `TransformerLatitude`, `TransformerLongitude`, `Transformer_ReadTimestamp`, `CircuitLatitude`, `CircuitLongitude`, `CircuitID`, `Meter_Line1InstVoltage`, `Meter_Line1InstCurrent`, `Meter_Line1Frequency`, `Meter_Apparent_m_Total`, `Meter_ActiveReceivedCumulativeRate_Total`, `Meter_ActiveDeliveredCumulativeRate_Total`, `Transformer_Line1Voltage`, `Transformer_Line2Voltage`, `Transformer_Line3Voltage`, `Transformer_Line1Current`, `Transformer_Line2Current`, `Transformer_Line3Current`, `Transformer_AmbientTemperarture`, `Transformer_TopTemperature`, `Transformer_BottomTemperature`, `Transformer_TransformerOilLevel`, `Transformer_Apparent_m_Total`, `Transformer_ActiveReceivedCumulativeRate_Total`, `Transformer_ActiveDeliveredCumulativeRate_Total`, `Meter_Line2InstVoltage`, `Meter_Line3InstVoltage`, `Meter_Line2InstCurrent`, `Meter_Line3InstCurrent`, `Meter_Line2Frequency`, `Meter_Line3Frequency`, `Meter_Phase`, `Meter_Line1PowerFactor`, `Meter_Line2PowerFactor`, `Meter_Line3PowerFactor`, `Transformer_Phase`, `Transformer_Line1PhaseAngle`, `Transformer_Line2PhaseAngle`, `Transformer_Line3PhaseAngle`, `Transformer_BatteryVoltage`, `Transformer_BatteryStatus`, `createdAt`, `updatedAt`) set id = NULL;"
                                            connection.query(sql, function (err, resultlatest) {
                                                if (err) {
                                                    reject(err)
                                                    logger.log(err);
                                                }
                                                else {
                                                    //fs.unlinkSynclinkSync(filePath_summary);
                                                    logger.log("unlinking temp file summary -->" + filePath_summary)
                                                }
                                                if (resultlatest) {
                                                    logger.log('success for summary and lastest');
                                                    smstat = 1;
                                                    resolve(smstat);
                                                    logger.log(resultlatest);
                                                    //fs.unlinkSynclinkSync(filePath_latest);
                                                    logger.log("unlinking temp file latest-->" + filePath_latest)
                                                }
                                            });
                                        }
                                    });
                                } else {
                                    logger.log('success for summary ,No data in latest');
                                    smstat = 1;
                                    //fs.unlinkSynclinkSync(filePath_summary);
                                    resolve(smstat);
                                }
                            }
                        });
                    }
                });
            }
            else if (summaryArray.length == 0 && latestArray.length > 0) {
                try {
                    csv = json2csv(latestArray, { latest_fields });
                } catch (err) {
                    logger.log(err);
                }
                fs.appendFile(filePath_latest, csv, function (err) {
                    if (err) {
                        logger.log(err);
                    } else {
                        var sql = "LOAD DATA LOCAL INFILE'" + filePath_latest + "' REPLACE INTO TABLE latesttransactions FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (`MeterID`, `Meter_DeviceID`, `MeterLatitude`, `MeterLongitude`, `HypersproutID`, `Meter_ReadTimestamp`, `TransformerID`, `Transformer_CellID`, `TransformerLatitude`, `TransformerLongitude`, `Transformer_ReadTimestamp`, `CircuitLatitude`, `CircuitLongitude`, `CircuitID`, `Meter_Line1InstVoltage`, `Meter_Line1InstCurrent`, `Meter_Line1Frequency`, `Meter_Apparent_m_Total`, `Meter_ActiveReceivedCumulativeRate_Total`, `Meter_ActiveDeliveredCumulativeRate_Total`, `Transformer_Line1Voltage`, `Transformer_Line2Voltage`, `Transformer_Line3Voltage`, `Transformer_Line1Current`, `Transformer_Line2Current`, `Transformer_Line3Current`, `Transformer_AmbientTemperarture`, `Transformer_TopTemperature`, `Transformer_BottomTemperature`, `Transformer_TransformerOilLevel`, `Transformer_Apparent_m_Total`, `Transformer_ActiveReceivedCumulativeRate_Total`, `Transformer_ActiveDeliveredCumulativeRate_Total`, `Meter_Line2InstVoltage`, `Meter_Line3InstVoltage`, `Meter_Line2InstCurrent`, `Meter_Line3InstCurrent`, `Meter_Line2Frequency`, `Meter_Line3Frequency`, `Meter_Phase`, `Meter_Line1PowerFactor`, `Meter_Line2PowerFactor`, `Meter_Line3PowerFactor`, `Transformer_Phase`, `Transformer_Line1PhaseAngle`, `Transformer_Line2PhaseAngle`, `Transformer_Line3PhaseAngle`, `Transformer_BatteryVoltage`, `Transformer_BatteryStatus`, `createdAt`, `updatedAt`) set id = NULL;"
                        connection.query(sql, function (err, result) {
                            if (err) {
                                reject(err)
                                logger.log(err);
                            }
                            if (result) {
                                smstat = 1;
                                resolve(smstat);
                                //fs.unlinkSynclinkSync(filePath_latest);
                                logger.log("unlinking temp file -->" + filePath_latest)
                            }
                        });
                    }
                });
                //callback(null,true);
            }
        });

    });

}
/**
 * @description - Code to loop over meter data
 * @param objData -   data
 * @param objHSKey - HS key
 * @return callback
 */
async function loopMeterData(objData, objHSKey, callback) {
    var objDeviceMergedData = objData.meterdata;
    var arrMeterIds = Object.keys(objDeviceMergedData[objHSKey]);
        forEach(arrMeterIds, function (objDeviceID) {
            // var done = this.async();
            if (objDeviceMergedData[objHSKey].hasOwnProperty(objDeviceID) && objDeviceID !== "TransformerData") {
                var indexOfMeter = arrAvailableMeters.indexOf(objDeviceID);
                if (indexOfMeter !== -1) {
                    arrAvailableMeters.splice(indexOfMeter, 1);
                }
                processTransactionRecords(arrDateToCompare, objData, objHSKey, objDeviceID, callback);
            }
            // setTimeout(function () {
            //     done();
            // }, 50);
        });
    
  
}
/**
 * @description - Code to process transaction data
 * @param arrDateToCompare -   date to compare
 * @param objData - data
 * @param objHSKey - HS key
 * @param objDeviceID - device id
 * @return callback
 */
function processTransactionRecords(arrDateToCompare, objData, objHSKey, objDeviceID, callback) {
    let objDeviceMergedData = objData.meterdata;
    let arrDateToCompareLen = arrDateToCompare.length;
    let objPropertyKey;
    while (arrDateToCompareLen--) {
        objPropertyKey = arrDateToCompare[arrDateToCompareLen];
        if (objDeviceMergedData[objHSKey][objDeviceID].TranData.hasOwnProperty(objPropertyKey)) {
            loopCount += objSummaryMapModelDAO.updateSummaryMapModel(arrDateToCompare,objDeviceMergedData, objHSKey, objDeviceID, objPropertyKey, summaryArray, latestArray,
                responseCallback);
        } else {
            objDeviceMergedData[objHSKey][objDeviceID].TranData = objDeviceMergedData[objHSKey][objDeviceID].TranData ? objDeviceMergedData[objHSKey][objDeviceID].TranData : {};
            objDeviceMergedData[objHSKey][objDeviceID].TranData[objPropertyKey] = {};
            var objMomentDateToUpdate = moment.utc(objPropertyKey, 'YYYY-MM-DD_HH');
            var objDateToUpdate = new Date();
            objDateToUpdate.setTime(objMomentDateToUpdate.valueOf());
            objDeviceMergedData[objHSKey][objDeviceID].TranData[objPropertyKey].Meter_ReadTimestamp = objDateToUpdate;
            objDeviceMergedData[objHSKey][objDeviceID].TranData[objPropertyKey].NetworkResponceRate = 0;
            loopCount += objSummaryMapModelDAO.updateSummaryMapModel(arrDateToCompare,objDeviceMergedData, objHSKey, objDeviceID, objPropertyKey, summaryArray, latestArray,
                responseCallback);
        }
    }
    loopCount += objSummaryMapModelDAO.updateSummaryMapModel(arrDateToCompare,objDeviceMergedData, objHSKey, objDeviceID, 'MeterLastData', summaryArray, latestArray,
        responseCallback);

    function responseCallback(err) {
        if (err) {
            logger.error("Error", err);
        }
        loopIndex++;
        //  logger.log("loopIndex"+loopIndex);

        if (loopIndex >= loopCount) {
            if (arrAvailableMeters.length > 0) {
                populateNonReportingMeterTransData(objData, callback);
            } else {
                callback(err, true);
            }
        }
    }
}

module.exports = {
    postAllSummaryMapRelatedDetails: postAllSummaryMapRelatedDetails
};