var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var moment = require("moment");
var objConfig = require('../config.js');
var objCalculations = require('../util/calculations.js');
var objjsFunctions = require('../util/jsfunctions.js');
var objProcessedManagerialData = require('./summarymapmanagerialdataprocessor');
var objProcessData = require('./summarymapdataprocessor');
var logger = console;
var dbConMysql = require('../dao/mysqlconnector.js');
var intNumOfDays = objConfig.numberofDaysSM;
var objOuptMeterTransactionData;
var objOuptTransTransactionData;
var COMMONFAULT_TEXT = "CommonFault";
var PhaseFault3 = "3PHASE_UART_FAULT";
var objStartDate;
var smstat1 = 0;
var mergeStatus = 0;
var mapStatus = 0;
var arrFilter = ["CellID", "Line1Voltage", "Line2Voltage", "Line3Voltage",
    "Line1Current", "Line2Current", "Line3Current", "AmbientTemperarture", "TopTemperature",
    "BottomTemperature", "TransformerOilLevel", "ActiveReceivedCumulativeRate_Total", "ActiveDeliveredCumulativeRate_Total",
    "Apparent_m_Total", "ReadTimestamp", "Line1PhaseAngle", "Line2PhaseAngle", "Line3PhaseAngle",
    "BatteryVoltage", "BatteryStatus"];

var arrMeterFilter = ["CellID", "DeviceID", "ActiveReceivedCumulativeRate_Total", "ActiveDeliveredCumulativeRate_Total",
    "Apparent_m_Total", "Line1InstVoltage", "Line2InstVoltage", "Line3InstVoltage", "Line1InstCurrent",
    "Line2InstCurrent", "Line3InstCurrent", "Line1Frequency", "Line2Frequency", "Line3Frequency",
    "ReadTimestamp", "Status", "Line1PowerFactor", "Line2PowerFactor", "Line3PowerFactor", "ApparentReceivedCumulativeRate_Total", "SelfHeal"];
/**
 * @description - Code to get all summary map related data
 * @param context - console
 * @return - callback
 */
function getAllSummaryMapRelatedDetails(context, callback) {
    logger = context;
    intNumOfDays = objConfig.numberofDaysSM;
    objStartDate = new Date();
    objStartDate.setUTCHours(objStartDate.getUTCHours() - 3);
    //objStartDate.setUTCDate(objStartDate.getUTCDate() - objConfig.numberofDaysSM);
    objStartDate.setUTCMinutes(0);
    objStartDate.setUTCSeconds(0);
    console.log("This is the objStartDate------->");
    console.log(objStartDate);
    getAllSummaryMapTransactionRelatedDetails(objStartDate, function (err, data) {
        try {
            if (data) {
                var transData = {}
                arraymeterdatabasedontransformer(transData, data).then((mapStatus) => {
                    if (mapStatus == 1) {
                        let meterobjectcount = Object.keys(data.meterdata).length;
                        console.log(meterobjectcount);
                        if (meterobjectcount > 0) {
                            let loopcount3 = 0;
                            for (let strPropertyKey in data.meterdata) {
                                for (var strMeterIdVal in data.meterdata[strPropertyKey]) {
                                    if (data.meterdata[strPropertyKey].hasOwnProperty(strMeterIdVal)) {
                                        objMeterData = data.meterdata[strPropertyKey][strMeterIdVal];
                                        if (transData[strMeterIdVal] !== undefined && transData[strMeterIdVal].parentTransformer !== undefined) {
                                            let MeterTranformerId = transData[strMeterIdVal].parentTransformer
                                            //if the tranformer cellid and cellid is diffrent
                                            if (MeterTranformerId != strPropertyKey) {
                                                //if the tranformer cellid is exsist in the meter array
                                                if (data.meterdata.hasOwnProperty(MeterTranformerId)) {
                                                    data.meterdata[MeterTranformerId][strMeterIdVal] = {};
                                                    assignValuesFrmObject(objMeterData, data.meterdata[MeterTranformerId][strMeterIdVal]);
                                                    delete data.meterdata[strPropertyKey][strMeterIdVal]
                                                    if (Object.keys(data.meterdata[strPropertyKey]).length == 0) {
                                                        console.log('deleteting/modifying the key --------------> ' + strPropertyKey)
                                                        data.meterdata[strPropertyKey] = {};
                                                        data.meterdata[strPropertyKey][0] = {};
                                                        data.meterdata[strPropertyKey][0].managerialdata = {};
                                                        data.meterdata[strPropertyKey][0].managerialdata.Meter_CellID = strPropertyKey;
                                                        data.meterdata[strPropertyKey][0].managerialdata.MeterSerialNumber = 0;
                                                        data.meterdata[strPropertyKey][0].MeterLastData = {};
                                                    }
                                                } else {
                                                    //if the tranformer cellid is not exsist in the meter array
                                                    data.meterdata[MeterTranformerId] = {};
                                                    data.meterdata[MeterTranformerId][strMeterIdVal] = {};
                                                    assignValuesFrmObject(objMeterData, data.meterdata[MeterTranformerId][strMeterIdVal]);
                                                    delete data.meterdata[strPropertyKey][strMeterIdVal];
                                                    if (Object.keys(data.meterdata[strPropertyKey]).length == 0) {
                                                        console.log('deleteting/modifying the key ------> ' + strPropertyKey)
                                                        data.meterdata[strPropertyKey] = {};
                                                        data.meterdata[strPropertyKey][0] = {};
                                                        data.meterdata[strPropertyKey][0].managerialdata = {};
                                                        data.meterdata[strPropertyKey][0].managerialdata.Meter_CellID = strPropertyKey;
                                                        data.meterdata[strPropertyKey][0].managerialdata.MeterSerialNumber = 0;
                                                        data.meterdata[strPropertyKey][0].MeterLastData = {};
                                                    }
                                                }
                                            }
                                        } else {
                                            console.log('In side the new check ** else')
                                            continue;
                                        }
                                    }
                                }
                                loopcount3++;
                                if (loopcount3 === meterobjectcount) {
                                    console.log('*****************************This is the Meter data after making the mapping with Transformer object****************************************');
                                    console.log(data.meterdata);
                                    mergeAndPopulateManagerialData(data, callback);
                                }
                            }
                        }else{
                            console.log('*****************************No chnages as meter count is 0 or null****************************************');
                            console.log(data.meterdata);
                            mergeAndPopulateManagerialData(data, callback);
                        }


                    }

                })

            } else {
                logger.log(err);
                callback(err, null);
            }
            //objdaoimpl.closeConnection();
        } catch (exc) {
            //objdaoimpl.closeConnection();
            callback(exc, null);
        }
    });
}

/**
 * @description - Code to merge and populate managerial data
 * @param data - data
 * @return - callback
 */

function mergeAndPopulateManagerialData(data, callback) {
    mergeKeys(data);
    objProcessedManagerialData.getManagerialData(data, function (err) {
        if (err) {
            logger.log(err);
        }
        mergeData(data)
        if (data && data.meterdata) {
            var objCellData = data.meterdata;
            var objMeterData;
            var arrCellObjDet;
            var totalNoOfMeters;
            for (var strCellId in objCellData) {
                if (objCellData.hasOwnProperty(strCellId) && objCellData[strCellId]) {
                    objMeterData = objCellData[strCellId];
                    arrCellObjDet = Object.keys(objMeterData);
                    totalNoOfMeters = arrCellObjDet.length - 1;
                    processMeterDataForAccumlation(objMeterData, totalNoOfMeters);
                }
            }
        }
        objdaoimpl.closeConnection();
        callback(null, data);
    });
}
function arraymeterdatabasedontransformer(transData, objParentData) {
    return new Promise((resolve, reject) => {
        try {
            var objInput = objParentData.meterdata;
            var objKeyLen = Object.keys(objInput).length;
            var loopIndex = 0;
            var TotalarrMeterData = [];
            if (objKeyLen === 0) {
                resolve(mapStatus = 1);
            }
            for (var strLoopPropertyKey in objInput) {
                if (objInput.hasOwnProperty(strLoopPropertyKey)) {
                    var strPropertyKey = parseInt(strLoopPropertyKey);
                    for (var strMeterIdVal in objInput[strPropertyKey]) {
                        if (objInput[strPropertyKey].hasOwnProperty(strMeterIdVal)) {
                            TotalarrMeterData.push(parseInt(strMeterIdVal));
                        }
                    }
                }
                loopIndex++
                if (loopIndex == objKeyLen) {
                    console.log("This is the tota meter array--------->");
                    console.log(TotalarrMeterData);
                    TotalarrMeterData.splice(0, TotalarrMeterData.length, ...(new Set(TotalarrMeterData)))
                    objdaoimpl.getDataFromCollection("DELTA_Meters", ['MeterID'], [TotalarrMeterData], function (err, objSelMeterData) {
                        if (err) {
                            reject(err)
                        } else {
                            if (objSelMeterData && objSelMeterData.length > 0) {
                                for (var i = 0; i < objSelMeterData.length; i++) {
                                    transData[objSelMeterData[i].MeterID] = {};
                                    transData[objSelMeterData[i].MeterID].parentTransformer = objSelMeterData[i].TransformerID
                                    if (i == (objSelMeterData.length - 1)) {
                                        resolve(mapStatus = 1);
                                    }
                                }
                            } else {
                                resolve(mapStatus = 1);
                            }
                        }

                    })
                }
            }


        } catch (err) {
            reject(err)
        }
    })
}
/**
 * @description - Code to process meter data for accumulative value
 * @param objMeterData - meter data
 * @param totalNoOfMeters - no of meters
 * @return - callback
 */
function processMeterDataForAccumlation(objMeterData, totalNoOfMeters) {
    for (var strDateVal in objMeterData.TransformerData) {
        if (objMeterData.TransformerData.hasOwnProperty(strDateVal)) {
            calculateAccumlativeValues(objMeterData, totalNoOfMeters, strDateVal);
        }
    }
}
/**
 * @description - Code to calculate accumulative value
 * @param objMeterData - meter data
 * @param totalNoOfMeters - no of meters
 * @param strDateVal - starting date
 * @return - callback
 */
function calculateAccumlativeValues(objMeterData, totalNoOfMeters, strDateVal) {
    /** old logic ---  objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total = 0;
      var objMeterDataDet;
      for (var strKey in objMeterData) {
          if (objMeterData.hasOwnProperty(strKey) && (strKey !== 'TransformerData' && objMeterData[strKey])) {
              objMeterDataDet = objMeterData[strKey].TranData[strDateVal];
              if (objMeterDataDet) {
                  objMeterData.TransformerData[strDateVal].AllMeter_CumulativeRate_Total += objMeterDataDet.Meter_;
              }
          }
      }
      objMeterData.TransformerData[strDateVal].Non_Technical_Loss =
          ((objMeterData.TransformerData[strDateVal].Transformer_ActiveReceivedCumulativeRate_Total -
              objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total) +
              (objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total * 0.10)) / totalNoOfMeters;
              **/

    var hour = parseInt(strDateVal.substring(strDateVal.lastIndexOf('_') + 1));
    var operationalHours;
    var nonTechLossItemUsage;
    var noOfConnectedItems;
    var nonTechLossPerItem;
    var totalNonTechLossItem;
    var lineLossFactor;
    var hyperSproutLoss;
    var hyperHubLoss = 0;

    objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total = 0;

    for (var strKey in objMeterData) {


        if (objMeterData[strKey].managerialdata != undefined) {

            totalNonTechLossItem = 0;

            if (objMeterData[strKey].managerialdata.TechnicalItems != undefined) {

                // add technical loss for each hour for each transformer
                for (var i = 0; i < objMeterData[strKey].managerialdata.TechnicalItems.length; i++) {
                    if (objMeterData[strKey].managerialdata.TransformerSerialNumber === objMeterData[strKey].managerialdata.TechnicalItems[i].TransformerSerialNo) {

                        nonTechLossPerItem = [];
                        operationalHours = objMeterData[strKey].managerialdata.TechnicalItems[i].OperationalHours;
                        nonTechLossItemMetered = parseInt(objMeterData[strKey].managerialdata.TechnicalItems[i].Metered);
                        nonTechLossItemUsage = parseInt(objMeterData[strKey].managerialdata.TechnicalItems[i].UsagePerDay);
                        noOfConnectedItems = parseInt(objMeterData[strKey].managerialdata.TechnicalItems[i].NoOfConnectedItems);

                        //technical loss will be applied only to specific hours
                        if (operationalHours.includes(hour)) {

                            nonTechLossPerItem = (nonTechLossItemUsage * noOfConnectedItems) / operationalHours.length;
                            totalNonTechLossItem = totalNonTechLossItem + nonTechLossPerItem;

                        }
                    } else {
                        totalNonTechLossItem = 0;
                    }
                }
            }

            lineLossFactor = objMeterData[strKey].managerialdata.LineLossFactor;
            hyperSproutLoss = objMeterData[strKey].managerialdata.HyperSproutLoss;
            hyperHubLoss = objMeterData[strKey].managerialdata.HyperHubLoss * objMeterData[strKey].managerialdata.NoOfHyperHubAllocated;

        }
        //calculate total of Meter ActiveReceivedCumulativeRate
        if (objMeterData.hasOwnProperty(strKey) && (strKey !== 'TransformerData' && objMeterData[strKey])) {
            var objMeterDataDet = objMeterData[strKey].TranData[strDateVal];
            if (objMeterDataDet) {
                objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total += objMeterDataDet.Meter_ActiveReceivedCumulativeRate_Total*objMeterData[strKey].managerialdata.Meter_CTRatio;;
            }
        }

    }
    // if technical loss is available for a transformer
    if (totalNonTechLossItem != undefined && lineLossFactor != undefined && hyperSproutLoss != undefined && hyperHubLoss != undefined) {
        objMeterData.TransformerData[strDateVal].Non_Technical_Loss =
            (objMeterData.TransformerData[strDateVal].Transformer_ActiveReceivedCumulativeRate_Total - (totalNonTechLossItem +
                objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total +
                (objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total * lineLossFactor) + hyperSproutLoss + hyperHubLoss))
        // if technical loss is not available for a transformer
    } else if (totalNonTechLossItem == undefined && lineLossFactor != undefined && hyperSproutLoss != undefined && hyperHubLoss != undefined) {
        objMeterData.TransformerData[strDateVal].Non_Technical_Loss =
            (objMeterData.TransformerData[strDateVal].Transformer_ActiveReceivedCumulativeRate_Total - (objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total +
                (objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total * lineLossFactor) + hyperSproutLoss + hyperHubLoss))

    }
}
/**
 * @description - Code to get all summary map transaction related data
 * @param   Nil
 * @return - callback
 */
function getAllSummaryMapTransactionRelatedDetails(objStartDate, callback) {
    async.parallel({
        meterdata: function (callback) {
            getAllSummaryMapRelatedDetailsMeter(callback);
        },
        transformerdata: function (callback) {
            getAllSummaryMapRelatedDetailsTransformer(callback);
        }
    }, function (err, results) {
        if (err) {
            callback(err, null);
            return;
        }
        try {
            if (results) {
                if (results.meterdata) {
                    objProcessData.processDataFor(results.meterdata, objStartDate, false);
                }
                if (results.transformerdata) {
                    objProcessData.processDataFor(results.transformerdata, objStartDate, true);
                }
                callback(null, results);
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}
/**
 * @description - Code to get summary map transformer related data
 * @return - callback
 */

function getAllSummaryMapRelatedDetailsTransformer(callback) {
    objdaoimpl.getCursorFromCollectionSorted("DELTA_Transaction_Data", ['result.Transformer.ReadTimestamp', 'result.Transformer.StatusTransformer'], [{ $gt: objStartDate }, "Connected"], null, {
        // 'result.meters': 0
        'result.CellID': 1,
        'result.Transformer.Line1Voltage': 1,
        'result.Transformer.Line2Voltage': 1,
        'result.Transformer.Line3Voltage': 1,
        'result.Transformer.Line1Current': 1,
        'result.Transformer.Line2Current': 1,
        'result.Transformer.Line3Current': 1,
        'result.Transformer.AmbientTemperarture': 1,
        'result.Transformer.TopTemperature': 1,
        'result.Transformer.BottomTemperature': 1,
        'result.Transformer.TransformerOilLevel': 1,
        'result.Transformer.ActiveReceivedCumulativeRate_Total': 1,
        'result.Transformer.ActiveDeliveredCumulativeRate_Total': 1,
        'result.Transformer.ReadTimestamp': 1,
        'result.Transformer.Line1PhaseAngle': 1,
        'result.Transformer.Line2PhaseAngle': 1,
        'result.Transformer.Line3PhaseAngle': 1,
        'result.Transformer.BatteryVoltage': 1,
        'result.Transformer.BatteryStatus': 1,
        'DBTimestamp': 1
    }, function (err, arrTransformerTransData) {
        objOuptTransTransactionData = {};
        if (arrTransformerTransData) {
            arrTransformerTransData.stream()
                .on('data', function (meterTranItem) {
                    processTransformerTransactionItem(meterTranItem);
                })
                .on('error', function (err) {
                    console.log('in trans error' + err);
                    callback(err, null);
                    arrTransformerTransData.close();
                })
                .on('end', function () {
                    callback(null, objOuptTransTransactionData);
                    arrTransformerTransData.close();
                });
        } else {
            console.log('in else case')
            callback(err, null);
        }

    });
}
/**
 * @description - Code to process transformer transaction 
 * @param meterTranItem - meter transaction data
 * @return Nil
 */
function processTransformerTransactionItem(meterTranItem) {
    try {
        var objMeterData = meterTranItem;
        var arrTransformerResultData = [];
        var strCellIdVal = null;
        if (objMeterData.result) {
            arrTransformerResultData = objMeterData.result.Transformer;
            strCellIdVal = objMeterData.result.CellID;
        }
        var objDBTimestampVal = objMeterData.DBTimestamp;
        var meterResultTranItem = arrTransformerResultData;
        var objRowData = {};

        processTransformerTransactionSkipItems(strCellIdVal, meterResultTranItem, objRowData);

        objRowData.Transformer_CellID = strCellIdVal;
        objRowData.MeterDBTimestampVal = new Date(objDBTimestampVal);
        objRowData.ActualTransformer_ReadTimestamp = objRowData.Transformer_ReadTimestamp ? objRowData.Transformer_ReadTimestamp : objRowData.MeterDBTimestampVal;
        //comment this for readtimestamp
        //objRowData.Transformer_ReadTimestamp = objDBTimestampVal;

        var strCellID = objRowData.Transformer_CellID;
        if (strCellID) {
            if (!objOuptTransTransactionData[strCellID]) {
                objOuptTransTransactionData[strCellID] = {};
            }
            if (!objOuptTransTransactionData[strCellID].TransLastData) {
                objOuptTransTransactionData[strCellID].TransLastData = [];
            }
            if (objRowData.Transformer_ReadTimestamp) {
                objRowData.Transformer_ReadTimestamp = objDBTimestampVal;
                var objMeterDateMoment = moment.utc(objRowData.Transformer_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss');
                var strTransformerDate = objMeterDateMoment.format('YYYY-MM-DD_HH');
                var strTransactionKey = strTransformerDate;
                updateDataToTransaction(objOuptTransTransactionData, strCellID, strTransactionKey, objRowData);
            }
        }

    } catch (err) {
        logger.log(err);
    }
}

/**
 * @description - Code to skip items for process transformer transaction  
 * @param strCellIdVal - cell id to be skipped
 * @param meterResultTranItem - meter transaction item
 * @param objRowData - row data
 * @return Nil
 */
function processTransformerTransactionSkipItems(strCellIdVal, meterResultTranItem, objRowData) {
    if (objConfig.cellIdToSkip.indexOf(strCellIdVal) === -1) {
        objjsFunctions.assignValuesFrmObject(meterResultTranItem, objRowData, "Transformer_", arrFilter, false, false);
    } else {
        objjsFunctions.assignValuesFrmObject(meterResultTranItem, objRowData, "Transformer_", arrFilter, false, true);
    }
}
/**
 * @description - Code to skip items for process transformer transaction  
 * @param strCellIdVal - cell id to be skipped
 * @param meterResultTranItem - meter transaction item
 * @param objRowData - row data
 * @return Nil
 */
function updateDataToTransaction(objOuptTransTransactionData, strCellID, strTransactionKey, objRowData) {
    if (!objOuptTransTransactionData[strCellID].TranData) {
        objOuptTransTransactionData[strCellID].TranData = {};
    }
    if (!objOuptTransTransactionData[strCellID].TranData[strTransactionKey]) {
        objOuptTransTransactionData[strCellID].TranData[strTransactionKey] = [];
    }
    objOuptTransTransactionData[strCellID].TranData[strTransactionKey].push(objRowData);
    objOuptTransTransactionData[strCellID].TransLastData = objRowData;
}
/**
 * @description - Code to get all summary map related meter data
 * @return callback
 */
function getAllSummaryMapRelatedDetailsMeter(callback) {
    objdaoimpl.getCursorFromCollectionSorted("DELTA_Transaction_Data", ['result.meters.ReadTimestamp', 'result.meters.Status'], [{ $gt: objStartDate }, ["Connected", COMMONFAULT_TEXT, PhaseFault3]], null, {
        'result.CellID': 1,
        //'result.Transformer': 0,
        'result.meters.DeviceID': 1,
        'result.meters.ActiveReceivedCumulativeRate_Total': 1,
        "result.meters.ActiveDeliveredCumulativeRate_Total": 1,
        "result.meters.ApparentReceivedCumulativeRate_Total": 1,
        "result.meters.Apparent_m_Total": 1,
        "result.meters.Line1InstVoltage": 1,
        "result.meters.Line2InstVoltage": 1,
        "result.meters.Line3InstVoltage": 1,
        "result.meters.Line1InstCurrent": 1,
        "result.meters.Line2InstCurrent": 1,
        "result.meters.Line3InstCurrent": 1,
        "result.meters.Line1Frequency": 1,
        "result.meters.Line2Frequency": 1,
        "result.meters.Line3Frequency": 1,
        "result.meters.ReadTimestamp": 1,
        "result.meters.Status": 1,
        "result.meters.Line1PowerFactor": 1,
        "result.meters.Line2PowerFactor": 1,
        "result.meters.Line3PowerFactor": 1,
        "result.meters.ParentHS": 1,
        "result.meters.SelfHeal": 1,
        "DBTimestamp": 1
    }, function (err, objMeterTransDataCursor) {
        objOuptMeterTransactionData = {};
        if (objMeterTransDataCursor) {
            objMeterTransDataCursor.stream()
                .on('data', function (meterTranItem) {
                    processMeterTransactionItem(meterTranItem);
                })
                .on('error', function (err) {
                    console.log('errr this is the err:', err);
                    callback(err, null);
                    objMeterTransDataCursor.close();
                })
                .on('end', function () {
                    callback(null, objOuptMeterTransactionData);
                    objMeterTransDataCursor.close();
                });
        } else {
            console.log('in else meter')
            callback(err, null);
        }
    });
}
/**
 * @description - Code to process meter transaction item  
 * @param meterTranItem - meter transaction item
 * @return Nil
 */
function processMeterTransactionItem(meterTranItem) {
    try {
        var objMeterData = meterTranItem;
        var arrMeterResultData = [];
        var strCellIdVal = null;
        if (objMeterData.result) {
            arrMeterResultData = objMeterData.result.meters;
            strCellIdVal = objMeterData.result.CellID;
        }
        var objDBTimestampVal = objMeterData.DBTimestamp;

        if (!arrMeterResultData) {
            return;
        }

        for (var i = 0; i < arrMeterResultData.length; i++) {
            processMeterTransactionRecord(i, arrMeterResultData, objOuptMeterTransactionData, strCellIdVal, objDBTimestampVal);
        }
    } catch (err) {
        logger.log(err);
    }
}
// function processMeterTransactionItem(meterTranItem) {
//     try {
//         var objMeterData = meterTranItem;
//         var arrMeterResultData = [];
//         var strCellIdVal = null;
//         let selfheal = {};
//         if (objMeterData.result) {
//             arrMeterResultData = objMeterData.result.meters;
//         }
//         var objDBTimestampVal = objMeterData.DBTimestamp;

//         if (!arrMeterResultData) {
//             return;
//         }
//         for (var i = 0; i < arrMeterResultData.length; i++) {
//             if (arrMeterResultData[i].SelfHeal == 1 && arrMeterResultData[i].SelfHeal !== undefined && arrMeterResultData[i].ParentHS !== undefined) {
//                 strCellIdVal = arrMeterResultData[i].ParentHS;
//                 selfheal.strCellIdVal = 1;
//             } else {
//                 strCellIdVal = objMeterData.result.CellID;
//                 selfheal.strCellIdVal = 0;
//             }
//             processMeterTransactionRecord(i, arrMeterResultData, objOuptMeterTransactionData, strCellIdVal, objDBTimestampVal);
//         }
//     } catch (err) {
//         logger.log(err);
//     }
// }
/**
 * @description - Code to process meter transaction record
 * @param i - index
 * @param arrMeterResultData - meter result data
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strCellIdVal - cell id
 * @param objDBTimestampVal- timestamp
 * @return Nil
 */
function processMeterTransactionRecord(i, arrMeterResultData, objOuptMeterTransactionData, strCellIdVal, objDBTimestampVal) {
    var meterResultTranItem = arrMeterResultData[i];
    var objRowData = {};
    assignValuesFrmObject(meterResultTranItem, objRowData, "Meter_", arrMeterFilter);
    objRowData.Meter_CellID = strCellIdVal;
    objRowData.Meter_NetworkResponseRate = 1;
    if (objRowData.Meter_Status === COMMONFAULT_TEXT || objRowData.Meter_Status === PhaseFault3) {
        objRowData.Meter_NetworkResponseRate = 0;
    }
    objRowData.MeterDBTimestampVal = new Date(objDBTimestampVal);
    var strCellID = objRowData.Meter_CellID;
    objRowData.ActualMeter_ReadTimestamp = objRowData.Meter_ReadTimestamp;

    if (!strCellID) {
        return;
    }

    if (!objOuptMeterTransactionData[strCellID]) {
        objOuptMeterTransactionData[strCellID] = {};
    }
    if (objRowData.Meter_Status === "Connected" || objRowData.Meter_Status === COMMONFAULT_TEXT || objRowData.Meter_Status === PhaseFault3) {
        updateMeterTransactionData(objOuptMeterTransactionData, strCellID, objRowData, objDBTimestampVal);
    }
}
/**
 * @description - Code to update meter transaction record
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strCellID - cell id
 * @param objRowData - row data
 * @param objDBTimestampVal- timestamp
 * @return Nil
 */
function updateMeterTransactionData(objOuptMeterTransactionData, strCellID, objRowData, objDBTimestampVal) {
    //comment this for read
    //objRowData.Meter_ReadTimestamp = objDBTimestampVal;
    var objMeterDateMoment = moment.utc(objRowData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss');
    var strMeterDate = objMeterDateMoment.format('YYYY-MM-DD_HH');
    var strTransactionKey = strMeterDate;
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID]) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID] = {};
    }
    if (objRowData.Meter_Status === "Connected") {
        updateMeterConnectedTransactionData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData);
    }
    if (objRowData.Meter_Status === COMMONFAULT_TEXT || objRowData.Meter_Status === PhaseFault3) {
        updateMeterFaultData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData);
    }
}
/**
 * @description - Code to update connected meter transaction record
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strTransactionKey - transaction key
 * @param objRowData - row data
 * @return Nil
 */
function updateMeterConnectedTransactionData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData) {
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData = {};
    }

    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].MeterLastData) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].MeterLastData = {};
    }
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData[strTransactionKey]) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData[strTransactionKey] = [];
    }
    objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData[strTransactionKey].push(objRowData);
    objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].MeterLastData = objRowData;
}
/**
 * @description - Code to update meter fault data
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strCellID - cell id
 * @param strTransactionKey - transaction key
 * @param objRowData - row data
 * @return Nil
 */
function updateMeterFaultData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData) {
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData = {};
    }
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData[strTransactionKey]) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData[strTransactionKey] = [];
    }
    objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData[strTransactionKey].push(objRowData);
}
/**
 * @description - Code to assign value
 * @param objInput - input data
 * @param objOutput - output data 
 * @param keyPrefix - key prefix
 * @param arrFilter - array of filter
 * @param isSkipValues - skip values
 * @return Nil
 */
function assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, isSkipValues) {
    objjsFunctions.assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, false, isSkipValues);
}
/**
 * @description - Code to merge keys
 * @param objInputData - input data
 * @return Nil
 */
function mergeKeys(objInputData) {
    try {
        for (var strPropertyKey in objInputData.transformerdata) {
            if (objInputData.transformerdata.hasOwnProperty(strPropertyKey) && !objInputData.meterdata[strPropertyKey]) {
                objProcessedManagerialData.createDummyMeterObj(objInputData, strPropertyKey);
            }
        }
    } catch (err) {
        logger.log(err);
    }
}
/**
 * @description - Code to merge data
 * @param objInputData - input data
 * @return Nil
 */
var mergeData = async function (objInputData) {

    try {
        let objTransformerData;
        for (let strPropertyKey in objInputData.meterdata) {
            if (!objInputData.transformerdata.hasOwnProperty(strPropertyKey)) {
                let cellID = parseInt(strPropertyKey);
                var input = {}
                assignValuesFrmObject(objInputData, input);
                if (!input.transformerdata[cellID]) {
                    input.transformerdata[cellID] = {};
                }
                if (!input.transformerdata[cellID].TransLastData) {
                    input.transformerdata[cellID].TransLastData = {};
                }
                if (!input.transformerdata[cellID].TranData) {
                    input.transformerdata[cellID].TranData = {};
                }
                if (!input.transformerdata[cellID].managerialdata) {
                    input.transformerdata[cellID].managerialdata = {};
                }
                objTransformerData = input.transformerdata[cellID];
                if (!input.meterdata[strPropertyKey]) {
                    objProcessedManagerialData.createDummyMeterObj(input, strPropertyKey);
                }
                loopAndMergeMeterData(input, objTransformerData, cellID)
            } else {
                if (objInputData.transformerdata.hasOwnProperty(strPropertyKey)) {
                    objTransformerData = objInputData.transformerdata[strPropertyKey];
                    if (!objInputData.meterdata[strPropertyKey]) {
                        objProcessedManagerialData.createDummyMeterObj(objInputData, strPropertyKey);
                    }
                    loopAndMergeMeterData(objInputData, objTransformerData, strPropertyKey)
                }
            }
        }
    } catch (err) {
        logger.log("Error while merge data", err);
    }
}

var getSelfHealdTrandata = async function (selfheatTransdata, latestSelfheal, cellID, objInputData, smstat1) {
    objInputData = objInputData;
    return new Promise((resolve, reject) => {
        dbConMysql.pool.getConnection(function (err, connection) {
            if (err) {
                console.log(err);
                reject(err)
            } else {
                var sql = "select * from summarymap where `Transformer_CellID` ='" + cellID + "' order by `updatedAt` desc limit 1;"
                connection.query(sql, function (err, summarymap) {
                    if (err) {
                        console.log('err summarymap--->');
                        console.log(err);
                        reject(err)
                    } else {
                        //transData
                        if (typeof summarymap !== undefined && summarymap.length > 0) {
                            selfheatTransdata.Transformer_ReadTimestamp = summarymap[0]['Transformer_ReadTimestamp'];
                            selfheatTransdata.ActualTransformer_ReadTimestamp = summarymap[0]['ActualTransformer_ReadTimestamp'];
                            selfheatTransdata.Transformer_Line1Voltage = summarymap[0]['Transformer_Line1Voltage'];
                            selfheatTransdata.Transformer_Line2Voltage = summarymap[0]['Transformer_Line2Voltage'];
                            selfheatTransdata.Transformer_Line3Voltage = summarymap[0]['Transformer_Line3Voltage'];
                            selfheatTransdata.Transformer_Line1Current = summarymap[0]['Transformer_Line1Current'];
                            selfheatTransdata.Transformer_Line2Current = summarymap[0]['Transformer_Line2Current'];
                            selfheatTransdata.Transformer_Line3Current = summarymap[0]['Transformer_Line3Current'];
                            selfheatTransdata.Transformer_AmbientTemperarture = summarymap[0]['Transformer_AmbientTemperarture'];
                            selfheatTransdata.Transformer_TopTemperature = summarymap[0]['Transformer_TopTemperature'];
                            selfheatTransdata.Transformer_BottomTemperature = summarymap[0]['Transformer_BottomTemperature'];
                            selfheatTransdata.Transformer_Line1PhaseAngle = summarymap[0]['Transformer_Line1PhaseAngle'];
                            selfheatTransdata.Transformer_Line3PhaseAngle = summarymap[0]['Transformer_Line3PhaseAngle'];
                            selfheatTransdata.Transformer_BatteryVoltage = summarymap[0]['Transformer_BatteryVoltage'];
                            selfheatTransdata.Transformer_BatteryStatus = summarymap[0]['Transformer_BatteryStatus'];
                            selfheatTransdata.Transformer_TransformerOilLevel = summarymap[0]['Transformer_TransformerOilLevel'];
                            selfheatTransdata.Transformer_Apparent_m_Total = summarymap[0]['Transformer_Apparent_m_Total'];
                            selfheatTransdata.Transformer_ActiveReceivedCumulativeRate_Total = summarymap[0]['Transformer_ActiveReceivedCumulativeRate_Total'];
                            selfheatTransdata.Transformer_ActiveDeliveredCumulativeRate_Total = summarymap[0]['Transformer_ActiveDeliveredCumulativeRate_Total'];
                            latestSelfheal = Object.assign(latestSelfheal, selfheatTransdata);
                            latestSelfheal.Transformer_CellID = summarymap[0]['Transformer_CellID']
                            latestSelfheal.MeterDBTimestampVal = summarymap[0]['Transformer_ReadTimestamp']
                            resolve(smstat1 = 1)
                        } else {
                            console.log("***********************************EMPTY ARRAY FROM DB****************************")
                            resolve(smstat1 = 1)
                        }

                    }

                })
            }
        })
    });
}
/**
 * @description - Code to loop over meter data and merge
 * @param objInputData - input data
 * @param objTransformerData - transformer data
 * @param strPropertyKey - property key
 * @return Nil
 */
function loopAndMergeMeterData(objInputData, objTransformerData, strPropertyKey) {
    var objMeterData;
    for (var strMeterIdVal in objInputData.meterdata[strPropertyKey]) {
        if (objInputData.meterdata[strPropertyKey].hasOwnProperty(strMeterIdVal)) {
            objMeterData = objInputData.meterdata[strPropertyKey][strMeterIdVal];
            if (!objMeterData) {
                continue;
            }
            loopTransformerAndMergeDataToMeter(objInputData, objTransformerData, objMeterData, strPropertyKey);
        }
    }
}
/**
 * @description - Code to loop over transformer data and merge
 * @param objInputData - input data
 * @param objTransformerData - transformer data
 * @param objMeterData - meter data
 * @param strPropertyKey - property key
 * @return Nil
 */
function loopTransformerAndMergeDataToMeter(objInputData, objTransformerData, objMeterData, strPropertyKey) {

    for (var strDatePropertyKey in objTransformerData) {
        if (objMeterData[strDatePropertyKey]) {
            if (strDatePropertyKey === 'TranData') {
                objInputData.meterdata[strPropertyKey].TransformerData = objTransformerData[strDatePropertyKey];
            } else {
                assignValuesFrmObject(objTransformerData[strDatePropertyKey], objMeterData[strDatePropertyKey]);

            }
        } else {
            objMeterData[strDatePropertyKey] = objTransformerData[strDatePropertyKey];

        }
    }
    assignValuesFrmObject(objTransformerData.TransLastData, objMeterData.MeterLastData);

}

module.exports = {
    getAllSummaryMapRelatedDetails: getAllSummaryMapRelatedDetails
};
