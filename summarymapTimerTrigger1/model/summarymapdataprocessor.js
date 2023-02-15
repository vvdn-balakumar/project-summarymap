var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var moment = require("moment");
var objConfig = require('../config.js');
var objCalculations = require('../util/calculations.js');
var objjsFunctions = require('../util/jsfunctions.js');
var logger = console;

function assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, isSkipValues) {
    objjsFunctions.assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, false, isSkipValues);
}

/**
    * @description - Code to process data 
    * @param objInput - input data
    * @param isTransformer - is transformer or not
    * @return Nil
    */
function processDataFor(objInput, objStartDate, isTransformer) {
    objdaoimpl.getDataFromCollection("DELTA_SystemSettings", ["Settings", "Type.Status"], ["Communications", "Updated"], function (err, transactionQueries) {
        if (err) {
            logger.log(err);
        } else {
            try {
                let poolingInterval = transactionQueries[0].Type.Values.HypersproutTransactionPoolingInterval;
                for (var devicePropertyKey in objInput) {
                    if (!objInput.hasOwnProperty(devicePropertyKey)) {
                        continue;
                    }
                    var objDeviceData = objInput[devicePropertyKey];
                    if (isTransformer) {
                        processDataForTransformer(objInput, objStartDate, objDeviceData, devicePropertyKey);
                    } else {
                        processDataForMeter(objInput, objStartDate, objDeviceData, devicePropertyKey,poolingInterval);
                    }
                }
            } catch (err) {
                logger.log("Error while processing data ", err);
            }
        }
    });
}
/**
    * @description - Code to process data for transformer
    * @param objInput - input data
    * @param objDeviceData - device data
    * @param devicePropertyKey - device property key
    * @return Nil
    */
   function processDataForTransformer(objInput, objStartDate, objDeviceData, devicePropertyKey) {
    var objDeviceTransactionData, propertyKey, objInnerData, objFormattedData, i, j, arrKeys, objPreviousInnerData;
    objDeviceTransactionData = objDeviceData.TranData;
    if (!objInput[devicePropertyKey].managerialdata) {
        objInput[devicePropertyKey].managerialdata = {};
    }
    var objMeterDateMoment = moment.utc(objStartDate, 'DD-MM-YYYY HH:mm:ss');
    var PreviousDateval = objMeterDateMoment.format('YYYY-MM-DD_HH');
    arrKeys = Object.keys(objDeviceTransactionData).sort();
    objPreviousInnerData = null;
    for (j = 0; j < arrKeys.length; j++) {
        propertyKey = arrKeys[j];
        if (objDeviceTransactionData.hasOwnProperty(propertyKey)) {
            var nonZeroInputArray = [];
            var nonZeroPreviousArray = [];
            objInnerDataFirst = objDeviceTransactionData[propertyKey];
            if (Object.keys(objInnerDataFirst).length > 0) {
                for (x = 0; x < objInnerDataFirst.length; x++) {
                    if (objInnerDataFirst[x].Transformer_ActiveReceivedCumulativeRate_Total > 0) {
                        nonZeroInputArray.push(objInnerDataFirst[x]);
                    }
                }
                objInnerData = nonZeroInputArray;
            }
            if (Object.keys(objInnerData).length > 0) {

                objFormattedData = {};

                for (i = 0; i < objInnerData.length; i++) {
                    objCalculations.processValues(objInnerData, i, objInput[devicePropertyKey].managerialdata, objConfig.transfomer_sm_arrNoProccessKey, propertyKey);
                    objCalculations.processArrTimeStampValues(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrTimestpampKeys, propertyKey);
                    objCalculations.processAverage(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrAverageKeys);
                    objCalculations.processLastValue(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrLastValKeys, propertyKey);
                    objCalculations.processSum(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrSumKeys);
                    if (j == 0 && (propertyKey == PreviousDateval)) {
                        objFormattedData = {};
                    }
                    objCalculations.processDifference(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrDifffernceSumKeys, objPreviousInnerData, propertyKey);
                }
                objPreviousInnerData = {};
                objjsFunctions.assignValuesFrmObject(objInput[devicePropertyKey].TranData, objPreviousInnerData, "", [propertyKey], false, false);
                objPreviousInnerData = objPreviousInnerData[propertyKey];
                if (Object.keys(objPreviousInnerData).length > 0) {
                    for (y = 0; y < objPreviousInnerData.length; y++) {
                        if (objPreviousInnerData[y].Transformer_ActiveReceivedCumulativeRate_Total > 0) {
                            nonZeroPreviousArray.push(objPreviousInnerData[y]);
                        }
                    }
                    if (Object.keys(nonZeroPreviousArray).length > 0) {
                        objPreviousInnerData = nonZeroPreviousArray;
                    } else {
                        objPreviousInnerData = null;
                    }
                   
                }
                objInput[devicePropertyKey].TranData[propertyKey] = objFormattedData;
                if (j == 0 && (propertyKey == PreviousDateval)) {
                    delete objInput[devicePropertyKey].TranData[propertyKey]
                }
            }else{
                continue;
            }

        }
    }
}
/**
    * @description - Code to process data for meter
    * @param objInput - input data
    * @param objDeviceData - device data
    * @param devicePropertyKey - device property key
    * @return Nil
    */
function processDataForMeter(objInput, objStartDate, objDeviceData, devicePropertyKey, poolingInterval) {
    
    for (var strMeterid in objDeviceData) {
        if (!objDeviceData[strMeterid].TranData && objDeviceData[strMeterid].FaultTranData) {
            objDeviceData[strMeterid].TranData = objDeviceData[strMeterid].FaultTranData;
            delete objDeviceData[strMeterid].FaultTranData;
        }
        if (objDeviceData.hasOwnProperty(strMeterid) && objDeviceData[strMeterid].TranData) {
            var objFaultTranData = objDeviceData[strMeterid].FaultTranData ? objDeviceData[strMeterid].FaultTranData : null;
            if (!objInput[devicePropertyKey][strMeterid].managerialdata) {
                objInput[devicePropertyKey][strMeterid].managerialdata = {};
            }
            loopThroughMeterAndCalculate(objInput, objStartDate, objDeviceData, devicePropertyKey, objFaultTranData, strMeterid, poolingInterval);
        }
    }
}
/**
    * @description - Code to loop through meter and calculate
    * @param objInput - input data
    * @param objDeviceData - device data
    * @param devicePropertyKey - device property key
    * @param objFaultTranData - Fault Transaction data
    * @param strMeterid - meter id
    * @param poolingInterval -- Passed : For NRR Changes
    * @return Nil
    */
   function loopThroughMeterAndCalculate(objInput, objStartDate, objDeviceData, devicePropertyKey, objFaultTranData, strMeterid, poolingInterval) {    
    var propertyKey, objInnerData, objFormattedData, i, j;

    var objDeviceTransactionData = objDeviceData[strMeterid].TranData;
    var arrKeys = Object.keys(objDeviceTransactionData).sort();
    var objPreviousInnerData = null;
    var objMeterDateMoment = moment.utc(objStartDate, 'DD-MM-YYYY HH:mm:ss');
    var PreviousDateval = objMeterDateMoment.format('YYYY-MM-DD_HH');
    for (j = 0; j < arrKeys.length; j++) {
        // Network Response Rate Changes for Current Hour-- Start
        if (j == arrKeys.length - 1) {
            let transScheduler = new Date().getUTCMinutes() + 1;
            objConfig.transScheduler = Math.ceil(transScheduler / poolingInterval);
        }
        else {
            objConfig.transScheduler = 60 / poolingInterval;
        }
        propertyKey = arrKeys[j];
        if (!objDeviceTransactionData.hasOwnProperty(propertyKey)) {
            continue;
        }
        var nonZeroInputArray = [];
        var nonZeroPreviousArray = [];
        objInnerDataFirst = objDeviceTransactionData[propertyKey];
        if (Object.keys(objInnerDataFirst).length > 0) {
            for (x = 0; x < objInnerDataFirst.length; x++) {
                if (objInnerDataFirst[x].Meter_ActiveReceivedCumulativeRate_Total > 0) {
                    nonZeroInputArray.push(objInnerDataFirst[x]);
                }
            }
            objInnerData = {};
            objInnerData = nonZeroInputArray;
        }
        objFormattedData = {};
        if (Object.keys(objInnerData).length > 0) {
            for (i = 0; i < objInnerData.length; i++) {
                objCalculations.processValues(objInnerData, i, objInput[devicePropertyKey][strMeterid].managerialdata, objConfig.meter_sm_arrNoProccessKey, propertyKey);
                objCalculations.processArrTimeStampValues(objInnerData, i, objFormattedData, objConfig.meter_sm_arrTimestpampKeys, propertyKey);
                objCalculations.processAverage(objInnerData, i, objFormattedData, objConfig.meter_sm_arrAverageKeys);
                objCalculations.processLastValue(objInnerData, i, objFormattedData, objConfig.meter_sm_arrLastValKeys, propertyKey);
                objCalculations.processSum(objInnerData, i, objFormattedData, objConfig.meter_sm_arrSumKeys);
                calculateNetworkResponseRate(objInnerData, i, objFormattedData, ['Meter_NetworkResponseRate']);
                if (j == 0 && (propertyKey == PreviousDateval)) {
                    objFormattedData = {};
                }
                objCalculations.processDifference(objInnerData, i, objFormattedData, objConfig.meter_sm_arrDifffernceSumKeys, objPreviousInnerData, propertyKey);
            }
            objPreviousInnerData = {};
            objjsFunctions.assignValuesFrmObject(objInput[devicePropertyKey][strMeterid].TranData, objPreviousInnerData, "", [propertyKey], false, false);
            objPreviousInnerData = objPreviousInnerData[propertyKey];
            if (Object.keys(objPreviousInnerData).length > 0) {
                for (y = 0; y < objPreviousInnerData.length; y++) {
                    if (objPreviousInnerData[y].Meter_ActiveReceivedCumulativeRate_Total > 0) {
                        nonZeroPreviousArray.push(objPreviousInnerData[y]);
                    }
                }
                if (Object.keys(nonZeroPreviousArray).length > 0) {
                    objPreviousInnerData = nonZeroPreviousArray;
                } else {
                    objPreviousInnerData = null;
                }
            }
            if (objFaultTranData && objFaultTranData[propertyKey]) {
                var objDataToCalNetworkResRate;
                objDataToCalNetworkResRate = objFaultTranData[propertyKey].concat(objDeviceTransactionData[propertyKey]);
                objFormattedData.Meter_NetworkResponseRate = 0;
                for (i = 0; i < objDataToCalNetworkResRate.length; i++) {
                    calculateNetworkResponseRate(objDataToCalNetworkResRate, i, objFormattedData, ['Meter_NetworkResponseRate']);
                }
            }
            objInput[devicePropertyKey][strMeterid].TranData[propertyKey] = objFormattedData;
            if (j == 0 && (propertyKey == PreviousDateval)) {
                delete objInput[devicePropertyKey][strMeterid].TranData[propertyKey]
            }
        } else {
            continue;
        }
    }
}


function calculateNetworkResponseRate(objInputData, index, objFormattedData, arrAverageKeys) {
    objCalculations.processAverage(objInputData, index, objFormattedData, arrAverageKeys, objConfig.transScheduler);
}

module.exports = {
    processDataFor: processDataFor
};