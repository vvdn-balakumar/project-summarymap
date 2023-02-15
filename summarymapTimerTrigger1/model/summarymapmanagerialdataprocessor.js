var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var objjsFunctions = require('../util/jsfunctions.js');
var logger = console;

/**
    * @description - Code to get Transformer data by Cell ID
    * @param objRowData - row data
    * @return callback
    */
function getTransformerDataByCellID(objRowData, callback) {
   // console.log('3reached inside trannsformer data----------------------------->'+objRowData.TransformerID)
    objRowData = objRowData ? objRowData : {};
    objdaoimpl.getDataFromCollection("DELTA_Transformer", ['TransformerID'], [objRowData.TransformerID], function (err, objSelTransformerData) {
        if (err) {
            callback(err, null);
        } else {
            try {
                if (objSelTransformerData && objSelTransformerData.length > 0) {
                    objRowData.TransformerID = objSelTransformerData[0].TransformerID;
                    objRowData.TransformerSerialNumber = objSelTransformerData[0].TransformerSerialNumber;
                    objRowData.NoOfHyperHubAllocated = objSelTransformerData[0].NoOfHyperHubAllocated;

                    //adding technical items details in managerial data
                    getNonTechnicalLossItems(objSelTransformerData[0].TransformerSerialNumber, function (err, technicalItems, lineLossFactor, hyperSproutLoss, hyperHubLoss) {

                        objRowData.TechnicalItems = technicalItems;
                        objRowData.LineLossFactor = lineLossFactor;
                        objRowData.HyperSproutLoss = hyperSproutLoss;
                        objRowData.HyperHubLoss = hyperHubLoss;

                        getCircuitByCircuitID(objSelTransformerData[0].CircuitID, function (err, objCircuitData) {
                            assignValuesFrmObject(objCircuitData, objRowData);
                            callback(null, objRowData);
                        });
                    });

                } else {
                    //console.log('3 This is the else condition 5-------------------+'+objRowData.TransformerID)
                    callback(null, objRowData);
                }
            } catch (exc) {
                callback(exc, null);
            }
        }
    });
}

function getNonTechnicalLossItems(transformerSerialNo, callback) {

    //fetch LineLossFactor, HyperSproutLoss, HyperHubLoss
    objdaoimpl.getDataFromCollectionSorted("DELTA_SystemSettings", ['Type.Status', 'Settings'], ['Updated', 'Communications'], null,
        {
            'Type.Values.LineLossFactor': 1,
            'Type.Values.HyperSproutLoss': 1,
            'Type.Values.HyperHubLoss': 1
        }, function (err, systemSett) {
            if (err) {
                callback(err, null);
            } else {

                var lineLossFactor = systemSett[0].Type.Values.LineLossFactor ? systemSett[0].Type.Values.LineLossFactor : 0;
                var hyperSproutLoss = systemSett[0].Type.Values.HyperSproutLoss ? systemSett[0].Type.Values.HyperSproutLoss : 0;
                var hyperHubLoss = systemSett[0].Type.Values.HyperHubLoss ? systemSett[0].Type.Values.HyperHubLoss : 0;

                objdaoimpl.getDataFromCollectionSorted("DELTA_Transformer_Tech_Item", ['TransformerSerialNo'], [transformerSerialNo], null, {
                    'TransformerSerialNo': 1,
                    'Metered': 1,
                    'UsagePerDay': 1,
                    'NoOfConnectedItems': 1,
                    'UsageTime': 1,
                    'StartHour': 1,
                    'EndHour': 1
                }
                    , function (err, technicalItems) {
                        if (err) {
                            callback(err, null);
                        } else {
                            if (technicalItems.length > 0) {
                                var technicalItemsArr = [];

                                for (var k = 0; k < technicalItems.length; k++) {
                                    technicalItems[k].OperationalHours = [];
                                    // if technical item is not metered than only calculate 
                                    if (!technicalItems[k].Metered) {
                                        // if custom then add operational hours accordinlgy
                                        if (technicalItems[k].UsageTime == 'custom') {
                                            if (parseInt(technicalItems[k].StartHour) < parseInt(technicalItems[k].EndHour)) {
                                                for (var i = parseInt(technicalItems[k].StartHour); i <= parseInt(technicalItems[k].EndHour); i++) {
                                                    technicalItems[k].OperationalHours.push(i);
                                                }
                                            }
                                            else if (parseInt(technicalItems[k].EndHour) < parseInt(technicalItems[k].StartHour)) {
                                                for (var i = parseInt(technicalItems[k].StartHour); i < 24; i++) {
                                                    technicalItems[k].OperationalHours.push(i);
                                                }
                                                for (var j = 0; j <= parseInt(technicalItems[k].EndHour); j++) {
                                                    technicalItems[k].OperationalHours.push(j);
                                                }
                                            } else {
                                                technicalItems[k].OperationalHours.push(technicalItems[k].StartHour);
                                            }
                                            // if allDay then add all hours in operational hours array
                                        } else if (technicalItems[k].UsageTime == 'allDay') {
                                            for (var i = 0; i <= 23; i++) {
                                                technicalItems[k].OperationalHours.push(i);
                                            }
                                        }
                                        // delete the extra keys
                                        delete technicalItems[k]._id;
                                        delete technicalItems[k].Metered;
                                        delete technicalItems[k].StartHour;
                                        delete technicalItems[k].EndHour;

                                        technicalItemsArr.push(technicalItems[k]);
                                    }
                                    if (k == technicalItems.length - 1) {
                                        callback(null, technicalItemsArr, lineLossFactor, hyperSproutLoss, hyperHubLoss);
                                    }
                                }
                            } else {
                                callback(null, [], lineLossFactor, hyperSproutLoss, hyperHubLoss);
                            }
                        }
                    });
            }
        });
}

/**
    * @description - Code to get circuit data by circuit ID
    * @param circuitIdVal - circuit id
    * @return callback
    */
function getCircuitByCircuitID(circuitIdVal, callback) {
    var objRowData = {};
    objdaoimpl.getDataFromCollection("DELTA_Circuit", ['CircuitID'], [circuitIdVal], function (err, objSelCircuitData) {
        if (err) {
            callback(err, null);
        } else {
            try {
                objRowData.CircuitID = -1;
                if (objSelCircuitData && objSelCircuitData.length > 0) {
                    objRowData.CircuitID = objSelCircuitData[0].CircuitID;
                    objRowData.CircuitLatitude = objSelCircuitData[0].Latitude;
                    objRowData.CircuitLongitude = objSelCircuitData[0].Longitude;
                }

                callback(null, objRowData);
            } catch (exc) {
                logger.log(exc);
                callback(exc, null);
            }
        }
    });
}
/**
    * @description - Code to get hypersprout data by cell ID
    * @param cellIdVal - cell id
    * @return callback
    */
function getHypersproutDataByCellID(cellIdVal, callback) {
    try {
        let objRowData = {};
        objRowData.cellIdVal = cellIdVal;
        objdaoimpl.getDataFromCollection("DELTA_Hypersprouts", ['HypersproutID'], [cellIdVal], function (err, objSelTransformerData) {
            if (err) {
                console.log(err)
                callback(err, null);
                return;
            }
            try {
                processHypersproutDataByCellID(objRowData, objSelTransformerData, callback);
            } catch (exc) {
                callback(exc, null);
            }
        });
    } catch (err) {
        console.log(err)
        callback(err, null);
    }
}
/**
    * @description - Code to get process hypersprout data 
    * @param objRowData - row data
    * @param objSelTransformerData - Transformer data
    * @return callback
    */

function processHypersproutDataByCellID(objRowData, objSelTransformerData, callback) {
    objRowData.TransformerID = -1;
    if (!objSelTransformerData && objSelTransformerData.length < 1) {
        console.log('In no hs data-------------------------------------?'+objRowData.cellIdVal);
        return;
    }
    objRowData.HypersproutSerialNumber = objSelTransformerData[0].HypersproutSerialNumber;
    var objHypersproutComm = objSelTransformerData[0].Hypersprout_Communications;
    if (objHypersproutComm) {
        objRowData.TransformerLatitude = objHypersproutComm.Latitude;
        objRowData.TransformerLongitude = objHypersproutComm.Longitude;
    }
    var objHypersproutDevDetails = objSelTransformerData[0].Hypersprout_DeviceDetails;
    if (objHypersproutDevDetails) {
        objRowData.Transformer_Phase = objHypersproutDevDetails.Phase;
        objRowData.TransformerRating = objHypersproutDevDetails.TransformerRating;
    }
    objRowData.IsHyperHub = objSelTransformerData[0].IsHyperHub;
    objRowData.TransformerID = objSelTransformerData[0].TransformerID;
    getTransformerDataByCellID(objRowData, callback);
}
/**
    * @description - Code to get meter data by meter ID
    * @param cellIdVal - cell id
    * @param meterIdVal - meter id
    * @return callback
    */
function getMeterByMeterID(cellIdVal, meterIdVal, callback) {
   //console.log('Reached inside meter----cellid value is >'+cellIdVal+" The meter id value is --->"+meterIdVal);
    var objRowData = {};
    objdaoimpl.getDataFromCollection("DELTA_Meters", ['MeterID'], [meterIdVal], function (err, objSelMeterData) {
        if (err) {
            callback(err, null);
        } else {
            try {
                if (objSelMeterData && objSelMeterData.length > 0) {
                    for (var i = 0; i < objSelMeterData.length; i++) {
                        objRowData[objSelMeterData[i].MeterID] = {};
                        objRowData[objSelMeterData[i].MeterID].cellId = cellIdVal;
                         objRowData[objSelMeterData[i].MeterID].Meter_DeviceID = objSelMeterData[i].MeterID;
                        objRowData[objSelMeterData[i].MeterID].MeterSerialNumber = objSelMeterData[i].MeterSerialNumber;
                        var objMeterComm = objSelMeterData[i].Meters_Communications;
                        if (objMeterComm) {
                            objRowData[objSelMeterData[i].MeterID].MeterLatitude = objMeterComm.Latitude;
                            objRowData[objSelMeterData[i].MeterID].MeterLongitude = objMeterComm.Longitude;
                        }
                        var objMeterDeviceDetails = objSelMeterData[i].Meters_DeviceDetails;
                        if (objMeterDeviceDetails) {
                            objRowData[objSelMeterData[i].MeterID].Meter_Phase = objMeterDeviceDetails.Phase;
                           objRowData[objSelMeterData[i].MeterID].Meter_CTRatio = objMeterDeviceDetails.CT_Ratio;
                        }
                        objSelMeterData[i].SolarPanel = objSelMeterData[i].SolarPanel;
                        objRowData[objSelMeterData[i].MeterID].SolarPanel = objSelMeterData[i].SolarPanel ? true : false;
                        objSelMeterData[i].EVMeter = objSelMeterData[i].EVMeter;
                        objRowData[objSelMeterData[i].MeterID].EVMeter = objSelMeterData[i].EVMeter ? true : false;
                    }
                }
                callback(null, objRowData);
            } catch (exc) {
                logger.log(exc);
                callback(exc, null);
            }
        }
    });
}
/**
    * @description - Code to assign value
    * @param objInput - input data
    * @param objOutput - output data
    * @param keyPrefix - key prefix
    * @param arrFilter - array of filter
    * @param isSkipValues - skip values
    * @return callback
    */
function assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, isSkipValues) {
    objjsFunctions.assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, false, isSkipValues);
}
/**
    * @description - Code to create meter dummy object
    * @param objInputData - input data
    * @param strPropertyKey - property key
    * @return callback
    */
function createDummyMeterObj(objInputData, strPropertyKey) {
    objInputData.meterdata[strPropertyKey] = {};
    objInputData.meterdata[strPropertyKey][0] = {};
    objInputData.meterdata[strPropertyKey][0].managerialdata = {};
    objInputData.meterdata[strPropertyKey][0].managerialdata.Meter_CellID = strPropertyKey;
    objInputData.meterdata[strPropertyKey][0].managerialdata.MeterSerialNumber = 0;
    objInputData.meterdata[strPropertyKey][0].MeterLastData = {};
}

/**
    * @description - Code to get managerial data
    * @param objParentData - parent data
    * @return callback
    */
function getManagerialData(objParentData, callback) {
    try {
        var objInput = objParentData.meterdata;
        if (!objInput) {
            return callback('No data to process', false);
        }
        var objKeyLen = Object.keys(objInput).length;
        var loopIndex = 0;
        if (objKeyLen === 0) {
            return callback('No data validaate', false);
        }
        for (var strLoopPropertyKey in objInput) {
            if (objInput.hasOwnProperty(strLoopPropertyKey)) {
                var arrMeterData = [];
                var strPropertyKey = parseInt(strLoopPropertyKey);
                for (var strMeterIdVal in objInput[strPropertyKey]) {
                    if (objInput[strPropertyKey].hasOwnProperty(strMeterIdVal)) {
                        arrMeterData.push(parseInt(strMeterIdVal));
                    }else{
                        console.log('In the else case 1');
                    }
                }
                if (objInput.hasOwnProperty(strPropertyKey)) {
                    invokeAsyncTask(strPropertyKey, arrMeterData);
                } else {
                    console.log('in else case--->2')
                    loopIndex++;
                }
            }
        }

    } catch (err) {
        logger.log(err);
        callback(err, false);
    }
    /**
        * @description - Code to invoke async tasks
        * @param strPropertyKey - property key
        * @param arrMeterData - array of meter data
        * @return callback
        */
    function invokeAsyncTask(strPropertyKey, arrMeterData) {
        async.parallel({
            hypersproutdata: function (innercallback) {
                getHypersproutDataByCellID(strPropertyKey, innercallback);
            },
            meterdata: function (innercallback) {
                getMeterByMeterID(strPropertyKey, arrMeterData, innercallback);
            }
        }, asyncResponse);
    }
    /**
            * @description - Code of async responses
            * @param results - results
            * @return Nil
            */
    function asyncResponse(err, results) {

        //console.log('This is the result in asyncResponse---------------------------->');
      //  console.log(results);
        try {
            loopIndex++;
            if (results) {

                if (results.meterdata) {
                    var intNumOfMeters = Object.keys(results.meterdata);
                    if (intNumOfMeters < 1 && results.transformerdata) {
                        if (results.hypersproutdata) {
                            assignValuesFrmObject(results.hypersproutdata, objInput[results.transformerdata.cellIdVal][0].managerialdata);
                        }
                        assignValuesFrmObject(results.transformerdata, objInput[results.transformerdata.cellIdVal][0].managerialdata);
                    } else {
                        for (var strMeterId in results.meterdata) {
                            if (results.meterdata.hasOwnProperty(strMeterId)) {
                                var meterCellIdVal = results.meterdata[strMeterId].cellId;
                                if (results.hypersproutdata) {
                                    assignValuesFrmObject(results.hypersproutdata, objInput[meterCellIdVal][strMeterId].managerialdata);
                                }
                                if (results.transformerdata) {
                                    assignValuesFrmObject(results.transformerdata, objInput[meterCellIdVal][strMeterId].managerialdata);
                                }
                                if (results.meterdata) {
                                    assignValuesFrmObject(results.meterdata[strMeterId], objInput[meterCellIdVal][strMeterId].managerialdata);
                                }
                            }
                        }
                    }
                }
            }
            if (loopIndex === objKeyLen) {
                callback(null, true);
            }
        } catch (exc) {
            callback(exc, false);
        }
    }
}


module.exports = {
    getManagerialData: getManagerialData,
    createDummyMeterObj: createDummyMeterObj,
};
