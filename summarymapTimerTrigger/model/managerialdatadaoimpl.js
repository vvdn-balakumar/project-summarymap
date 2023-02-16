var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var moment = require("moment");

var logger = console;
/**
 * @description - Code to get managerial data
 * @param {Respose to be returned} callback
 * @return - callback
 */
function getManagerialData(callback) {
    try {
        async.waterfall([
            function (innercallback) {
                getMeterByMeterID([], innercallback);
            },
            function (objInput, innercallback) {
                getHypersproutDataByCellID(objInput, [], null, innercallback);
            },
            function (objInput, innercallback) {
                getTransformerDataByCellID(objInput, [], null, innercallback);
            },
            function (objInput, innercallback) {
                getCircuitByCircuitID(objInput, [], null, innercallback);
            },
        ], function (err, results) {
            try {
                if (results) {
                    delete results.transformerids;
                    delete results.circuitids;
                }
                callback(err, results);
            } catch (exc) {
                callback(exc, false);
            }
        });

    } catch (err) {
        logger.log(err);
        callback(err, false);
    }
}
/**
 * @description - Code to get transformer data by cell id
 * @param objInput - input data
 * @param arrWhereKey - where key
 * @param arrWhereValue - where value
 * @return - callback
 */
function getTransformerDataByCellID(objInput, arrWhereKey, arrWhereValue, callback) {
    objdaoimpl.getDataFromCollectionSorted("DELTA_Transformer", arrWhereKey, arrWhereValue,
        null,
        {
            "TransformerID": 1,
            "TransformerSerialNumber": 1,
            "CircuitID": 1,
            "NoOfMeterAllocated": 1
        }
        , function (err, objSelTransformerData) {
            if (err) {
                callback(err, null);
                return;
            }
            try {
                processDataForTransformer(objInput, objSelTransformerData, callback);
            } catch (exc) {
                callback(exc, null);
            }
        });
}
/**
 * @description - Code to process data for transformer
 * @param objInput - input data
 * @param objSelTransformerData - transformer data
 * @return - callback
 */
function processDataForTransformer(objInput, objSelTransformerData, callback) {
    objInput.circuitids = [];
    objInput.arrtransformeridstoprocess = [];
    if (objSelTransformerData && objSelTransformerData.length > 0) {
        objInput.transformerobj = objInput.transformerobj ? objInput.transformerobj : {};
        for (var i = 0; i < objSelTransformerData.length; i++) {
            processDataForTransformerItem(objInput, objSelTransformerData, i);
        }
    }
    //Hyperhub transformer and serial number data updated
    for (var data in objInput.transformerobj) {
        if (objInput.transformerobj[data].IsHyperHub &&
            objInput.transformerobj[objInput.transformerobj[data].TransformerID]) {
            objInput.transformerobj[data]['TransformerSerialNumber'] = objInput.transformerobj[objInput.transformerobj[data].TransformerID].TransformerSerialNumber;
            objInput.transformerobj[data]['CircuitID'] = objInput.transformerobj[objInput.transformerobj[data].TransformerID].CircuitID;
        }
    }
    callback(null, objInput);
}
/**
 * @description - Code to process each transformer data
 * @param objInput - input data
 * @param objSelTransformerData - transformer data
 * @param index - index in array
 * @return - callback
 */

function processDataForTransformerItem(objInput, objSelTransformerData, index) {
    if (objInput.transformerobj[objSelTransformerData[index].TransformerID]) {
        objInput.transformerobj[objSelTransformerData[index].TransformerID].TransformerSerialNumber = objSelTransformerData[index].TransformerSerialNumber;
        objInput.transformerobj[objSelTransformerData[index].TransformerID].CircuitID = objSelTransformerData[index].CircuitID;
        if (objSelTransformerData[index].NoOfMeterAllocated === 0 && objInput.arrtransformeridstoprocess.indexOf(objInput.arrtransformeridstoprocess) === -1) {
            objInput.arrtransformeridstoprocess.push(objSelTransformerData[index].TransformerID);
        }
    }
    if (objInput.circuitids.indexOf(objSelTransformerData[index].CircuitID) === -1) {
        objInput.circuitids.push(objSelTransformerData[index].CircuitID);
    }
}
/**
 * @description - Code to get Circuit data by circuit id
 * @param objInput - input data
 * @param arrWhereKey - where key
 * @param arrWhereValue - where value
 * @return - callback
 */
function getCircuitByCircuitID(objInput, arrWhereKey, arrWhereValue, callback) {
    var objRowData = {};
    objdaoimpl.getDataFromCollectionSorted("DELTA_Circuit", arrWhereKey, arrWhereValue,
        null,
        {
            "CircuitID": 1,
            "Latitude": 1,
            "Longitude": 1
        },
        function (err, objSelCircuitData) {
            if (err) {
                callback(err, null);
                return;
            }
            try {
                processDataForCircuit(objRowData, objInput, objSelCircuitData, callback);
            } catch (exc) {
                callback(exc, null);
            }
        });
}
/**
 * @description - Code to process circuit data
 * @param objRowData - row data
 * @param objInput - input data
 * @param objSelCircuitData - circuit data
 * @return - callback
 */
function processDataForCircuit(objRowData, objInput, objSelCircuitData, callback) {
    objRowData.CircuitID = -1;
    objInput.circuitobj = {};
    if (objSelCircuitData && objSelCircuitData.length > 0) {
        for (var i = 0; i < objSelCircuitData.length; i++) {
            objInput.circuitobj[objSelCircuitData[i].CircuitID] = {};
            objInput.circuitobj[objSelCircuitData[i].CircuitID].CircuitID = objSelCircuitData[i].CircuitID;

            objInput.circuitobj[objSelCircuitData[i].CircuitID].CircuitLatitude = objSelCircuitData[i].Latitude;
            objInput.circuitobj[objSelCircuitData[i].CircuitID].CircuitLongitude = objSelCircuitData[i].Longitude;
        }
    }
    callback(null, objInput);
}
/**
 * @description - Code to get hypersprout data by cell id
 * @param objInput - input data
 * @param arrWhereKey - where key
 * @param arrWhereValue - where value
 * @return - callback
 */
function getHypersproutDataByCellID(objInput, arrWhereKey, arrWhereValue, callback) {
    try {
        objdaoimpl.getDataFromCollectionSorted("DELTA_Hypersprouts", arrWhereKey, arrWhereValue,
            null,
            {
                "HypersproutID": 1,
                "HypersproutSerialNumber": 1,
                "IsHyperHub": 1,
                "TransformerID": 1,
                "Hypersprout_Communications.Latitude": 1,
                "Hypersprout_Communications.Longitude": 1,
                "Hypersprout_DeviceDetails.TransformerRating": 1,
                "Hypersprout_DeviceDetails.Phase": 1
            },
            function (err, objSelTransformerData) {
                if (err) {
                    callback(err, null);
                    return;
                }
                try {
                    processInputForHS(objInput, objSelTransformerData, callback);
                } catch (exc) {
                    callback(exc, null);
                }
            });
    } catch (err) {
        callback(err, null);
    }
}
/**
 * @description - Code to process hypersprout data
 * @param objInput - input data
 * @param objSelTransformerData - hypersprout data
 * @return - callback
 */
function processInputForHS(objInput, objSelTransformerData, callback) {
    objInput.transformerobj = {};
    if (objSelTransformerData && objSelTransformerData.length > 0) {
        for (var i = 0; i < objSelTransformerData.length; i++) {
            objInput.transformerobj[objSelTransformerData[i].HypersproutID] = {};
            objInput.transformerobj[objSelTransformerData[i].HypersproutID].HypersproutID = objSelTransformerData[i].HypersproutID;
            objInput.transformerobj[objSelTransformerData[i].HypersproutID].HypersproutSerialNumber = objSelTransformerData[i].HypersproutSerialNumber;
            objInput.transformerobj[objSelTransformerData[i].HypersproutID].IsHyperHub = objSelTransformerData[i].IsHyperHub ? true : false;
            objInput.transformerobj[objSelTransformerData[i].HypersproutID].TransformerID = objSelTransformerData[i].TransformerID;

            var objHypersproutComm = objSelTransformerData[i].Hypersprout_Communications;
            if (objHypersproutComm) {
                objInput.transformerobj[objSelTransformerData[i].HypersproutID].TransformerLatitude = objHypersproutComm.Latitude;
                objInput.transformerobj[objSelTransformerData[i].HypersproutID].TransformerLongitude = objHypersproutComm.Longitude;
            }

            var objHypersproutDevDetails = objSelTransformerData[i].Hypersprout_DeviceDetails;
            if (objHypersproutDevDetails) {
                objInput.transformerobj[objSelTransformerData[i].HypersproutID].Transformer_Phase = objHypersproutDevDetails.Phase;
                objInput.transformerobj[objSelTransformerData[i].HypersproutID].TransformerRating = objHypersproutDevDetails.TransformerRating;
            }
        }

    }
    callback(null, objInput);
}
/**
 * @description - Code to get meter data by meter id
 * @param meterIdVal - meter id
 * @return - callback
 */

function getMeterByMeterID(meterIdVal, callback) {
    var objRowData = {};
    var objWhereKeys = meterIdVal && meterIdVal.length > 0 ? ['MeterID'] : [];
    objdaoimpl.getDataFromCollectionSorted("DELTA_Meters", ["Status"], ["Registered"], null, {
        "MeterSerialNumber": 1,
        "MeterID": 1,
        "TransformerID": 1,
        "HypersproutID": 1,
        "Meters_Communications.Latitude": 1,
        "Meters_Communications.Longitude": 1,
        "Meters_DeviceDetails.Phase": 1,
        "Meters_DeviceDetails.CT_Ratio":1,
        "SolarPanel": 1,
        "EVMeter": 1,
        "Status": 1
    }, function (err, objSelMeterData) {
        if (err) {
            callback(err, null);
            return;
        }
        try {
            processInputForMeter(objRowData, objSelMeterData, callback);
        } catch (exc) {
            logger.log(exc);
            callback(exc, null);
        }
    });
}
/**
 * @description - Code to process meter data
 * @param objRowData - row data
 * @param objSelMeterData - meter data
 * @return - callback
 */

function processInputForMeter(objRowData, objSelMeterData, callback) {
    objRowData.transformerids = [];
    objRowData.meterobj = {};
    if (objSelMeterData && objSelMeterData.length > 0) {
        for (var i = 0; i < objSelMeterData.length; i++) {
            if (objSelMeterData[i].Status !== 'Registered') {
                continue;
            }
            objRowData.meterobj[objSelMeterData[i].MeterID] = {};
            objRowData.meterobj[objSelMeterData[i].MeterID].TransformerID = objSelMeterData[i].TransformerID;
            objRowData.meterobj[objSelMeterData[i].MeterID].HypersproutID = objSelMeterData[i].HypersproutID;
            objRowData.meterobj[objSelMeterData[i].MeterID].MeterSerialNumber = objSelMeterData[i].MeterSerialNumber;
            objRowData.meterobj[objSelMeterData[i].MeterID].Status = objSelMeterData[i].Status;
            if (objRowData.transformerids.indexOf(objSelMeterData[i].TransformerID) === -1) {
                objRowData.transformerids.push(objSelMeterData[i].TransformerID);
            }

            var objMeterComm = objSelMeterData[i].Meters_Communications;
            if (objMeterComm) {
                objRowData.meterobj[objSelMeterData[i].MeterID].MeterLatitude = objMeterComm.Latitude;
                objRowData.meterobj[objSelMeterData[i].MeterID].MeterLongitude = objMeterComm.Longitude;
            }

            var objMeterDeviceDetails = objSelMeterData[i].Meters_DeviceDetails;
            if (objMeterDeviceDetails) {
                objRowData.meterobj[objSelMeterData[i].MeterID].Meter_Phase = objMeterDeviceDetails.Phase;
                objRowData.meterobj[objSelMeterData[i].MeterID].Meter_CTRatio = objMeterDeviceDetails.CT_Ratio;
            }
            objSelMeterData[i].SolarPanel = objSelMeterData[i].SolarPanel;
            objRowData.meterobj[objSelMeterData[i].MeterID].SolarPanel = objSelMeterData[i].SolarPanel ? true : false;
            objSelMeterData[i].EVMeter = objSelMeterData[i].EVMeter;
            objRowData.meterobj[objSelMeterData[i].MeterID].EVMeter = objSelMeterData[i].EVMeter ? true : false;
        }
    }
    callback(null, objRowData);
}

module.exports = {
    getManagerialData: getManagerialData,
    getCircuitByCircuitID: getCircuitByCircuitID,
    getMeterByMeterID: getMeterByMeterID,
    getHypersproutDataByCellID: getHypersproutDataByCellID,
    getTransformerDataByCellID: getTransformerDataByCellID
};
