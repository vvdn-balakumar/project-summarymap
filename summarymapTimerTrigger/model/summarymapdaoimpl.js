var objdaoimpl = require('../dao/mysqldaoimpl.js');
var moment = require("moment");
var objSummaryMapModel = require('../model/sqltables/summarymapmodel.js');
var objLatestTransactionModel = require('../model/sqltables/latesttransactionmodel.js');

/**
    * @description - Code to update summary map data
    * @param objDeviceMergedData - merged data
    * @param objHSKey - HS key
    * @param objDeviceID - device id
    * @param objPropertyKey - property key
    * @return Nil
    */
function updateSummaryMapModel(arrDateToCompare, objDeviceMergedData, objHSKey, objDeviceID, objPropertyKey, summaryArray, latestArray, callback) {

    var objLooppedData = objDeviceMergedData[objHSKey][objDeviceID].TranData;
    var objLooppedTransformerData = objDeviceMergedData[objHSKey].TransformerData;
    objLooppedData = objPropertyKey === 'MeterLastData' ? objDeviceMergedData[objHSKey][objDeviceID] : objLooppedData;

    if (objPropertyKey === 'MeterLastData' && !objLooppedData[objPropertyKey]) {
        return 0;
    }
    var DEFAULT_START_DATE = new Date(1970, 0, 1, 0, 0, 0);
    var datetime = new Date(Date.now());
    var now = datetime.toISOString();
    if (objPropertyKey !== 'managerialdata') {
        var objManagerialData = objDeviceMergedData[objHSKey][objDeviceID].managerialdata;
        var objTransactionData = objLooppedData[objPropertyKey];
        objLooppedTransformerData = objLooppedTransformerData ? objLooppedTransformerData : {};
        let objTransfomerTransactionData = objLooppedTransformerData[objPropertyKey];
        // console.log('This is the device merge keys')
        // console.log(objDeviceMergedData[objHSKey]);
        // if(objDeviceMergedData[objHSKey][objDeviceID].MeterLastData.Meter_SelfHeal!==undefined){
        //     if (objDeviceMergedData[objHSKey][objDeviceID].MeterLastData.Meter_SelfHeal == 1 && objPropertyKey !== 'MeterLastData') {
        //         if (!objLooppedTransformerData.hasOwnProperty(objPropertyKey)) {
        //             let arrDateToCompareLen = arrDateToCompare.length;
        //             while (arrDateToCompare--) {
        //                 //getting the previous non zero value for the self healed meters
        //                 let previousDateobjPropertyKey = arrDateToCompare[arrDateToCompareLen];
        //                 if (objLooppedTransformerData.hasOwnProperty(previousDateobjPropertyKey)) {
        //                     objTransfomerTransactionData = objLooppedTransformerData[previousDateobjPropertyKey];
        //                 } 
        //             }
        //         }
        //     } 
        // }
       
        objManagerialData = !objManagerialData ? {} : objManagerialData;
        objTransactionData = !objTransactionData ? {} : objTransactionData;
        objTransfomerTransactionData = !objTransfomerTransactionData ? {} : objTransfomerTransactionData;

        var objectToInsert = {};

        if (objManagerialData.MeterSerialNumber || objManagerialData.Transformer_CellID) {
            objectToInsert.MeterID = objManagerialData.MeterSerialNumber;
            objectToInsert.Meter_DeviceID = objManagerialData.Meter_DeviceID?objManagerialData.Meter_DeviceID:objDeviceID;
            objectToInsert.MeterLatitude = objManagerialData.MeterLatitude ? parseFloat(objManagerialData.MeterLatitude).toFixed(7) : 500.0000000;
            objectToInsert.MeterLongitude = objManagerialData.MeterLongitude ? parseFloat(objManagerialData.MeterLongitude).toFixed(7) : 500.0000000;
            objectToInsert.HypersproutID = objManagerialData.HypersproutSerialNumber;

            if (objManagerialData.MeterSerialNumber === 0 && !objTransactionData.Meter_ReadTimestamp) {
                objTransactionData.Meter_ReadTimestamp = objTransactionData.Transformer_ReadTimestamp;
            }
            if (objTransactionData.Meter_ReadTimestamp) {
                objTransactionData.Meter_ReadTimestamp.setUTCMinutes(0);
                objTransactionData.Meter_ReadTimestamp.setUTCSeconds(0);
                objTransactionData.Meter_ReadTimestamp.setUTCMilliseconds(0);
            }
            objectToInsert.Meter_ReadTimestamp = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : DEFAULT_START_DATE;

            //objectToInsert.Meter_ReadTimestamp =(new Date(objTransactionData.Meter_ReadTimestamp)).toLocaleString();

            objectToInsert.TransformerID = objManagerialData.TransformerSerialNumber;
            objectToInsert.Transformer_CellID = objManagerialData.Transformer_CellID ? objManagerialData.Transformer_CellID : objHSKey;
            objectToInsert.TransformerLatitude = objManagerialData.TransformerLatitude ? parseFloat(objManagerialData.TransformerLatitude).toFixed(7) : 0;
            objectToInsert.TransformerLongitude = objManagerialData.TransformerLongitude ? parseFloat(objManagerialData.TransformerLongitude).toFixed(7) : 0;
            if (objTransfomerTransactionData.Transformer_ReadTimestamp) {
                objTransfomerTransactionData.Transformer_ReadTimestamp.setUTCMinutes(0);
                objTransfomerTransactionData.Transformer_ReadTimestamp.setUTCSeconds(0);
                objTransfomerTransactionData.Transformer_ReadTimestamp.setUTCMilliseconds(0);
            }
            objectToInsert.Transformer_ReadTimestamp = objTransfomerTransactionData.Transformer_ReadTimestamp ? moment(objTransfomerTransactionData.Transformer_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : objectToInsert.Meter_ReadTimestamp;
            objectToInsert.CircuitLatitude = objManagerialData.CircuitLatitude ? parseFloat(objManagerialData.CircuitLatitude).toFixed(7) : 0;
            objectToInsert.CircuitLongitude = objManagerialData.CircuitLongitude ? parseFloat(objManagerialData.CircuitLongitude).toFixed(7) : 0;
            objectToInsert.CircuitID = objManagerialData.CircuitID;
            objectToInsert.Meter_Line1InstVoltage = objTransactionData.Meter_Line1InstVoltage ? objTransactionData.Meter_Line1InstVoltage.toFixed(7) : 0;
            objectToInsert.Meter_Line1InstCurrent = objTransactionData.Meter_Line1InstCurrent ? objTransactionData.Meter_Line1InstCurrent.toFixed(7) : 0;
            objectToInsert.Meter_Line1Frequency = objTransactionData.Meter_Line1Frequency ? objTransactionData.Meter_Line1Frequency.toFixed(7) : 0;
            objectToInsert.Meter_Apparent_m_Total = objTransactionData.Meter_Apparent_m_Total ? objTransactionData.Meter_Apparent_m_Total.toFixed(3) : 0;
            objectToInsert.Meter_ActiveReceivedCumulativeRate_Total = objTransactionData.Meter_ActiveReceivedCumulativeRate_Total ?(objManagerialData.Meter_CTRatio?(objTransactionData.Meter_ActiveReceivedCumulativeRate_Total*objManagerialData.Meter_CTRatio).toFixed(6):objTransactionData.Meter_ActiveReceivedCumulativeRate_Total.toFixed(6))  : 0;
            objectToInsert.Meter_ActiveDeliveredCumulativeRate_Total = objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total ? objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total.toFixed(6) : 0;
            objectToInsert.MeterApparentReceivedCumulativeRate_Total = objTransactionData.Meter_ApparentReceivedCumulativeRate_Total ? objTransactionData.Meter_ApparentReceivedCumulativeRate_Total : 0;

            objectToInsert.Transformer_Line1Voltage = objTransfomerTransactionData.Transformer_Line1Voltage ? objTransfomerTransactionData.Transformer_Line1Voltage : 0;
            objectToInsert.Transformer_Line2Voltage = objTransfomerTransactionData.Transformer_Line2Voltage ? objTransfomerTransactionData.Transformer_Line2Voltage : 0;
            objectToInsert.Transformer_Line3Voltage = objTransfomerTransactionData.Transformer_Line3Voltage ? objTransfomerTransactionData.Transformer_Line3Voltage : 0;
            objectToInsert.Transformer_Line1Current = objTransfomerTransactionData.Transformer_Line1Current ? objTransfomerTransactionData.Transformer_Line1Current.toFixed(2) : 0;
            objectToInsert.Transformer_Line2Current = objTransfomerTransactionData.Transformer_Line2Current ? objTransfomerTransactionData.Transformer_Line2Current.toFixed(2) : 0;
            objectToInsert.Transformer_Line3Current = objTransfomerTransactionData.Transformer_Line3Current ? objTransfomerTransactionData.Transformer_Line3Current.toFixed(2) : 0;
            objectToInsert.Transformer_AmbientTemperarture = objTransfomerTransactionData.Transformer_AmbientTemperarture ? objTransfomerTransactionData.Transformer_AmbientTemperarture.toFixed(7) : 0;
            objectToInsert.Transformer_TopTemperature = objTransfomerTransactionData.Transformer_TopTemperature ? objTransfomerTransactionData.Transformer_TopTemperature.toFixed(7) : 0;
            objectToInsert.Transformer_BottomTemperature = objTransfomerTransactionData.Transformer_BottomTemperature ? objTransfomerTransactionData.Transformer_BottomTemperature.toFixed(7) : 0;
            objectToInsert.Transformer_TransformerOilLevel = objTransfomerTransactionData.Transformer_TransformerOilLevel ? objTransfomerTransactionData.Transformer_TransformerOilLevel : 0;
            objectToInsert.Transformer_Apparent_m_Total = objTransfomerTransactionData.Transformer_Apparent_m_Total ? objTransfomerTransactionData.Transformer_Apparent_m_Total.toFixed(3) : 0;
            objectToInsert.Transformer_ActiveReceivedCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total : 0;
            objectToInsert.Transformer_ActiveDeliveredCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total : 0;


            objectToInsert.DateTime = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : DEFAULT_START_DATE;
            objectToInsert.Hours = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc().hours() : DEFAULT_START_DATE;
            objectToInsert["Circuit ID"] = objManagerialData.CircuitID;
            objectToInsert.Circuit_Latitude = objManagerialData.CircuitLatitude ? parseFloat(objManagerialData.CircuitLatitude).toFixed(7) : 0;
            objectToInsert.Circuit_Longitude = objManagerialData.CircuitLongitude ? parseFloat(objManagerialData.CircuitLongitude).toFixed(7) : 0;
            objectToInsert["Transformer ID"] = objManagerialData.TransformerSerialNumber;
            objectToInsert["Hypersprout ID"] = objManagerialData.HypersproutSerialNumber;
            objectToInsert.Transformer_Latitude = objManagerialData.TransformerLatitude ? parseFloat(objManagerialData.TransformerLatitude).toFixed(7) : 0;
            objectToInsert.Transformer_Longitude = objManagerialData.TransformerLongitude ? parseFloat(objManagerialData.TransformerLongitude).toFixed(7) : 0;
            objectToInsert["Meter ID"] = objManagerialData.MeterSerialNumber;
            objectToInsert.Meter_Latitude = objManagerialData.MeterLatitude ? parseFloat(objManagerialData.MeterLatitude).toFixed(7) : 0;
            objectToInsert.Meter_Longitude = objManagerialData.MeterLongitude ? parseFloat(objManagerialData.MeterLongitude).toFixed(7) : 0;

            objectToInsert.Transformer_active_energy_received = objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total : 0;
            objectToInsert.Transformer_active_energy_delivered = objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total : 0;
            objectToInsert.Meter_active_energy_received = objTransactionData.Meter_ActiveReceivedCumulativeRate_Total ?(objManagerialData.Meter_CTRatio?(objTransactionData.Meter_ActiveReceivedCumulativeRate_Total*objManagerialData.Meter_CTRatio).toFixed(6):objTransactionData.Meter_ActiveReceivedCumulativeRate_Total.toFixed(6))  : 0;
            objectToInsert.Meter_active_energy_delivered = objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total ? objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total.toFixed(6) : 0;
            objectToInsert.Top_Temperature = objTransfomerTransactionData.Transformer_TopTemperature ? objTransfomerTransactionData.Transformer_TopTemperature.toFixed(7) : 0;
            objectToInsert.Bottom_Temperature = objTransfomerTransactionData.Transformer_BottomTemperature ? objTransfomerTransactionData.Transformer_BottomTemperature.toFixed(7) : 0;
            objectToInsert.ambient_temparature = objTransfomerTransactionData.Transformer_AmbientTemperarture ? objTransfomerTransactionData.Transformer_AmbientTemperarture.toFixed(7) : 0;
            objectToInsert.Energy_Apparent_Absolute = objTransfomerTransactionData.Transformer_Apparent_m_Total ? objTransfomerTransactionData.Transformer_Apparent_m_Total.toFixed(3) : 0;

            objectToInsert.Date = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : DEFAULT_START_DATE;
            objectToInsert.Date = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : DEFAULT_START_DATE;
            objectToInsert.TransformerActiveReceivedCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total : 0;
            objectToInsert.TransformerActiveDeliveredCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total : 0;
            objectToInsert.MeterActiveReceivedCumulativeRate_Total = objTransactionData.Meter_ActiveReceivedCumulativeRate_Total ?(objManagerialData.Meter_CTRatio?(objTransactionData.Meter_ActiveReceivedCumulativeRate_Total*objManagerialData.Meter_CTRatio).toFixed(6):objTransactionData.Meter_ActiveReceivedCumulativeRate_Total.toFixed(6))  : 0;
            objectToInsert.MeterActiveDeliveredCumulativeRate_Total = objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total ? objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total.toFixed(6) : 0;

            objectToInsert.NetworkResponceRate = objTransactionData.Meter_NetworkResponseRate ? parseFloat(objTransactionData.Meter_NetworkResponseRate).toFixed(2) : 0;
            objectToInsert.TopTemperature = objTransfomerTransactionData.Transformer_TopTemperature ? objTransfomerTransactionData.Transformer_TopTemperature.toFixed(7) : 0;
            objectToInsert.BottomTemperature = objTransfomerTransactionData.Transformer_BottomTemperature ? objTransfomerTransactionData.Transformer_BottomTemperature.toFixed(7) : 0;
            objectToInsert.AmbientTemperarture = objTransfomerTransactionData.Transformer_AmbientTemperarture ? objTransfomerTransactionData.Transformer_AmbientTemperarture.toFixed(7) : 0;
            objectToInsert.Apparent_m_Total = objTransfomerTransactionData.Transformer_Apparent_m_Total ? objTransfomerTransactionData.Transformer_Apparent_m_Total.toFixed(3) : 0;
            objectToInsert.Circuit_Id = objManagerialData.CircuitID;
            objectToInsert.Tranformer_Id = objManagerialData.TransformerSerialNumber;
            objectToInsert.Hypersprout_ID = objManagerialData.HypersproutSerialNumber;
            objectToInsert.Meter_Id = objManagerialData.MeterSerialNumber;
            // Commented based on shobin/mumbai team request
            objectToInsert.Non_technichal_Loss = objTransfomerTransactionData.Non_Technical_Loss ? objTransfomerTransactionData.Non_Technical_Loss : 0;
            // objectToInsert.Non_technichal_Loss = 0;

            objectToInsert.ActualMeter_ReadTimestamp = objTransactionData.ActualMeter_ReadTimestamp ? moment(objTransactionData.ActualMeter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : DEFAULT_START_DATE;
            objectToInsert.ActualTransformer_ReadTimestamp = objTransfomerTransactionData.ActualTransformer_ReadTimestamp ? moment(objTransfomerTransactionData.ActualTransformer_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : DEFAULT_START_DATE;
            objectToInsert.SolarPanel = objManagerialData.SolarPanel ? 1 : 0;
            objectToInsert.EVMeter = objManagerialData.EVMeter ? 1 : 0;

            objectToInsert.Meter_Line2InstVoltage = objTransactionData.Meter_Line2InstVoltage ? parseFloat(objTransactionData.Meter_Line2InstVoltage).toFixed(7) : 0;
            objectToInsert.Meter_Line3InstVoltage = objTransactionData.Meter_Line3InstVoltage ? parseFloat(objTransactionData.Meter_Line3InstVoltage).toFixed(7) : 0;
            objectToInsert.Meter_Line2InstCurrent = objTransactionData.Meter_Line2InstCurrent ? parseFloat(objTransactionData.Meter_Line2InstCurrent).toFixed(7) : 0;
            objectToInsert.Meter_Line3InstCurrent = objTransactionData.Meter_Line3InstCurrent ? parseFloat(objTransactionData.Meter_Line3InstCurrent).toFixed(7) : 0;
            objectToInsert.Meter_Line2Frequency = objTransactionData.Meter_Line2Frequency ? parseFloat(objTransactionData.Meter_Line2Frequency).toFixed(7) : 0;
            objectToInsert.Meter_Line3Frequency = objTransactionData.Meter_Line3Frequency ? parseFloat(objTransactionData.Meter_Line3Frequency).toFixed(7) : 0;
            objectToInsert.Meter_Phase = objManagerialData.Meter_Phase ? objManagerialData.Meter_Phase : 0;
            objectToInsert.Meter_Line1PowerFactor = objTransactionData.Meter_Line1PowerFactor ? parseFloat(objTransactionData.Meter_Line1PowerFactor).toFixed(7) : 0;
            objectToInsert.Meter_Line2PowerFactor = objTransactionData.Meter_Line2PowerFactor ? parseFloat(objTransactionData.Meter_Line2PowerFactor).toFixed(7) : 0;
            objectToInsert.Meter_Line3PowerFactor = objTransactionData.Meter_Line3PowerFactor ? parseFloat(objTransactionData.Meter_Line3PowerFactor).toFixed(7) : 0;

            objectToInsert.Transformer_Phase = objManagerialData.Transformer_Phase ? objManagerialData.Transformer_Phase : 0;
            objectToInsert.Transformer_Line1PhaseAngle = objTransfomerTransactionData.Transformer_Line1PhaseAngle ? parseFloat(objTransfomerTransactionData.Transformer_Line1PhaseAngle).toFixed(7) : 0;
            objectToInsert.Transformer_Line2PhaseAngle = objTransfomerTransactionData.Transformer_Line2PhaseAngle ? parseFloat(objTransfomerTransactionData.Transformer_Line2PhaseAngle).toFixed(7) : 0;
            objectToInsert.Transformer_Line3PhaseAngle = objTransfomerTransactionData.Transformer_Line3PhaseAngle ? parseFloat(objTransfomerTransactionData.Transformer_Line3PhaseAngle).toFixed(7) : 0;
            objectToInsert.Transformer_BatteryVoltage = objTransfomerTransactionData.Transformer_BatteryVoltage ? objTransfomerTransactionData.Transformer_BatteryVoltage : 0;
            objectToInsert.Transformer_BatteryStatus = objTransfomerTransactionData.Transformer_BatteryStatus ? objTransfomerTransactionData.Transformer_BatteryStatus : 0;

            objectToInsert.TransformerRating = objManagerialData.TransformerRating ? objManagerialData.TransformerRating : 0;
            objectToInsert.IsHyperHub = objManagerialData.IsHyperHub ? 1 : 0;
            objectToInsert.createdAt = now;
            objectToInsert.updatedAt = now;
            if (objPropertyKey === 'MeterLastData') {
                // Transformer data is going null in latesttransaction table, so adding these values
                if (objLooppedData.hasOwnProperty('MeterLastData')) {
                    //change array for latest with filter
                    objectToInsertlatest = {}
                    objectToInsertlatest.MeterID = objectToInsert.MeterID;
                    objectToInsertlatest.Meter_DeviceID = objectToInsert.Meter_DeviceID;
                    objectToInsertlatest.MeterLatitude = objectToInsert.MeterLatitude;
                    objectToInsertlatest.MeterLongitude = objectToInsert.MeterLongitude;
                    objectToInsertlatest.HypersproutID = objectToInsert.HypersproutID;
                    objectToInsertlatest.Meter_ReadTimestamp = objectToInsert.Meter_ReadTimestamp;
                    objectToInsertlatest.TransformerID = objectToInsert.TransformerID;
                    objectToInsertlatest.Transformer_CellID = objectToInsert.Transformer_CellID;
                    objectToInsertlatest.TransformerLatitude = objectToInsert.TransformerLatitude;
                    objectToInsertlatest.TransformerLongitude = objectToInsert.TransformerLongitude;
                    objectToInsertlatest.Transformer_ReadTimestamp = objectToInsert.Transformer_ReadTimestamp;
                    objectToInsertlatest.CircuitLatitude = objectToInsert.CircuitLatitude;
                    objectToInsertlatest.CircuitLongitude = objectToInsert.CircuitLongitude;
                    objectToInsertlatest.CircuitID = objectToInsert.CircuitID;
                    objectToInsertlatest.Meter_Line1InstVoltage = objectToInsert.Meter_Line1InstVoltage;
                    objectToInsertlatest.Meter_Line1InstCurrent = objectToInsert.Meter_Line1InstCurrent;
                    objectToInsertlatest.Meter_Line1Frequency = objectToInsert.Meter_Line1Frequency;
                    objectToInsertlatest.Meter_Apparent_m_Total = objectToInsert.Meter_Apparent_m_Total;
                    objectToInsertlatest.Meter_ActiveReceivedCumulativeRate_Total = objectToInsert.Meter_ActiveReceivedCumulativeRate_Total;
                    objectToInsertlatest.Meter_ActiveDeliveredCumulativeRate_Total = objectToInsert.Meter_ActiveDeliveredCumulativeRate_Total;
                    objectToInsertlatest.Transformer_Line1Voltage = objLooppedData['MeterLastData']['Transformer_Line1Voltage'];
                    objectToInsertlatest.Transformer_Line2Voltage = objLooppedData['MeterLastData']['Transformer_Line2Voltage'];
                    objectToInsertlatest.Transformer_Line3Voltage = objLooppedData['MeterLastData']['Transformer_Line3Voltage'];
                    objectToInsertlatest.Transformer_Line1Current = objLooppedData['MeterLastData']['Transformer_Line1Current'];
                    objectToInsertlatest.Transformer_Line2Current = objLooppedData['MeterLastData']['Transformer_Line2Current'];
                    objectToInsertlatest.Transformer_Line3Current = objLooppedData['MeterLastData']['Transformer_Line3Current'];
                    objectToInsertlatest.Transformer_AmbientTemperarture = objLooppedData['MeterLastData']['Transformer_AmbientTemperarture'];
                    objectToInsertlatest.Transformer_TopTemperature = objLooppedData['MeterLastData']['Transformer_TopTemperature'];
                    objectToInsertlatest.Transformer_BottomTemperature = objLooppedData['MeterLastData']['Transformer_BottomTemperature'];
                    objectToInsertlatest.Transformer_TransformerOilLevel = objLooppedData['MeterLastData']['Transformer_TransformerOilLevel'];
                    objectToInsertlatest.Transformer_Apparent_m_Total = objLooppedData['MeterLastData']['Transformer_Apparent_m_Total'];
                    objectToInsertlatest.Transformer_ActiveReceivedCumulativeRate_Total = objLooppedData['MeterLastData']['Transformer_ActiveReceivedCumulativeRate_Total'];
                    objectToInsertlatest.Transformer_ActiveDeliveredCumulativeRate_Total = objLooppedData['MeterLastData']['Transformer_ActiveDeliveredCumulativeRate_Total'];
                    objectToInsertlatest.Meter_Line2InstVoltage = objectToInsert.Meter_Line2InstVoltage;
                    objectToInsertlatest.Meter_Line3InstVoltage = objectToInsert.Meter_Line3InstVoltage;
                    objectToInsertlatest.Meter_Line2InstCurrent = objectToInsert.Meter_Line2InstCurrent;
                    objectToInsertlatest.Meter_Line3InstCurrent = objectToInsert.Meter_Line3InstCurrent;
                    objectToInsertlatest.Meter_Line2Frequency = objectToInsert.Meter_Line2Frequency;
                    objectToInsertlatest.Meter_Line3Frequency = objectToInsert.Meter_Line3Frequency;
                    objectToInsertlatest.Meter_Phase = objectToInsert.Meter_Phase;
                    objectToInsertlatest.Meter_Line1PowerFactor = objectToInsert.Meter_Line1PowerFactor;
                    objectToInsertlatest.Meter_Line2PowerFactor = objectToInsert.Meter_Line2PowerFactor;
                    objectToInsertlatest.Meter_Line3PowerFactor = objectToInsert.Meter_Line3PowerFactor;
                    objectToInsertlatest.Transformer_Phase = objectToInsert.Transformer_Phase;
                    objectToInsertlatest.Transformer_Line1PhaseAngle = objLooppedData['MeterLastData']['Transformer_Line1PhaseAngle'];
                    objectToInsertlatest.Transformer_Line2PhaseAngle = objLooppedData['MeterLastData']['Transformer_Line2PhaseAngle'];
                    objectToInsertlatest.Transformer_Line3PhaseAngle = objLooppedData['MeterLastData']['Transformer_Line3PhaseAngle'];
                    objectToInsertlatest.Transformer_BatteryVoltage = objLooppedData['MeterLastData']['Transformer_BatteryVoltage'];
                    objectToInsertlatest.Transformer_BatteryStatus = objLooppedData['MeterLastData']['Transformer_BatteryStatus'];
                    objectToInsertlatest.createdAt = now;
                    objectToInsertlatest.updatedAt = now;
                }

                latestArray.push(objectToInsertlatest);
                //console.log("latestArray length -->" ,latestArray.length);
                // objdaoimpl.insertData("latesttransactions", objLatestTransactionModel.objLatestTrans,
                //     objLatestTransactionModel.objTableProps,
                //     objectToInsert, function (err, objMeterTransData) {
                //         if (err) {
                //             console.error("Error", err);
                //         }
                //         callback(err, objMeterTransData);
                //     });
            } else {
                if (!objectToInsert["Circuit ID"] && !objectToInsert["Transformer ID"]) {
                    return 0;
                } else {
                    summaryArray.push(objectToInsert);
                    // console.log("summaryArray length -->", summaryArray);
                    // objdaoimpl.insertData("summarymap", objSummaryMapModel.objSummaryMap,
                    //     objSummaryMapModel.objTableProps,
                    //     objectToInsert, function (err, objTransformerTransData) {
                    //         callback(err, objTransformerTransData);
                    //     });
                    // CSV Push
                }
            }
        } else {
            return 0;
        }
    } else {
        return 0;
    }
    return 1;
}

module.exports = {
    updateSummaryMapModel: updateSummaryMapModel
};
