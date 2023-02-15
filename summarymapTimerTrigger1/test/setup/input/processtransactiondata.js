var objTransactionData = require('./transactiondata.js');
var objjsfunctions = require('../../../util/jsfunctions.js');
var objConfig = require('../../../config.js');

var intNumberOfDays = 1;
var intIntervalMins = 15;

var objTransMeterMap = {
    1 : ['21', '22'],
    2 : ['1', '3']    
}
var objDate = new Date();

var arrTransactionData = [];
var interimCount = 0;

function getTransactionData() {
    objDate = new Date();
    objDate.setUTCMinutes(0, 0 ,0);
    arrTransactionData = [];
    var objCurrentDate = new Date();
    objCurrentDate.setTime(objDate.getTime());
    objCurrentDate.setUTCMinutes(0, 0 ,0);    
    var objStartDate = new Date();
    objStartDate.setTime(objDate.getTime());
    objStartDate.setUTCDate(objStartDate.getDate() - intNumberOfDays); 

    while (objStartDate < objCurrentDate) {
        interimCount++;
        for(var objKey in objTransMeterMap) {         
            var intTransformerId = objKey;
            var arrMeterId = objTransMeterMap[intTransformerId];
            updateData(objStartDate, objCurrentDate, objTransactionData.objTransTransaction, intTransformerId, arrMeterId);        
        }  
        objStartDate.setUTCMinutes(objStartDate.getUTCMinutes() + intIntervalMins);              
    }
    return arrTransactionData;
}

function updateData(objStartDate, objCurrentDate, objTransaction, intTransformerId, arrMeterId) {
    var objDataToInsert = JSON.parse(JSON.stringify(objTransaction)); //Using JSON.parse Considering Unit test case there won't be concern on performance'
    objDataToInsert.DBTimestamp = new Date();
    objDataToInsert.DBTimestamp.setTime(objStartDate.getTime()); 
    objDataToInsert.result.CellID = intTransformerId;
    var intDivisorVal = 60 / intIntervalMins;
    var valToAssign = interimCount % intDivisorVal;
    valToAssign = valToAssign === 0 ?  intDivisorVal : valToAssign;
    objDataToInsert.result.Transformer.Line1Voltage = valToAssign;
    updateObjWithValue(objDataToInsert.result.Transformer, objConfig.transfomer_sm_arrLastValKeys, valToAssign, "Transformer_");
    updateObjWithValue(objDataToInsert.result.Transformer, objConfig.transfomer_sm_arrAverageKeys, valToAssign, "Transformer_");
    updateObjWithValue(objDataToInsert.result.Transformer, objConfig.transfomer_sm_arrDifffernceSumKeys, interimCount, "Transformer_");
    updateObjWithValue(objDataToInsert.result.Transformer, objConfig.transfomer_sm_arrSumKeys, valToAssign, "Transformer_");
    objDataToInsert.result.meters = [];
    for(var i = 0; i < arrMeterId.length; i++) {
        var objMeterDataToInsert = {};
        objMeterDataToInsert = JSON.parse(JSON.stringify(objTransactionData.objMeterTransData));
        objMeterDataToInsert.DeviceID = arrMeterId[i];
        updateObjWithValue(objMeterDataToInsert, objConfig.meter_sm_arrLastValKeys, valToAssign, "Meter_");
        updateObjWithValue(objMeterDataToInsert, objConfig.meter_sm_arrAverageKeys, valToAssign, "Meter_");
        updateObjWithValue(objMeterDataToInsert, objConfig.meter_sm_arrDifffernceSumKeys, interimCount, "Meter_");
        updateObjWithValue(objMeterDataToInsert, objConfig.meter_sm_arrSumKeys, valToAssign, "Meter_");                
        objDataToInsert.result.meters.push(objMeterDataToInsert);
    }
    arrTransactionData.push(objDataToInsert);
}

function updateObjWithValue(objData, arrInputKeys, valToAssign, strTextToReplaceInKey) {
    for(var i = 0; i < arrInputKeys.length; i++) {
        var strKeyName = arrInputKeys[i]; 
        strKeyName = strKeyName.replace(strTextToReplaceInKey, '');
        objData[strKeyName] = valToAssign
    }
}

module.exports = {
    getTransactionData : getTransactionData,
    objTransMeterMap : objTransMeterMap,
    objDate : objDate,
    intNumberOfDays : intNumberOfDays,
    intIntervalMins : intIntervalMins   
};