var objAlarmData = require('./alarmandeventsdata.js');
var objjsfunctions = require('../../../util/jsfunctions.js');
var objConfig = require('../../../config.js');

var intNumberOfDays = 1;
var intIntervalMins = 10;

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
            updateData(objStartDate, objCurrentDate, objAlarmData.objData, intTransformerId, arrMeterId);        
        }  
        objStartDate.setMinutes(objStartDate.getMinutes() + intIntervalMins);              
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
    valToAssign = interimCount % 50;
    if(intTransformerId === "1" && valToAssign === 0) {
        objDataToInsert.result.Transformer.PowerFailure = 1;
    }
    objDataToInsert.result.meters = [];
    for(var i = 0; i < arrMeterId.length; i++) {
        var objMeterDataToInsert = {};
        objMeterDataToInsert = JSON.parse(JSON.stringify(objAlarmData.objMeterAlarmData));
        objMeterDataToInsert.DeviceID = arrMeterId[i];      
        objDataToInsert.result.meters.push(objMeterDataToInsert);
    }
    arrTransactionData.push(objDataToInsert);
}

module.exports = {
    getTransactionData : getTransactionData,
    objTransMeterMap : objTransMeterMap,
    objDate : objDate,
    intNumberOfDays : intNumberOfDays,
    intIntervalMins : intIntervalMins   
};