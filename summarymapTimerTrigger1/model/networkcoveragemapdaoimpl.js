var objdaoimpl = require('../dao/mysqldaoimpl.js');
var moment = require("moment");
var objConfig = require('../config.js');
var objNetworkCoverageMapModel = require('../model/sqltables/networkcoveragemodel.js');
/**
* @description - Code to update network coverage map
* @param objData - object data
* @param objindex - index
* @param param - parameter
* @param objAngle - angle
* @param objAngleRadians - Angle radians
* @param genLatitude- latitude
* @param genLongitude - longitude
* @param objTransformerData - transformer data
* @return callback
*/
function updateNetworkCoverageMapModel(objData, objindex, param, objAngle, objAngleRadians, genLatitude, genLongitude, callback) {
    var objectToInsert = {};
    if (param === 'Circuit') {
        objectToInsert.TypeID = objindex;
        objectToInsert.Shape = 'CR';
        objectToInsert.Latitude = objData.circuitobj[objindex].CircuitLatitude;
        objectToInsert.Longitude = objData.circuitobj[objindex].CircuitLongitude;
        objectToInsert.Gen_Latitude = genLatitude;
        objectToInsert.Gen_Longitude = genLongitude;
        objectToInsert["Angle(degrees)"] = objAngle;
        objectToInsert["Angle(radians)"] = objAngleRadians;
        objectToInsert["Network Coverage"] = objConfig.networkcoverage.hyperSprouts;
        objectToInsert["Connected Circuit"] = objindex;
        objectToInsert["Connected Transformer"] = null;
        objectToInsert["Circuit ID"] = objindex;
        objectToInsert["Transformer ID"] = null;
        objectToInsert["Meter ID"] = null;
        objdaoimpl.insertData("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage,
            objNetworkCoverageMapModel.objTableProps,
            objectToInsert, function name(err, objCircuitData) {
                if (err) {
                    console.error("Error", err);
                }
                callback(err, objCircuitData);
            });
    }

    if (param === 'Transformer' || param === 'HyperHub') {
        if (param === 'Transformer') {
            objectToInsert.TypeID = objData.transformerobj[objindex].TransformerSerialNumber;
            objectToInsert.Shape = 'TR';
        } else {
            objectToInsert.TypeID = objData.transformerobj[objindex].HypersproutSerialNumber;
            objectToInsert.Shape = 'HH';
        }

        objectToInsert.Latitude = objData.transformerobj[objindex].TransformerLatitude;
        objectToInsert.Longitude = objData.transformerobj[objindex].TransformerLongitude;
        objectToInsert.Gen_Latitude = genLatitude;
        objectToInsert.Gen_Longitude = genLongitude;
        objectToInsert["Angle(degrees)"] = objAngle;
        objectToInsert["Angle(radians)"] = objAngleRadians;
        objectToInsert["Network Coverage"] = objConfig.networkcoverage.hyperSprouts;
        objectToInsert["Connected Circuit"] = objData.transformerobj[objindex].CircuitID;
        objectToInsert["Connected Transformer"] = objData.transformerobj[objindex].TransformerSerialNumber;
        objectToInsert["Circuit ID"] = objData.transformerobj[objindex].CircuitID;
        objectToInsert["Transformer ID"] = objData.transformerobj[objindex].TransformerSerialNumber;
        objectToInsert["Meter ID"] = null;

        objdaoimpl.insertData("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage,
            objNetworkCoverageMapModel.objTableProps,
            objectToInsert, function name(err, objTransformerData) {
                if (err) {
                    console.error("Error", err);
                }
                callback(err, objTransformerData);
            });
    }

    if (param === 'Meter') {
        if (objData.meterobj[objindex].Status === 'Registered' && typeof (objData.meterobj[objindex].TransformerID) !== 'undefined') {
            objectToInsert.TypeID = objData.meterobj[objindex].MeterSerialNumber;
            objectToInsert.Shape = 'MR';
            objectToInsert.Latitude = objData.meterobj[objindex].MeterLatitude;
            objectToInsert.Longitude = objData.meterobj[objindex].MeterLongitude;
            objectToInsert.Gen_Latitude = genLatitude;
            objectToInsert.Gen_Longitude = genLongitude;
            objectToInsert["Angle(degrees)"] = objAngle;
            objectToInsert["Angle(radians)"] = objAngleRadians;
            objectToInsert["Network Coverage"] = objConfig.networkcoverage.meters;
            var connectedTransformer = objData.meterobj[objindex].TransformerID;
            objectToInsert["Connected Circuit"] = objData.transformerobj[connectedTransformer].CircuitID;
            objectToInsert["Connected Transformer"] = objData.transformerobj[connectedTransformer].TransformerSerialNumber;
            objectToInsert["Circuit ID"] = objData.transformerobj[connectedTransformer].CircuitID;
            objectToInsert["Transformer ID"] = objData.transformerobj[connectedTransformer].TransformerSerialNumber;
            objectToInsert["Meter ID"] = objData.meterobj[objindex].MeterSerialNumber;

            objdaoimpl.insertData("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage,
                objNetworkCoverageMapModel.objTableProps,
                objectToInsert, function name(err, objMeterData) {
                    if (err) {
                        console.error("Error", err);
                    }
                    callback(err, objMeterData);
                });
        }
    }

}
module.exports = {
    updateNetworkCoverageMapModel: updateNetworkCoverageMapModel
};