var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objConfig = require('../config.js');
var moment = require("moment");
var forEach = require('async-foreach').forEach;
var objNetworkCoverageMapModel = require('../model/sqltables/networkcoveragemodel.js');
var objNetworkCoverageMapModelDAO = require('../model/networkcoveragemapdaoimpl.js');

var loopIndex = 0;
var loopCount = 0;
var logger = console;
/**
* @description - Code to post network coverage map
* @param objData - object data
* @return callback
*/
function postAllNetworkCoverageMapRelatedDetails(objData, callback) {
    try {
        loopIndex = 0;
        loopCount = 0;
        objdaoimpl.synctable("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage,
            objNetworkCoverageMapModel.objTableProps, function (err) {
                if (err) {
                    logger.log(err);
                }
                var network_coverage, i, objAngleRadians, genLatitude, genLongitude;
                objdaoimpl.truncateEntries("networkcoverage", objNetworkCoverageMapModel.objNetworkCoverage,
                    objNetworkCoverageMapModel.objTableProps, {}, function (err) {
                        if (err) {
                            logger.log(err);
                        }
                        var objAngle;
                        for (var objCircuit in objData.circuitobj) {
                            if (objData.circuitobj.hasOwnProperty(objCircuit)) {
                                var circuitLatitude = objData.circuitobj[objCircuit].CircuitLatitude;
                                var circuitLongitude = objData.circuitobj[objCircuit].CircuitLongitude;

                                network_coverage = objConfig.networkcoverage.hyperSprouts;
                                for (i = 1; i <= 360; i = i + 5) {
                                    objAngle = i;
                                    objAngleRadians = objAngle * Math.PI / 180;
                                    genLatitude = parseFloat(circuitLatitude) + network_coverage * Math.sin(objAngleRadians) / 110540;
                                    genLongitude = parseFloat(circuitLongitude) + network_coverage * Math.cos(objAngleRadians) / (111320 * Math.cos((Math.PI * circuitLatitude) / 180));
                                    objNetworkCoverageMapModelDAO.updateNetworkCoverageMapModel(objData, objCircuit, 'Circuit', objAngle, objAngleRadians, genLatitude, genLongitude, responseCallback);
                                    loopCount++;
                                }
                            }
                        }

                        for (var objTransformer in objData.transformerobj) {
                            if (objData.transformerobj.hasOwnProperty(objTransformer)) {
                                var transformerLatitude = objData.transformerobj[objTransformer].TransformerLatitude;
                                var transformerLongitude = objData.transformerobj[objTransformer].TransformerLongitude;
                                var isHyperHub = objData.transformerobj[objTransformer].IsHyperHub;
                                network_coverage = objConfig.networkcoverage.hyperSprouts;
                                for (i = 1; i <= 360; i = i + 5) {
                                    objAngle = i;
                                    objAngleRadians = objAngle * Math.PI / 180;
                                    genLatitude = parseFloat(transformerLatitude) + network_coverage * Math.sin(objAngleRadians) / 110540;
                                    genLongitude = parseFloat(transformerLongitude) + network_coverage * Math.cos(objAngleRadians) / (111320 * Math.cos((Math.PI * transformerLatitude) / 180));
                                    objNetworkCoverageMapModelDAO.updateNetworkCoverageMapModel(objData, objTransformer, 'Transformer', objAngle, objAngleRadians, genLatitude, genLongitude, responseCallback);

                                    if (isHyperHub) {
                                        objNetworkCoverageMapModelDAO.updateNetworkCoverageMapModel(objData, objTransformer, 'HyperHub', objAngle, objAngleRadians, genLatitude, genLongitude, responseCallback);
                                        loopCount++;
                                    }

                                    loopCount++;
                                }
                            }
                        }

                        for (var objMeter in objData.meterobj) {
                            if (objData.meterobj.hasOwnProperty(objMeter)) {
                                var meterLatitude = objData.meterobj[objMeter].MeterLatitude;
                                var meterLongitude = objData.meterobj[objMeter].MeterLongitude;
                                network_coverage = objConfig.networkcoverage.meters;
                                for (i = 1; i <= 360; i = i + 5) {
                                    objAngle = i;
                                    objAngleRadians = objAngle * Math.PI / 180;
                                    genLatitude = parseFloat(meterLatitude) + network_coverage * Math.sin(objAngleRadians) / 110540;
                                    genLongitude = parseFloat(meterLongitude) + network_coverage * Math.cos(objAngleRadians) / (111320 * Math.cos((Math.PI * meterLatitude) / 180));
                                    objNetworkCoverageMapModelDAO.updateNetworkCoverageMapModel(objData, objMeter, 'Meter', objAngle, objAngleRadians, genLatitude, genLongitude, responseCallback);
                                    loopCount++;
                                }
                            }
                        }
                        if (loopCount === 0) {
                            callback(null, true);
                        }
                    });
            });
    } catch (err) {
        logger.error(err);
    }

    function responseCallback(err) {
        if (err) {
            console.error("Error", err);
        }
        loopIndex++;
        if (loopIndex >= loopCount) {
            callback(err, true);
        }
    }
}


module.exports = {
    postAllNetworkCoverageMapRelatedDetails: postAllNetworkCoverageMapRelatedDetails
};