var objdaoimpl = require('../dao/mongodaoimpl.js');
var objManagerialData = require('../model/managerialdatadaoimpl.js');
var async = require('async');
var moment = require("moment");

/**
* @description - Code to get all network coverage map related data
* @return callback
*/
function getAllNetworkCoverageMapRelatedDetails(callback) {
    try {
        async.waterfall([
            function (innercallback) {
                objManagerialData.getMeterByMeterID([], innercallback);
            },
            function (objInput, innercallback) {
                objManagerialData.getHypersproutDataByCellID(objInput, ['TransformerID'], objInput.transformerids, innercallback);
            },
            function (objInput, innercallback) {
                objManagerialData.getTransformerDataByCellID(objInput, ['TransformerID'], objInput.transformerids, innercallback);
            },
            function (objInput, innercallback) {
                objManagerialData.getCircuitByCircuitID(objInput, ['CircuitID'], objInput.circuitids, innercallback);
            },

        ], function (err, results) {
            try {
                delete results.transformerids;
                delete results.circuitids;
                callback(err, results);
            } catch (exc) {
                callback(exc, false);
            }
        });


    } catch (err) {
        console.error(err);
        callback(err, false);
    }
}

module.exports = {
    getAllNetworkCoverageMapRelatedDetails: getAllNetworkCoverageMapRelatedDetails
};