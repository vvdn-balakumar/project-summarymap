var objInputModel = require('../model/manageoutageinput.js');
var objOutputModel = require('../model/outagemapdaoimpl.js');

var logger = console;

/**
 * @description - Code to retrieve the data for Outage table and insert 
 * to mysql
 * @param  context - console
 * @param {Respose to be returned} callback
 * @return - callback
 */
function processOutages(context, callback) {
    logger = context;
    logger.log("process outages", new Date());
    objInputModel.processOutageDetails(context, function (err, data) {
        logger.log('received response');
        if (data) {
            objOutputModel.updateOutageMapModel(data, function (err, outputData) {
                logger.log(err, outputData, new Date());
                callback(err, outputData);
            }, logger);
        }
    });
}

module.exports = {
    processOutages: processOutages
};