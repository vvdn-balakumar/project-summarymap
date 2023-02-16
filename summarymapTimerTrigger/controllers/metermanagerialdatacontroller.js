var objInputModel = require('../model/metermanagerialdatadao.js');

var logger = console;
/**
 * @description - Code to retrieve the data for Managerial table and insert 
 * to mysql
 * @param  context - console
 * @param {Respose to be returned} callback
 * @return - callback
 */
function updateMeterManagerialDataToRDBMS(context, callback) {
    logger = context;
    logger.log("updateMeterManagerialDataToRDBMS to RDBMS invoked", new Date());
    objInputModel.updateMeterManagerialDataToRDBMS(context, function (err, data) {
        logger.log("updateMeterManagerialDataToRDBMS to RDBMS received", new Date());
        callback(err, data);
    });
}

module.exports = {
    updateMeterManagerialDataToRDBMS: updateMeterManagerialDataToRDBMS
};