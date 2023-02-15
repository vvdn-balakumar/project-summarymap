var objInputModel = require('../model/managesummarymapinput.js');
var objOutputModel = require('../model/managesummarymapoutput.js');

var logger = console;

/**
 * @description - Code to retrieve the data for Summary Map table and insert 
 * to mysql
 * @param  context - console
 * @param {Respose to be returned} callback
 * @return - callback
 */
function getSummaryMap(context, callback) {
    logger = context;
    logger.log("get summary map invoked", new Date());
    objInputModel.getAllSummaryMapRelatedDetails(context, function (err, data) {
        logger.log("data received", new Date());
        if (data) {
            console.log('This is the data just before post ****************************************************>');
            console.log(data);
            data.transformerdata = {};
            objOutputModel.postAllSummaryMapRelatedDetails(context,data, callback);
        } else {
            logger.error('error in parsing');
            callback(null, false);
        }
    });
}

module.exports = {
    getSummaryMap: getSummaryMap
};