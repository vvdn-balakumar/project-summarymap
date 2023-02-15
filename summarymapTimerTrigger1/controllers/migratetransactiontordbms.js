var objInputModel = require('../model/transformertransactionmodeldao.js');

var logger = console;
/**
 * @description - Code to retrieve the data for transformer table and insert 
 * to mysql
 * @param  context - console
 * @param {Respose to be returned} callback
 * @return - callback
 */
function migrateTransactionsToRdbms(context, callback) {
    logger = context;
    logger.log("migrate transaction to RDBMS invoked", new Date());
    objInputModel.updateTransformerTransactionDataToRDBMS(context, function (err, data) {
        logger.log("migrate transaction to RDBMS received", data, new Date());
        callback(null, true);
    });
}

module.exports = {
    migrateTransactionsToRdbms: migrateTransactionsToRdbms
};