var objAlarmsandeventsModel = require('../model/alarmsandeventsmodeldao.js');

var logger = console;
/**
 * @description - Code to retrieve the data for alarms events table and insert 
 * to mysql
 * @param  context - console
 * @param {Respose to be returned} callback
 * @return - callback
 */
function migrateAlarmsandEventsToRdbms(context, callback) {
    logger = context;
    logger.log("migrate alarms and events to RDBMS invoked", new Date());
    objAlarmsandeventsModel.updateAlarmsandEventsDataToRDBMS(context, function (err, data) {
        logger.log("migrate alarms and events to RDBMS received", data, new Date());
        callback(null, true);
    });
}

module.exports = {
    migrateAlarmsandEventsToRdbms: migrateAlarmsandEventsToRdbms
};