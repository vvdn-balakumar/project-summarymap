var cron = require('node-cron');

var objSummaryMap = require('./controllers/summarymapcontroller.js');
var outagecontroller = require('./controllers/outagecontroller.js');
var objMigrateAllTransactionsData = require('./controllers/migratetransactiontordbms.js');
var objMigrateAllMeterTransactionsData = require('./controllers/migratemetertransactiontordbms.js');
var objNetworkCoverageMap = require('./controllers/networkcoveragemapcontroller.js');
var objMeterManagerialData = require('./controllers/metermanagerialdatacontroller.js');
var objDBStatistics = require('./controllers/dbStatistics.js');

var cons = console;
// console = {
//     log: function () { },
//     warn: function () { },
//     error: function () { }
// }
var objMigrateAllAlarmsandEventsData = require('./controllers/migratealarmsandeventstordbms.js');

//var objtask = cron.schedule('*/15 * * * *', function () {

    require('fs').appendFile('datamart-logs.txt', "datamart invoked at " + Date.now() + "\n");

	
    objMigrateAllAlarmsandEventsData.migrateAlarmsandEventsToRdbms(console, function (err, obj) {
        cons.log('Error migrateAlarmsandEventsToRdbms', err);
           objNetworkCoverageMap.getNetworkCoverageMap(console, function (err, obj) {
               cons.log('Error getNetworkCoverageMap',err);
               objMigrateAllTransactionsData.migrateTransactionsToRdbms(console, function (err, obj) {
                   cons.log('Error migrateTransactionsToRdbms',err);
                   objMigrateAllMeterTransactionsData.migrateTransactionsToRdbms(console, function (err, obj) {
                       cons.log('Error migrateTransactionsToRdbms',err);
                       outagecontroller.processOutages(console, function (err, obj) {
                           cons.log('Error processOutages',err);
                           objSummaryMap.getSummaryMap(console, function (err, obj) {
                               cons.log('Error getSummaryMap',err);
                               objMeterManagerialData.updateMeterManagerialDataToRDBMS(console, function (err, obj) {
                                   cons.log('Error updateMeterManagerialDataToRDBMS',err);
                                   cons.log("end app", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2), 'Mb');
                                   objDBStatistics.insertDatabaseStatistics(console, function (err, obj) {
                                       cons.log('Error insertDatabaseStatistics',err);
					process.exit(0);
                                   });
                               });

                           });
                       });

                   });

               });
           });
       });

//}, true);


module.exports = {};
