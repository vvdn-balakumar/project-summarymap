var objSummaryMap = require('./controllers/summarymapcontroller.js');
var dbCon = require('./dao/mongodaoimpl');

module.exports = function (context, myTimer) {
    context.log('Summary map initiated');
    objSummaryMap.getSummaryMap(context, function (err, obj) {
        context.log('Error', err);
       // dbCon.closeConnection();
        context.done();
    });
};