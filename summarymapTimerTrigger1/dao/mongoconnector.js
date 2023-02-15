var mongodb = require("mongodb");
var objConfig = require('../config.js');
var mongoUrl = '';
var objDb = null;
var connectTimeoutMS = process.env.connectTimeoutMS;
var socketTimeoutMS =  process.env.socketTimeoutMS;
var maxPoolSize = process.env.maxPoolSize ;
global.dbase = {};
/**
 * @description - Code to connect with Mongo DB
 * @param {Respose to be returned} callback
 * @return - callback
 */
function getDb(callback) {
    mongoUrl = objConfig.inputdatasource.url;
    if (!objDb) {
        // connect to the database 
        mongodb.MongoClient.connect(mongoUrl,{poolSize: maxPoolSize,socketTimeoutMS:socketTimeoutMS} ,function (err, databaseClient) {
            if (err) {
                callback(err, null);
            } else {
                dbase = databaseClient;
                let db = databaseClient.db("DELTA");
                objDb = {
                    db: db,

                    // Collection - DELTA_Transformer_Transactions
                    DELTA_Transformer_Transactions: db.collection("DELTA_Transformer_Transactions"),
                    // Collection - DELTA_Meters_Transactions
                    DELTA_Meters_Transactions: db.collection("DELTA_Meters_Transactions"),
                    DELTA_Transaction_Data: db.collection("DELTA_Transaction_Data"),
                    DELTA_AlarmsAndEvents: db.collection("DELTA_AlarmsAndEvents"),
                    DELTA_Meters: db.collection("DELTA_Meters"),
                    DELTA_Transformer: db.collection("DELTA_Transformer"),
                    DELTA_Hypersprouts: db.collection("DELTA_Hypersprouts"),
                    DELTA_Circuit: db.collection("DELTA_Circuit"),
                    DELTA_SystemSettings: db.collection("DELTA_SystemSettings"),
                    DELTA_DBStatistics: db.collection("DELTA_DBStatistics"),
                    DELTA_Transformer_Tech_Item: db.collection("DELTA_Transformer_Tech_Item")
                };
                callback(null, objDb);
            }
        });
    } else {
        callback(null, objDb);
    }
}

function closeDB() {
    if (objDb) {
        dbase.close();
        objDb = null;
    }
}

module.exports = {
    getDb: getDb,
    closeDB: closeDB
};