var dbCon = require('../dao/mongoconnector.js');
var objConfig = require('../config.js');
var logger = console;

/**
 * @description - Code to get Data from Collection 
 * @param collectionName - Name of Collection
 * @param arrWhereKey - condition key
 * @param arrWhereValue - condition Value
 * @return - callback
 */
function getDataFromCollection(collectionName, arrWhereKey, arrWhereValue, callback) {
    getDataFromCollectionSorted(collectionName, arrWhereKey, arrWhereValue, null, null, callback);
}

/**
 * @description - Code to get Cursor from Collection 
 * @param collectionName - Name of Collection
 * @param arrWhereKey - condition key
 * @param arrWhereValue - condition Value
 * @return - callback
 */
function getCursorFromCollection(collectionName, arrWhereKey, arrWhereValue, callback) {
    getCursorFromCollectionSorted(collectionName, arrWhereKey, arrWhereValue, null, null, callback);
}
/**
 * @description - Code to get Sorted Data from Collection 
 * @param collectionName - Name of Collection
 * @param arrWhereKey - condition key
 * @param arrWhereValue - condition Value
 * @param objSortRecord - record to be sorted
 * @param objSkipCols - columns to be skipped
 * @return - callback
 */
function getDataFromCollectionSorted(collectionName, arrWhereKey, arrWhereValue, objSortRecord, objSkipCols, callback) {
    arrWhereKey = !arrWhereKey ? [] : arrWhereKey;
    arrWhereValue = !arrWhereValue ? [] : arrWhereValue;
    dbCon.getDb(function (err, db) {
        if (err) {
            callback(err, null);
        } else {
            var objCollection = db[collectionName];
            collectionfind(objCollection, arrWhereKey, arrWhereValue, objSortRecord, objSkipCols, callback);
        }
    });
}
/**
 * @description - Code to get Cursor Data from Collection 
 * @param collectionName - Name of Collection
 * @param arrWhereKey - condition key
 * @param arrWhereValue - condition Value
 * @param objSortRecord - record to be sorted
 * @param objSkipCols - columns to be skipped
 * @return - callback
 */
function getCursorFromCollectionSorted(collectionName, arrWhereKey, arrWhereValue, objSortRecord, objSkipCols, callback) {
    arrWhereKey = !arrWhereKey ? [] : arrWhereKey;
    arrWhereValue = !arrWhereValue ? [] : arrWhereValue;
    dbCon.getDb(function (err, db) {
        if (err) {
            callback(err, null);
        } else {
            var objCollection = db[collectionName];
            collectionfindCursor(objCollection, arrWhereKey, arrWhereValue, objSortRecord, objSkipCols, callback);
        }
    });
}
/**
 * @description - Code to find data from collection
 * @param collectionName - Name of Collection
 * @param arrWhereKey - condition key
 * @param arrWhereValue - condition Value
 * @param objSortRecord - record to be sorted
 * @param objSkipCols - columns to be skipped
 * @return - callback
 */
function collectionfind(objCollection, arrWhereKey, arrWhereValue, objSortRecord, objSkipCols, callback) {
    try {
        objSkipCols = objSkipCols ? objSkipCols : {};
        var objWhereCond = getWhereCondObj(arrWhereKey, arrWhereValue);
        if (objSortRecord) {
            objCollection.find(objWhereCond, objSkipCols).sort(objSortRecord).toArray(function (err, docs) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, docs);
                }
            });
        } else {
            objCollection.find(objWhereCond, objSkipCols).toArray(function (err, docs) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, docs);
                }
            });
        }
    } catch (err) {
        callback(err, null);
    }
}
/**
 * @description - Code to find Cursor from collection
 * @param collectionName - Name of Collection
 * @param arrWhereKey - condition key
 * @param arrWhereValue - condition Value
 * @param objSortRecord - record to be sorted
 * @param objSkipCols - columns to be skipped
 * @return - callback
 */

function collectionfindCursor(objCollection, arrWhereKey, arrWhereValue, objSortRecord, objSkipCols, callback) {
    try {
        objSkipCols = objSkipCols ? objSkipCols : {};
        var objWhereCond = getWhereCondObj(arrWhereKey, arrWhereValue);
        var objCursor;
        if (objSortRecord) {
            objCursor = objCollection.find(objWhereCond, objSkipCols).sort(objSortRecord);
            callback(null, objCursor);
        } else {
            objCursor = objCollection.find(objWhereCond, objSkipCols);
            callback(null, objCursor);
        }
    } catch (err) {
        callback(err, null);
    }
}
/**
 * @description - Code to generate where condition
 * @param arrWhereKey - condition key
 * @param arrWhereValue - condition Value
 * @return - callback
 */

function getWhereCondObj(arrWhereKey, arrWhereValue) {
    var objOutput = {};
    try {
        if (arrWhereKey.length !== arrWhereValue.length) {
            objOutput[arrWhereKey[0]] = { $in: arrWhereValue };
            return objOutput;
        }
        for (var i = 0; i < arrWhereKey.length; i++) {
            if (Array.isArray(arrWhereValue[i])) {
                var objValObj = { $in: arrWhereValue[i] };
                objOutput[arrWhereKey[i]] = objValObj;
            } else {
                objOutput[arrWhereKey[i]] = arrWhereValue[i];
            }
        }
    } catch (err) {
        logger.log(err);
    }
    return objOutput;
}
/**
 * @description - Code to insert document
 * @param collectionName - collection name
 * @param objToInsert - data to be inserted
 * @return - callback
 */
function insertDoc(collectionName, objToInsert, callback) {
    if (objConfig.environment !== "testcases") {
        callback(new Error('Not authorzied'), null);
        return;
    }
    dbCon.getDb(function (err, db) {
        if (err) {
            callback(err, null);
        } else {
            var objCollection = db[collectionName];
            objCollection.insert(objToInsert, function (err, r) {
                callback(err, r);
            });
        }
    });
}

/**
 * @description - Code to delete all document
 * @param collectionName - collection name
 * @return - callback
 */
function deleteAllDocs(collectionName, callback) {
    if (objConfig.environment !== "testcases") {
        callback(new Error('Not authorzied'), null);
        return;
    }
    dbCon.getDb(function (err, db) {
        if (err) {
            callback(err, null);
        } else {
            var objCollection = db[collectionName];
            objCollection.remove({}, function (err, r) {
                callback(err, r);
            });
        }
    });
}
/**
 * @description - Code to close connection
 * @param Nil
 * @return - callback
 */
function closeConnection() {
    dbCon.closeDB();
}

module.exports = {
    getDataFromCollection: getDataFromCollection,
    getCursorFromCollection: getCursorFromCollection,
    getDataFromCollectionSorted: getDataFromCollectionSorted,
    getCursorFromCollectionSorted: getCursorFromCollectionSorted,
    closeConnection: closeConnection,
    insertDoc: insertDoc,
    deleteAllDocs: deleteAllDocs
};