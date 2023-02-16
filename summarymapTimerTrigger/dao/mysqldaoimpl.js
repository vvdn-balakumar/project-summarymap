var dbCon = require('../dao/mysqlconnector.js');
var objAllModels = {};

/**
 * @description - Code to insert data to MySql
 * @param collectionName - Collection Name
 * @param objAttributes - attributes
 * @param objOptions - Options
 * @param objDataToInsert - object Data to be inserted
 * @param {Respose to be returned} callback
 * @return - callback
 */
function insertData(collectionName, objAttributes, objOptions, objDataToInsert, callback) {
    dbCon.getDb(function (err, conn) {
        try {
            if (err) {
                callback(err, null);
            } else {
                var objModel = getModal(conn, collectionName, objAttributes, objOptions);
                objModel.upsert(objDataToInsert).then(function () {
                    callback(null, true);
                }, function (err) {
                    callback(err, null);
                });
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}

/**
 * @description - Code to update data to MySql
 * @param collectionName - Collection Name
 * @param objAttributes - attributes
 * @param objOptions - Options
 * @param objDataToInsert - object Data to be inserted
 * @param {Respose to be returned} callback
 * @return - callback
 */
function updateData(collectionName, objAttributes, objOptions, objDataToInsert, objWhereCond, callback) {
    dbCon.getDb(function (err, conn) {
        try {
            if (err) {
                callback(err, null);
            } else {
                var objModel = getModal(conn, collectionName, objAttributes, objOptions);
                objModel.update(objDataToInsert, { where: objWhereCond }).then(function () {
                    callback(null, true);
                }, function (err) {
                    callback(err, null);
                });
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}
/**
 * @description - Code to truncate data to MySql
 * @param collectionName - Collection Name
 * @param objAttributes - attributes
 * @param objOptions - Options
 * @param objWhereCond - where condition
 * @param {Respose to be returned} callback
 * @return - callback
 */
function truncateEntries(collectionName, objAttributes, objOptions, objWhereCond, callback) {
    dbCon.getDb(function (err, conn) {
        try {
            if (err) {
                callback(err, null);
            } else {
                var objModel = getModal(conn, collectionName, objAttributes, objOptions);
                objModel.sync().then(function () {
                    objModel.destroy({ where: objWhereCond }).then(function () {
                        callback(null, true);
                    }, function (err) {
                        callback(err, null);
                    });
                }, function (err) {
                    callback(err, null);
                });
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}
/**
 * @description - Code to sync data to MySql
 * @param collectionName - Collection Name
 * @param objAttributes - attributes
 * @param objOptions - Options
 * @param {Respose to be returned} callback
 * @return - callback
 */
function synctable(collectionName, objAttributes, objOptions, callback) {
    dbCon.getDb(function (err, conn) {
        try {
            if (err) {
                callback(err, null);
            } else {
                var objModel = getModal(conn, collectionName, objAttributes, objOptions);

                objModel.sync().then(function () {
                    callback(null, true);
                }, function (err) {
                    callback(err, null);
                });
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}
/**
 * @description - Code to find data from MySql
 * @param collectionName - Collection Name
 * @param objAttributes - attributes
 * @param objOptions - Options
 * @param objDataToInsert - object Data to be inserted
 * @param {Respose to be returned} callback
 * @return - callback
 */
function findAll(collectionName, objAttributes, objOptions, arrColumnNamesToBeFetched, objWhereCond, callback) {
    dbCon.getDb(function (err, conn) {
        try {
            if (err) {
                callback(err, null);
            } else {
                var objModel = conn.define(collectionName, objAttributes, objOptions);
                var objParam = { attributes: arrColumnNamesToBeFetched, where: objWhereCond, raw: true };
                objModel.findAll(objParam).then(function (params) {
                    callback(null, params);
                }, function (err) {
                    callback(err, null);
                });
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}
/**
 * @description - Code to get Modal
 * @param collectionName - Collection Name
 * @param objAttributes - attributes
 * @param objOptions - Options
 * @param {Respose to be returned} callback
 * @return - callback
 */
function getModal(conn, collectionName, objAttributes, objOptions) {
    if (!objAllModels[collectionName]) {
        objAllModels[collectionName] = conn.define(collectionName, objAttributes,
            objOptions);
    }
    return objAllModels[collectionName];
}

module.exports = {
    insertData: insertData,
    truncateEntries: truncateEntries,
    synctable: synctable,
    findAll: findAll,
    updateData: updateData
};