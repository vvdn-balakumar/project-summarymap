var Sequelize = require('sequelize');

var objTransformerevents = {
    CellID: { type: Sequelize.INTEGER(), primaryKey: true, unique: 'uniquemeterid', defaultValue: 0},
    MeterSerialNumber: { type: Sequelize.STRING(100)},
    CircuitID: { type: Sequelize.STRING(100) },    
    HypersproutID: { type: Sequelize.STRING(100)},
    TransformerID: { type: Sequelize.STRING(100)},    
    Meter_DeviceID: { type: Sequelize.INTEGER(1), primaryKey: true, unique: 'uniquemeterid', defaultValue: 0},      
    Phase : { type: Sequelize.INTEGER()},
    Status : { type: Sequelize.STRING(100)},
    AlarmsType: { type: Sequelize.STRING(100), primaryKey: true, unique: 'uniquemeterid'},
    AlarmsValue: { type: Sequelize.INTEGER(1)},
    DBTimestamp : { type: Sequelize.DATE}, 
}

var objTableProps = {
        timestamps: true,
        freezeTableName: true,
        tableName: 'alarmseventstransformerlatest'
    } 

module.exports = {
    objTransformerevents: objTransformerevents,
    objTableProps: objTableProps
}