var Sequelize = require("sequelize");

var objTableColumns = {
    id: { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    CircuitID : { type: Sequelize.STRING(100)},
    TransformerID: { type: Sequelize.STRING(100)},
    HypersproutID : { type: Sequelize.STRING(100)},
    TransformerSerialNumber : { type: Sequelize.STRING(100)},
    HypersproutSerialNumber : { type: Sequelize.STRING(100)},
    MeterID : { type: Sequelize.STRING(100)},
    MeterStatus : { type: Sequelize.STRING(100)},
    MeterSerialNumber : { type: Sequelize.STRING(100)}
}

var objTableProps = {
        timestamps: true,
        freezeTableName: true,
        tableName: 'managerialdata'
    } 

module.exports = {
    objTableColumns: objTableColumns,
    objTableProps: objTableProps
}