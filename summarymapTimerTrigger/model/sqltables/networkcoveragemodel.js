var Sequelize = require("sequelize");

var objNetworkCoverage = {
    id: { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    TypeID: { type: Sequelize.STRING(100) },
    Shape: { type: Sequelize.STRING(100) },
    Latitude: { type: Sequelize.DECIMAL(20, 7) },
    Longitude: { type: Sequelize.DECIMAL(20, 7) },
    Gen_Latitude: { type: Sequelize.DECIMAL(20, 7) },
    Gen_Longitude: { type: Sequelize.DECIMAL(20, 7) },
    "Angle(degrees)": { type: Sequelize.INTEGER() },
    "Angle(radians)": { type: Sequelize.DECIMAL(20, 7) },
    "Network Coverage": { type: Sequelize.DECIMAL(20) },
    "Connected Circuit": { type: Sequelize.STRING(100) },
    "Connected Transformer": { type: Sequelize.STRING(100) },
    "Circuit ID": { type: Sequelize.STRING(100) },
    "Transformer ID": { type: Sequelize.STRING(100) },
    "Meter ID": { type: Sequelize.STRING(100) }
}

var objTableProps = {
        timestamps: true,
        freezeTableName: true,
        tableName: 'networkcoverage'
    } 

module.exports = {
    objNetworkCoverage: objNetworkCoverage,
    objTableProps: objTableProps
}