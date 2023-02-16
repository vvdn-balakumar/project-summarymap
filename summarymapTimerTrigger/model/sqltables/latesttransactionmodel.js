var Sequelize = require("sequelize");

var objLatestTrans = {
    id: { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    MeterID: { type: Sequelize.STRING(100) },
    Meter_DeviceID: { type: Sequelize.STRING(100), unique: 'uniqueSelectedItem' },
    MeterLatitude: { type: Sequelize.DECIMAL(20, 7) },
    MeterLongitude: { type: Sequelize.DECIMAL(20, 7) },
    HypersproutID: { type: Sequelize.STRING(100) },
    Meter_ReadTimestamp: { type: Sequelize.DATE },
    TransformerID: { type: Sequelize.STRING(100) },
    Transformer_CellID: { type: Sequelize.STRING(100), unique: 'uniqueSelectedItem' },
    TransformerLatitude: { type: Sequelize.DECIMAL(20, 7) },
    TransformerLongitude: { type: Sequelize.DECIMAL(20, 7) },
    Transformer_ReadTimestamp: { type: Sequelize.DATE },
    CircuitLatitude: { type: Sequelize.DECIMAL(20, 7) },
    CircuitLongitude: { type: Sequelize.DECIMAL(20, 7) },
    CircuitID: { type: Sequelize.STRING(100) },
    Meter_Line1InstVoltage: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line1InstCurrent: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line1Frequency: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Apparent_m_Total: { type: Sequelize.DECIMAL(20, 3) },
    Meter_ActiveReceivedCumulativeRate_Total: { type: Sequelize.DECIMAL(20, 6) },
    Meter_ActiveDeliveredCumulativeRate_Total: { type: Sequelize.DECIMAL(20, 6) },
    Transformer_Line1Voltage: { type: Sequelize.DECIMAL(20) },
    Transformer_Line2Voltage: { type: Sequelize.DECIMAL(20) },
    Transformer_Line3Voltage: { type: Sequelize.DECIMAL(20) },
    Transformer_Line1Current: { type: Sequelize.DECIMAL(20, 2) },
    Transformer_Line2Current: { type: Sequelize.DECIMAL(20, 2) },
    Transformer_Line3Current: { type: Sequelize.DECIMAL(20, 2) },
    Transformer_AmbientTemperarture: { type: Sequelize.DECIMAL(20, 7) },
    Transformer_TopTemperature: { type: Sequelize.DECIMAL(20, 7) },
    Transformer_BottomTemperature: { type: Sequelize.DECIMAL(20, 7) },
    Transformer_TransformerOilLevel: { type: Sequelize.DECIMAL(20) },
    Transformer_Apparent_m_Total: { type: Sequelize.DECIMAL(20, 3) },
    Transformer_ActiveReceivedCumulativeRate_Total: { type: Sequelize.DECIMAL(20, 6) },
    Transformer_ActiveDeliveredCumulativeRate_Total: { type: Sequelize.DECIMAL(20, 6) },

    Meter_Line2InstVoltage: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line3InstVoltage: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line2InstCurrent: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line3InstCurrent: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line2Frequency: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line3Frequency: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Phase: { type: Sequelize.INTEGER(1) },
    Meter_Line1PowerFactor: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line2PowerFactor: { type: Sequelize.DECIMAL(20, 7) },
    Meter_Line3PowerFactor: { type: Sequelize.DECIMAL(20, 7) },

    Transformer_Phase: { type: Sequelize.INTEGER(1) },
    Transformer_Line1PhaseAngle: { type: Sequelize.DECIMAL(20, 7) },
    Transformer_Line2PhaseAngle: { type: Sequelize.DECIMAL(20, 7) },
    Transformer_Line3PhaseAngle: { type: Sequelize.DECIMAL(20, 7) },
    Transformer_BatteryVoltage: { type: Sequelize.INTEGER() },
    Transformer_BatteryStatus: { type: Sequelize.INTEGER() }
}

var objTableProps = {
    timestamps: true,
    freezeTableName: true,
    tableName: 'latesttransactions'
}

module.exports = {
    objLatestTrans: objLatestTrans,
    objTableProps: objTableProps
}