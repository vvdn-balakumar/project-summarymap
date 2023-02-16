var Sequelize = require("sequelize");

var objOutageData = {
    "Event ID": { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
	"Start Time": { type: Sequelize.DATE},
	"End Time": { type: Sequelize.DATE},
	"Duration": { type: Sequelize.STRING(8)},
	Status: { type: Sequelize.STRING(20)},
    "Circuit ID": { type: Sequelize.STRING(100) },
	"Circuit Latitude": { type: Sequelize.DECIMAL(20, 7) },
	"Circuit Longitude": { type: Sequelize.DECIMAL(20, 7) },
	"Transformer ID": { type: Sequelize.STRING(100)},
    "Hypersprout ID": { type: Sequelize.STRING(100) },
	"Transformer Latitude": { type: Sequelize.DECIMAL(20, 7) },
	"Transformer Longitude": { type: Sequelize.DECIMAL(20, 7) },
	"Meter ID": { type: Sequelize.STRING(100), unique: 'uniqueoutageid'},
	"Meter Latitude": { type: Sequelize.DECIMAL(20, 7) },
	"Meter Longitude": { type: Sequelize.DECIMAL(20, 7) },
    "Circuit Power Flow Out":  { type: Sequelize.DECIMAL(20) },
    "Transformer Power Flow Out": {type: Sequelize.DECIMAL(20)},
    "Top Oil Temperature": {type: Sequelize.DECIMAL(20, 7)},
    "Bottom Oil Temperature": {type: Sequelize.DECIMAL(20, 7)},
    "Total KVA": {type: Sequelize.DECIMAL(20)},
    "Unique Start Time": { type: Sequelize.DATE, unique: 'uniqueoutageid', allowNull: false},
}

var objTableProps = {
        timestamps: true,
        freezeTableName: true,
        tableName: 'outagemap'
    } 

module.exports = {
    objOutageData: objOutageData,
    objTableProps: objTableProps
}