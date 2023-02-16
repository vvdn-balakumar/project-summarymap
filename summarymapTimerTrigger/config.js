var objConfig;
if (!objConfig) {
    objConfig = {};
}

/**
 * CellIDs to be not calculated, this is done specifically for SA setup where
 * Hyperhub is simulated but actual implementation is not in place
 */
objConfig.cellIdToSkip = [];
objConfig.transScheduler = 4;

/**
 * Code to process the input data source details to read the data for processing
 * Datasource host should be provided with IP / Domain name of the database server
 * Datasource port should be provided with port on which the database server runs
 * Datasource database should be provided with database name in the mysql server where the transaction data is present
 * Datasource username should be provided with user name of the database server
 * Datasource password should be provided with password of the database server
 * Datasource admin should be provided with admin user name of the database server
 * Datasource adminpassword should be provided with admin password of the database server
 */
objConfig.inputdatasource = {};
objConfig.inputdatasource.host = process.env.mongodbHost;
objConfig.inputdatasource.port = process.env.mongodbPort;
objConfig.inputdatasource.database = process.env.mongodbDatabase;
objConfig.inputdatasource.username = process.env.mongodbUsername;
objConfig.inputdatasource.password = process.env.mongodbPassword;
objConfig.inputdatasource.admindatabase = process.env.adminDatabase;
objConfig.inputdatasource.admin = null;
objConfig.inputdatasource.adminpassword = null;
//Code to populate the datasource URL based on the configurations
if (objConfig.inputdatasource.username) {
    // Populate the data source URL when user name value is provided
    objConfig.inputdatasource.url = 'mongodb://' + objConfig.inputdatasource.username + ':' +
        objConfig.inputdatasource.password + '@' +
        objConfig.inputdatasource.host + ':' + objConfig.inputdatasource.port + '/' +
        objConfig.inputdatasource.database + '?authSource=' + objConfig.inputdatasource.admindatabase;
       // +'&connectTimeoutMS='+objConfig.inputdatasource.connectTimeoutMS 
        // +'&socketTimeoutMS='+objConfig.inputdatasource.socketTimeoutMS
        // +'&maxPoolSize='+objConfig.inputdatasource.maxPoolSize;
} else {
    // Populate the data source URL when user name value is not provided
    objConfig.inputdatasource.url = 'mongodb://' + objConfig.inputdatasource.host + ':' + objConfig.inputdatasource.port + '/' +
        objConfig.inputdatasource.database + '?authSource=' + objConfig.inputdatasource.admindatabase;
        // +'&connectTimeoutMS='+objConfig.inputdatasource.connectTimeoutMS 
        // +'&socketTimeoutMS='+objConfig.inputdatasource.socketTimeoutMS
        // +'&maxPoolSize='+objConfig.inputdatasource.maxPoolSize;
}


/**
 * Code to populate the output data source details to read the data for processing
 * Datasource host should be provided with IP / Domain name of the database server
 * Datasource port should be provided with port on which the database server runs
 * Datasource database should be provided with database name in the mysql server where the transaction data is present
 * Datasource username should be provided with user name of the database server
 * Datasource password should be provided with password of the database server
 */
objConfig.outputdatasource = {};
objConfig.outputdatasource.host = process.env.mysqlHost;
objConfig.outputdatasource.database = process.env.mysqlDb;
objConfig.outputdatasource.username = process.env.mysqlUsername;
objConfig.outputdatasource.password = process.env.mysqlPassword;

// }
// Dialect property input for Sequelize library
objConfig.outputdatasource.dialect = 'mysql';
// To disable the logging of Sequelize library
objConfig.outputdatasource.logging = false;
objConfig.networkcoverage = {};
// Network coverage value for hypersprout required for Network coverage map calculation
objConfig.networkcoverage.hyperSprouts = '250';
// Network coverage value for meter required for Network coverage map calculation
objConfig.networkcoverage.meters = '60';
// Number of days to process for Summary map calculation
objConfig.numberofDaysSM = 1;
// Number of days to process for Transform meter transaction data processing
objConfig.numberofDaysTransMeterTransaction = 1;
// Meter related Keys which are not required to be processed for Summary map
objConfig.meter_sm_arrNoProccessKey = ['Meter_CellID', 'Meter_DeviceID'];
// Meter related Keys which are required to be processed based on timestamp
objConfig.meter_sm_arrTimestpampKeys = ['Meter_ReadTimestamp', 'ActualMeter_ReadTimestamp'];
// Meter related Keys which are required to be processed based on for average
objConfig.meter_sm_arrAverageKeys = [];
// Meter related Keys which are required to be processed considering the last value
objConfig.meter_sm_arrLastValKeys = ['Meter_Line1InstVoltage', 'Meter_Line1InstCurrent', 'Meter_Line1Frequency',
    "Meter_Line2InstVoltage", "Meter_Line3InstVoltage", "Meter_Line2InstCurrent",
    "Meter_Line3InstCurrent", "Meter_Line2Frequency", "Meter_Line3Frequency",
    "Meter_Line1PowerFactor", "Meter_Line2PowerFactor", "Meter_Line3PowerFactor"];
// Meter related Keys which are required to be processed considering the sum of values
objConfig.meter_sm_arrSumKeys = [];
// Meter related Keys which are required to be processed considering the difference of values
objConfig.meter_sm_arrDifffernceSumKeys = ['Meter_Apparent_m_Total', 'Meter_ActiveReceivedCumulativeRate_Total', 'Meter_ActiveDeliveredCumulativeRate_Total','Meter_ApparentReceivedCumulativeRate_Total'];

// Transformer related Keys which are not required to be processed for Summary map
objConfig.transfomer_sm_arrNoProccessKey = ['Transformer_CellID'];
// Transformer related Keys which are required to be processed based on timestamp
objConfig.transfomer_sm_arrTimestpampKeys = ['Transformer_ReadTimestamp', 'ActualTransformer_ReadTimestamp'];
// Transformer related Keys which are required to be processed based on for average
objConfig.transfomer_sm_arrAverageKeys = [];
// Transformer related Keys which are required to be processed considering the last value
objConfig.transfomer_sm_arrLastValKeys = ["Transformer_Line1Voltage", "Transformer_Line2Voltage", "Transformer_Line3Voltage",
    "Transformer_Line1Current", "Transformer_Line2Current", "Transformer_Line3Current",
    "Transformer_AmbientTemperarture", "Transformer_TopTemperature",
    "Transformer_BottomTemperature", "Transformer_Line1PhaseAngle",
    "Transformer_Line2PhaseAngle", "Transformer_Line3PhaseAngle", "Transformer_BatteryVoltage",
    "Transformer_BatteryStatus"];
// Transformer related Keys which are required to be processed considering the sum of values
objConfig.transfomer_sm_arrSumKeys = ["Transformer_TransformerOilLevel"];
// Transformer related Keys which are required to be processed considering the difference of values
objConfig.transfomer_sm_arrDifffernceSumKeys = ["Transformer_Apparent_m_Total", "Transformer_ActiveReceivedCumulativeRate_Total",
    "Transformer_ActiveDeliveredCumulativeRate_Total"];


// Alarm and events list of meter keys to be considered for processing
objConfig.arrMeterAlarmKey = ['Meter_VoltageSagLine1', 'Meter_VoltageSagLine2', 'Meter_VoltageSagLine3', 'Meter_VoltageSwellLine1',
    'Meter_VoltageSwellLine2', 'Meter_VoltageSwellLine3', 'Meter_VoltageUnbalance', 'Meter_VoltageCablelossLine1',
    'Meter_VoltageCablelossLine2', 'Meter_VoltageCablelossLine3', 'Meter_VoltageTHDOverLimitLine1', 'Meter_VoltageTHDOverLimitLine2',
    'Meter_VoltageTHDOverLimitLine3', 'Meter_CurrentTHDOverLimitLine1', 'Meter_CurrentTHDOverLimitLine2', 'Meter_CurrentTHDOverLimitLine3',
    'Meter_PrimaryPowerUp', 'Meter_PrimaryPowerDown', 'Meter_LongOutagedetection', 'Meter_ShortOutagedetection',
    'Meter_NonvolatileMemoryFailed', 'Meter_Clockerrordetected', 'Meter_LowBatteryVoltage', 'Meter_FlashMemoryFailed',
    'Meter_Firmwareupgraded', 'Meter_Demandreset', 'Meter_TimeSynchronized', 'Meter_Historylogcleared',
    'Meter_Coverremoval', 'Meter_Terminalcoverremoval', 'Meter_MeterDisconnected', 'Meter_MeterConnected',
    'Meter_Demandresponseofimportactpwr(kW+)', 'Meter_Demandresponseofexportactpwr(kW-)', 'Meter_Demandresponseofimportreactpwr(kVar+)',
    'Meter_Demandresponseofexportreactpwr(kVar-)'];
// Alarm and events list of hypersprout keys to be considered for processing
objConfig.arrHypersproutAlarmKey = ['OverVoltage', 'UnderVoltage', 'OverLoadLine1(MD Alarm)', 'OverLoadLine2(MD Alarm)', 'OverLoadLine3(MD Alarm)',
    'OverFrequency', 'UnderFrequency', 'PowerFailure', 'CTOpen', 'PTOpen', 'OilLevelSensorFailure',
    'TamperLid', 'TamperBox', 'LowOilLevel', 'HighOilTemperature', 'LowBatteryVoltage', 'BatteryFailure',
    'BatteryRemoved', 'PrimaryPowerUp', 'PrimaryPowerDown', 'Non-TechnicalLoss', 'MeterConnected',
    'MeterDisconnected', 'Wi-FiCommunicationLoss', '3G/4G/LTECommunicationLoss', 'Communicationattemptsexceeded',
    'UnAuthenticatedConnectionRequest'];
// Timeout value for testcases
objConfig.testcasetimeout = 15000;

module.exports = objConfig;
