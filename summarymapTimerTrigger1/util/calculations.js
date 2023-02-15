var moment = require("moment");
/**
      * @description - Code to process data
      * @param objInput - input data
      * @param arrAverageKeys - array of average keys
      * @param arrSumKeys - array of sum keys
      * @param arrDifferenceColKeys - array of difference key
      * @param arrTimestpampKeys - array of timestamp key
      * @param arrLastValKeys - array of last value key
      * @return callback
      */
function processDataFor(objInput, arrNoProccessKey, arrAverageKeys, arrSumKeys, arrDifferenceColKeys, arrTimestpampKeys, arrLastValKeys) {
    try {
        for (var propertyKey in objInput) {
            if (objInput.hasOwnProperty(propertyKey)) {
                var objInnerData = objInput[propertyKey];
                var objFormattedData = {};
                for (var i = 0; i < objInnerData.length; i++) {
                    processValues(objInnerData, i, objFormattedData, arrNoProccessKey);
                    processArrTimeStampValues(objInnerData, i, objFormattedData, arrTimestpampKeys);
                    processAverage(objInnerData, i, objFormattedData, arrAverageKeys);
                    processLastValue(objInnerData, i, objFormattedData, arrLastValKeys);
                    processSum(objInnerData, i, objFormattedData, arrSumKeys);
                    processDifference(objInnerData, i, objFormattedData, arrDifferenceColKeys);
                }

                objInput[propertyKey] = objFormattedData;
            }
        }
    } catch (err) {
        console.error("Error while processing data ", err);
    }
}

function processAverage(objInputData, index, objFormattedData, arrAverageKeys, intAverageLen) {
    try {
        if (arrAverageKeys) {
            var intAvgLenToUse = intAverageLen ? intAverageLen : objInputData.length;
            for (var j = 0; j < arrAverageKeys.length; j++) {
                var valToAssign = objInputData[index][arrAverageKeys[j]] ? objInputData[index][arrAverageKeys[j]] : 0;
                if (objFormattedData[arrAverageKeys[j]]) {
                    objFormattedData[arrAverageKeys[j]] += valToAssign;
                } else {
                    objFormattedData[arrAverageKeys[j]] = valToAssign;
                }

                if (index === (objInputData.length - 1)) {
                    objFormattedData[arrAverageKeys[j]] = objFormattedData[arrAverageKeys[j]] / intAvgLenToUse;
                }
            }
        }
    } catch (err) {
        console.error("Error while calculating average ", err);
    }
}

function processLastValue(objInputData, index, objFormattedData, arrLastValues) {
    try {
        if (arrLastValues) {
            for (var j = 0; j < arrLastValues.length; j++) {
                var valToAssign = objInputData[index][arrLastValues[j]] ? objInputData[index][arrLastValues[j]] : 0;
                objFormattedData[arrLastValues[j]] = objFormattedData[arrLastValues[j]] ? objFormattedData[arrLastValues[j]] : {};
                objFormattedData[arrLastValues[j]].minutes = objFormattedData[arrLastValues[j]].minutes ? objFormattedData[arrLastValues[j]].minutes : 0;
                if (objInputData[index].MeterDBTimestampVal.getUTCMinutes() >= objFormattedData[arrLastValues[j]].minutes) {
                    objFormattedData[arrLastValues[j]].minutes = objInputData[index].MeterDBTimestampVal.getMinutes();
                    objFormattedData[arrLastValues[j]].value = valToAssign;
                }

                if (index === (objInputData.length - 1)) {
                    objFormattedData[arrLastValues[j]] = objFormattedData[arrLastValues[j]].value;
                }
            }
        }
    } catch (err) {
        console.error("Error while calculating last value ", err);
    }
}

function processValues(objInputData, index, objFormattedData, arrNoProcessKeys) {
    try {
        if (arrNoProcessKeys && index === 0) {
            for (var j = 0; j < arrNoProcessKeys.length; j++) {
                var valToAssign = objInputData[index][arrNoProcessKeys[j]] ? objInputData[index][arrNoProcessKeys[j]] : 0;
                objFormattedData[arrNoProcessKeys[j]] = valToAssign;
            }
        }
    } catch (err) {
        console.error("Error while calculating average ", err);
    }
}

function processArrTimeStampValues(objInputData, index, objFormattedData, arrTimestpampKeys) {
    try {
        if (arrTimestpampKeys && index === 0) {
            for (var j = 0; j < arrTimestpampKeys.length; j++) {
                var valToAssign = objInputData[index][arrTimestpampKeys[j]] ? objInputData[index][arrTimestpampKeys[j]] : null;
                objFormattedData[arrTimestpampKeys[j]] = valToAssign ? moment(valToAssign, 'DD-MM-YYYY HH:mm:ss').toDate() : null;
            }
        }
    } catch (err) {
        console.error("Error while calculating average ", err);
    }
}

function processSum(objInputData, index, objFormattedData, arrSumKeys) {
    try {
        if (arrSumKeys) {
            for (var j = 0; j < arrSumKeys.length; j++) {
                var valToAssign = objInputData[index][arrSumKeys[j]] ? objInputData[index][arrSumKeys[j]] : 0;
                if (objFormattedData[arrSumKeys[j]]) {
                    objFormattedData[arrSumKeys[j]] += valToAssign;
                } else {
                    objFormattedData[arrSumKeys[j]] = valToAssign;
                }
            }
        }
    } catch (err) {
        console.error("Error while calculating sum ", err);
    }
}

function processDifference(objInputData, index, objFormattedData, arrDiffKeys, objPreviousInnerData,propertyKey) {
    try {
        if (arrDiffKeys) {
            var arrSumKeyLen = arrDiffKeys.length;
            for (var j = 0; j < arrSumKeyLen; j++) {
                if(Object.keys(objFormattedData).length>0){
                    if (index === 0 || index === (objInputData.length - 1)) {
                        var valToAssign = objInputData[index][arrDiffKeys[j]] ? objInputData[index][arrDiffKeys[j]] : 0;
                        if (objInputData.length <= 1) {
                            var intPreviousObjIndex = objPreviousInnerData ? objPreviousInnerData.length - 1 : -1;
                            objFormattedData[arrDiffKeys[j]] = objPreviousInnerData ?
                                (objPreviousInnerData[intPreviousObjIndex][arrDiffKeys[j]] ?
                                   valToAssign-objPreviousInnerData[intPreviousObjIndex][arrDiffKeys[j]] : 0) :
                                0;
                            continue;
                        }
                        if (index === (objInputData.length - 1)) {
                            objFormattedData[arrDiffKeys[j]] = valToAssign - objFormattedData[arrDiffKeys[j]];
                        } else {
                            var intPreviousObjIndex = objPreviousInnerData ? objPreviousInnerData.length - 1 : -1;
                            objFormattedData[arrDiffKeys[j]] = objPreviousInnerData ?
                                (objPreviousInnerData[intPreviousObjIndex][arrDiffKeys[j]] ?
                                    objPreviousInnerData[intPreviousObjIndex][arrDiffKeys[j]] : valToAssign) :
                                valToAssign;
                               
                        }
                    }
                }
               
            }
        }
    } catch (err) {
        console.error("Error while calculating sum ", err);
    }
}

module.exports = {
    processDataFor: processDataFor,
    processAverage: processAverage,
    processLastValue: processLastValue,
    processValues: processValues,
    processArrTimeStampValues: processArrTimeStampValues,
    processSum: processSum,
    processDifference: processDifference
};