/**
    * @description - Code to assign value from object
    * @param objInput - input data
    * @param objOutput - output data
    * @param keyPrefix - key prefix
    * @param arrFilter - array of filter
    * @param skipFilter - skip filter data
    * @param isSkipValues - values to be skipped
    * @return callback
    */
function assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, skipFilter, isSkipValues) {
    try {
        objOutput = (!objOutput) ? {} : objOutput;
        for (var propertyKey in objInput) {
            if (objInput.hasOwnProperty(propertyKey)) {
                processAssignValues(propertyKey, objInput, objOutput, keyPrefix, arrFilter, skipFilter, isSkipValues);
            }
        }
    } catch (err) {
        console.error("Error while assign values from object", err);
    }
}
/**
      * @description - Code to assign process valued
      * @param objInput - input data
      * @param objOutput - output data
      * @param keyPrefix - key prefix
      * @param arrFilter - array of filter
      * @param skipFilter - skip filter data
      * @param isSkipValues - values to be skipped
      * @return callback
      */
function processAssignValues(propertyKey, objInput, objOutput, keyPrefix, arrFilter, skipFilter, isSkipValues) {
    if (arrFilter && !skipFilter && arrFilter.indexOf(propertyKey) === -1) {
        return;
    }
    var strKeyName = (keyPrefix) ? (keyPrefix + propertyKey) : propertyKey;
    objOutput[strKeyName] = isSkipValues ? 0 : objInput[propertyKey];
}

module.exports = {
    assignValuesFrmObject: assignValuesFrmObject
};