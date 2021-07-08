"use strict";
exports.__esModule = true;
exports.autoBind = void 0;
function isMethod(propertyName, value) {
    return propertyName !== 'constructor' && typeof value === 'function';
}
function autoBind(obj) {
    var propertyNames = Object.getOwnPropertyNames(obj.constructor.prototype);
    propertyNames.forEach(function (propertyName) {
        var value = obj[propertyName];
        if (isMethod(propertyName, value)) {
            obj[propertyName] = value.bind(obj);
        }
    });
}
exports.autoBind = autoBind;
