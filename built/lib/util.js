"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.objectToString = (obj) => Object.prototype.toString.call(obj);
/**
 * 判断数据类型是否为Object
 * @param {object} arg 判断对象
 */
exports.isObject = (arg) => exports.objectToString(arg) === '[object Object]';
/**
 * 将object转化为数组
 * @param {object} obj 转化对象
 */
exports.convertObjectToArray = (obj) => {
    const result = [];
    for (const [key, value] of Object.entries(obj)) {
        result.push(key, value);
    }
    return result;
};
