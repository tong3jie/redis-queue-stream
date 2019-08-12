const objectToString = obj => {
  return Object.prototype.toString.call(obj);
};

/**
 * 判断数据类型是否为Object
 * @param {object} arg 判断对象
 */
const isObject = arg => {
  return objectToString(arg) === '[object Object]';
};

/**
 * 将object转化为数组
 * @param {object} obj 转化对象
 */
const convertObjectToArray = obj => {
  const result = [];
  for (const [ key, value ] of Object.entries(obj)) {
    result.push(key, value);
  }
  return result;
};

module.exports = { isObject, convertObjectToArray };
