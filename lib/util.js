function objectToString(o) {
  return Object.prototype.toString.call(o);
}

exports.isObject = arg => {
  return objectToString(arg) === '[object Object]';
};
