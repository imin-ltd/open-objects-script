const isObject = require('lodash.isobject');
const merge = require('deepmerge');
const fs = require('fs-extra');

const { readJsonNullIfNotExists } = require('./readJsonNullIfNotExists');

/**
 * @param {object} model
 * @param {object} dataInstance
 */
function mergeToModel(model, dataInstance) {
  // If both are not objects, the object is preferred
  if (isObject(model) && !isObject(dataInstance)) return model;
  if (!isObject(model) && isObject(dataInstance)) return dataInstance;

  // If both are not arrays, the array is preferred
  if (Array.isArray(model) && !Array.isArray(dataInstance)) return model;
  if (Array.isArray(dataInstance) && !Array.isArray(model)) return dataInstance;

  const combineMerge = (target, source) => {
    // If both are not arrays, the array is preferred
    if (Array.isArray(target) && !Array.isArray(source)) return target;
    if (Array.isArray(source) && !Array.isArray(target)) return source;

    // If an array of strings, combine and dedup them (up to a max of 10 items)
    if (target.every((s) => typeof s === 'string')
      && source.every((s) => typeof s === 'string')) {
      return Array.from(new Set([].concat(target, source))).slice(0, 10);
    }
    // If an array of objects, merge them
    if (target.every((s) => isObject(s))
      && source.every((s) => isObject(s))) {
      let destination = Array.isArray(target) && target.length > 0 ? target[0] : {};
      source.forEach((item) => {
        destination = mergeToModel(destination, item);
      });
      return [destination];
    }
    // Otherwise, the data doesn't fit with schema.org, so just ignore this data, it's probably broken
    return source;
  };

  return merge(model, dataInstance, { arrayMerge: combineMerge });
}

/**
 * @param {string} modelFilePath
 * @param {object} data
 */
async function mergeIntoModelExample(modelFilePath, data) {
  let existingModel = await readJsonNullIfNotExists(modelFilePath);
  if (!existingModel) {
    existingModel = {};
  }
  const newModel = mergeToModel(existingModel, data);
  await fs.writeJson(modelFilePath, newModel);
}

module.exports = {
  mergeIntoModelExample,
};
