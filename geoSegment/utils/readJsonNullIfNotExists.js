const fs = require('fs-extra');

/**
 * @param {string} filePath
 */
async function readJsonNullIfNotExists(filePath) {
  try {
    return await fs.readJson(filePath);
  } catch (error) {
    if (error && error.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

module.exports = {
  readJsonNullIfNotExists,
};
