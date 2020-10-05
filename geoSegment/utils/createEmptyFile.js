const fsPromises = require('fs').promises;

/**
 * @param {string} filepath
 */
async function createEmptyFile(filepath) {
  await fsPromises.writeFile(filepath, '');
}

module.exports = {
  createEmptyFile,
};
