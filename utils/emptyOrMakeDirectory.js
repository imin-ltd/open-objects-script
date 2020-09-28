const fsPromises = require('fs').promises;
const fs = require('fs-extra');

/**
 * @param {string} directoryPath
 */
async function emptyOrMakeDirectory(directoryPath) {
  // Create/Wipe folder
  if (await fs.pathExists(directoryPath)) {
    await fs.emptyDir(directoryPath);
  } else {
    await fsPromises.mkdir(directoryPath);
  }
}

module.exports = {
  emptyOrMakeDirectory,
};
