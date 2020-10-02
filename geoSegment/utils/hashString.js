const crypto = require('crypto');

/**
 * @param {string} string
 */
function hashString(string) {
  return crypto.createHash('sha1').update(string).digest('hex');
}

module.exports = {
  hashString,
};
