const moment = require('moment-timezone');
const util = require('util');

/**
 * @param {'info' | 'warn' | 'error'} level
 * @param  {...unknown} messages
 */
function log(level, ...messages) {
  /** @type {string[]} */
  const completeLogMsgParts = [
    `[${moment.tz('utc').toISOString()}]`, // timestamp
    `[${level}]`, // log level
    ...messages.map((message) => {
      if (typeof message === 'string') {
        return message;
      }
      return util.inspect(message, false, 10);
    }),
  ];
  const completeLogMsg = completeLogMsgParts.join(' ');
  console[level](completeLogMsg);
}

module.exports = {
  log,
};
