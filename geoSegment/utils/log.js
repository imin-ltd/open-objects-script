const fs = require('fs-extra');
const moment = require('moment-timezone');
const util = require('util');

/** @type {string} */
let logFilePath;

/**
 * Set global log file path. Note: This path must exist on the file system.
 *
 * @param {string} filePath
 */
function setGlobalLogFilePathMut(filePath) {
  logFilePath = filePath;
}

/**
 * Log some stuff to stdout and to a file
 *
 * PRE-REQUISITE: the log file must already exist
 *
 * @param {'info' | 'warn' | 'error'} level
 * @param  {...unknown} messages
 */
async function log(level, ...messages) {
  // ## Log to stdout
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
  // ## Log to file
  if (logFilePath) {
    await fs.appendFile(logFilePath, `${completeLogMsg}\r\n`);
  } else {
    throw new Error('Log file path has not been set');
  }
}

module.exports = {
  setGlobalLogFilePathMut,
  log,
};
