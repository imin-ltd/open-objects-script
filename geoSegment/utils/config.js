const s = require('@imin/speck');
const fs = require('fs-extra');
const path = require('path');
const { log } = require('./log');

const CONFIG_FILE_PATH = path.join(__dirname, '..', 'config.json');

const Config = s.type({
  firehoseBaseUrl: s.string,
  firehoseApiKey: s.string,
  segments: s.array(s.type({
    identifier: s.string,
    latitude: s.float,
    longitude: s.float,
    radius: s.float,
    attendanceModeFilter: s.union([
      s.literal('virtual-only'),
      s.union([
        s.literal('physical-only'),
        s.literal('all'),
      ]),
    ]),
  })),
});

async function loadConfig() {
  const configOrError = s.validate(Config, await fs.readJson(CONFIG_FILE_PATH), { skipStrict: true });
  if (configOrError instanceof s.SpeckValidationErrors) {
    await log('error', `Config file is invalid: ${configOrError.summary}`);
    process.exit(1);
  }
  return configOrError;
}

/**
 * @typedef {import('@imin/speck/lib/types').TypeOf<Config>} ConfigType
 * @typedef {ConfigType['segments'][number]} ConfigSegmentType
 */

module.exports = {
  loadConfig,
};
