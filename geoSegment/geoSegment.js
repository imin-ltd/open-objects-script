const path = require('path');
const { backOff } = require('exponential-backoff');
const fs = require('fs-extra');
const fsPromises = require('fs').promises;
const geolib = require('geolib');
const Joi = require('joi');
const moment = require('moment-timezone');
const { performance } = require('perf_hooks');

const { generateDatesWithinTimeWindow } = require('./utils/generateDatesWithinTimeWindow');
const { getFirehosePage } = require('./utils/getFirehosePage');
const { wait } = require('./utils/wait');
const { loggableAxiosError } = require('./utils/loggableAxiosError');
const { Http404Error } = require('./utils/errors');
const { hashString } = require('./utils/hashString');
const { log, setGlobalLogFilePathMut } = require('./utils/log');
const { createEmptyFile } = require('./utils/createEmptyFile');

const scriptStartTime = performance.now();

// File paths
const CONFIG_FILE_PATH = path.join(__dirname, 'config.json');
const OUTPUT_DIRECTORY_PATH = path.join(__dirname, 'output');
const OUTPUT_SESSION_SERIES_DIRECTORY_PATH = path.join(OUTPUT_DIRECTORY_PATH, 'sessionseries');
const OUTPUT_SEGMENTS_DIRECTORY_PATH = path.join(OUTPUT_DIRECTORY_PATH, 'segments');
const OUTPUT_LOG_FILE_PATH = path.join(OUTPUT_DIRECTORY_PATH, 'log.txt');

// Parameters controlling retry backoff
const RETRY_BACKOFF_MIN_SECONDS = 1;
const RETRY_BACKOFF_MAX_SECONDS = 1024; // ~ 17 mins
const RETRY_BACKOFF_EXPONENTIATION_RATE = 2;
const MIN_REQUEST_DELAY_SECONDS = 0.1;

// Event Schedule generation
const SCHEDULE_TYPE = 'Schedule';
const MINIMAL_EVENT_SCHEDULE_SCHEMA = Joi.object().keys({
  // If this array has length 0, an infinite loop will occur in the date
  // generation algorithm below.
  byDay: Joi.array().items(Joi.string()).min(1).required(),
  startTime: Joi.string().required(),
  endTime: Joi.string().required(),
  idTemplate: Joi.string().required(),
});

/**
 * @typedef {import('axios').AxiosError} AxiosError
 * @typedef {import('axios').AxiosRequestConfig} AxiosRequestConfig
 * @typedef {import('axios').AxiosResponse} AxiosResponse
 */

/**
 * @typedef {{
 *   geo?: {
 *     latitude: number,
 *     longitude: number,
 *   }
 * }} OaLocation
 *
 * @typedef {{
 *   location?: OaLocation,
 *   'beta:affiliatedLocation'?: OaLocation,
 *   'imin:segment'?: string[],
 *   [k:string]: unknown
 * }} SessionSeriesData
 *
 * @typedef {{
 *  id: string,
 *  state: 'updated' | 'deleted',
 *  kind: 'SessionSeries',
 *  data: SessionSeriesData
 * }} SessionSeriesRpdeItem
 * @typedef {{
 *  startDate?: string,
 *  superEvent?: string,
 *  id?: string,
 *  [k:string]: unknown
 * }} ScheduledSessionData
 * @typedef {{
 *  id: string,
 *  state: 'updated' | 'deleted',
 *  kind: 'ScheduledSession',
 *  data: ScheduledSessionData
 * }} ScheduledSessionRpdeItem
 * @typedef {{
 *  latitude: number,
 *  longitude: number,
 *  radius: number,
 *  identifier: string
 * }} Segment
 */

/**
 * @param {number} seconds
 */
function secondsToMilliseconds(seconds) {
  return seconds * 1000;
}

/**
 * @param {string} segmentIdentifier
 */
function getSegmentIndexFilePath(segmentIdentifier) {
  return path.join(OUTPUT_SEGMENTS_DIRECTORY_PATH, segmentIdentifier, 'index.txt');
}

/**
 * @param {string} sessionSeriesIdHash
 */
function getSessionSeriesFilePath(sessionSeriesIdHash) {
  return path.join(OUTPUT_SESSION_SERIES_DIRECTORY_PATH, `${sessionSeriesIdHash}.json`);
}

/**
 * @param {string} segmentIdentifier
 * @param {string} scheduledSessionIdHash
 */
function getScheduledSessionFilePath(segmentIdentifier, scheduledSessionIdHash) {
  return path.join(OUTPUT_SEGMENTS_DIRECTORY_PATH, segmentIdentifier, `${scheduledSessionIdHash}.json`);
}

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

function logExitTime() {
  const scriptEndTime = performance.now();
  const totalTimeMillis = scriptEndTime - scriptStartTime;
  const totalTimeSeconds = totalTimeMillis / 1000;
  log('info', `Script run time: ${totalTimeSeconds.toFixed(2)} seconds`);
}

/**
 * @param {Segment[]} segments
 */
async function emptyOrMakeOutputDirectories(segments) {
  // ## Create or empty root output/ directory
  if (await fs.pathExists(OUTPUT_DIRECTORY_PATH)) {
    await fs.emptyDir(OUTPUT_DIRECTORY_PATH);
  } else {
    await fsPromises.mkdir(OUTPUT_DIRECTORY_PATH, { recursive: true });
  }
  // ## Create SessionSeries directory
  await fsPromises.mkdir(OUTPUT_SESSION_SERIES_DIRECTORY_PATH);
  // ## Create segment directories
  await fsPromises.mkdir(OUTPUT_SEGMENTS_DIRECTORY_PATH);
  for (const { identifier } of segments) {
    const segmentDirectoryPath = path.join(OUTPUT_SEGMENTS_DIRECTORY_PATH, identifier);
    await fsPromises.mkdir(segmentDirectoryPath);

    // Create index file
    await createEmptyFile(getSegmentIndexFilePath(identifier));
  }
  // ## Create log.txt file
  await createEmptyFile(OUTPUT_LOG_FILE_PATH);
  setGlobalLogFilePathMut(OUTPUT_LOG_FILE_PATH);
}

/**
 * @param {ScheduledSessionData} scheduledSessionData
 * @param {SessionSeriesData} sessionSeries
 */
async function linkScheduledSessionAndSessionSeriesAndWrite(scheduledSessionData, sessionSeries) {
  // Link ScheduledSession with SessionSeries
  const linkedScheduledSessionData = {
    ...scheduledSessionData,
    superEvent: sessionSeries,
  };

  // Write ScheduledSession into each output segment directory
  for (const segmentIdentifier of sessionSeries['imin:segment']) {
    const scheduledSessionIdHash = hashString(linkedScheduledSessionData.id);
    const scheduledSessionFilePath = getScheduledSessionFilePath(segmentIdentifier, scheduledSessionIdHash);

    // Check if the ScheduledSession already exists
    /** @type {boolean} */
    let isExistingScheduledSession;
    {
      const existingScheduledSession = await readJsonNullIfNotExists(scheduledSessionFilePath);
      isExistingScheduledSession = Boolean(existingScheduledSession);
      if (isExistingScheduledSession) {
        if (existingScheduledSession.id !== linkedScheduledSessionData.id) {
          // Hash clash
          await log('warn', `Already downloaded ScheduledSession:${existingScheduledSession.id} and newly received ScheduledSession: ${linkedScheduledSessionData.id} are not the same despite having the same hash: ${scheduledSessionIdHash}`);
          continue;
        }
      }
    }
    // ScheduledSession does not already exist, so save it
    await fs.writeJson(scheduledSessionFilePath, linkedScheduledSessionData);

    // Write to the index file if this ScheduledSession did not already exist
    if (!isExistingScheduledSession) {
      const indexFilePath = getSegmentIndexFilePath(segmentIdentifier);
      await fs.appendFile(indexFilePath, `${scheduledSessionIdHash}.json\r\n`);
    }
  }
}

/**
 * @param {SessionSeriesRpdeItem[]} items
 * @param {Segment[]} segments
 */
async function processSessionSeriesItems(items, segments) {
  for (const item of items) {
    // Filter deleted SessionSeries
    if (item.state === 'deleted') {
      return;
    }
    // If for some reason, the SessionSeries doesn't have an ID, don't process it.
    if (!item.id) {
      return;
    }

    // Filter virtual sessions
    let sessionSeriesPhysicalLocationGeo;
    if (item.data.location && item.data.location.geo) {
      sessionSeriesPhysicalLocationGeo = item.data.location.geo;
    } else if (item.data['beta:affiliatedLocation'] && item.data['beta:affiliatedLocation'].geo) {
      sessionSeriesPhysicalLocationGeo = item.data['beta:affiliatedLocation'].geo;
    } else {
      return;
    }

    /** @type {SessionSeriesData} */
    const sessionSeriesData = {
      ...item.data,
      'imin:segment': [],
    };

    // Add imin:segment if applicable
    for (const segment of segments) {
      const segmentRadiusInMeters = segment.radius * 1000;
      const distanceBetweenGeos = geolib.getDistance(
        { latitude: sessionSeriesPhysicalLocationGeo.latitude, longitude: sessionSeriesPhysicalLocationGeo.longitude },
        { latitude: segment.latitude, longitude: segment.longitude },
      );
      if (distanceBetweenGeos <= segmentRadiusInMeters) {
        sessionSeriesData['imin:segment'].push(segment.identifier);
      }
    }
    // If there are no segments, drop the SessionSeries as it will not appear in an output folder and therefore we don't need to store it
    if (sessionSeriesData['imin:segment'].length === 0) {
      return;
    }

    // Validate Schedules and generate ScheduledSessions if needed
    const scheduledSessionsForEveryEventSchedule = [];
    const correctedEventSchedules = []; // EventSchedules that are type Schedule but are not valid should be changed to PartialSchedule, and not generate ScheduledSessions
    if (Array.isArray(sessionSeriesData.eventSchedule)) {
      for (const eventSchedule of sessionSeriesData.eventSchedule) {
        // Validate
        if (eventSchedule.type !== SCHEDULE_TYPE) {
          continue;
        }
        const joiValidation = MINIMAL_EVENT_SCHEDULE_SCHEMA.validate(eventSchedule, {
          allowUnknown: true,
        });

        if (joiValidation.error) {
          const correctedSchedule = {
            ...eventSchedule,
            type: 'PartialSchedule',
          };
          correctedEventSchedules.push(correctedSchedule);
          continue;
        }

        // Generate
        correctedEventSchedules.push(eventSchedule);
        scheduledSessionsForEveryEventSchedule.push(
          ...generateDatesWithinTimeWindow(eventSchedule),
        );
      }
    }

    // Write SessionSeries to file
    const sessionSeriesDataWithCorrectedEventSchedules = {
      ...sessionSeriesData,
      eventSchedule: correctedEventSchedules,
    };

    const sessionSeriesIdHash = hashString(item.id);
    const sessionSeriesFilePath = getSessionSeriesFilePath(sessionSeriesIdHash);
    {
      // If the path already exists, then either the SessionSeries has already been processed and saved, or there's a hash clash
      const existingSessionSeries = await readJsonNullIfNotExists(sessionSeriesFilePath);
      if (existingSessionSeries) {
        // Hash clash
        if (existingSessionSeries.id !== item.id) {
          await log('warn', `Already downloaded SessionSeries:${existingSessionSeries.id} and newly received SessionSeries: ${item.id} are not the same despite having the same hash: ${sessionSeriesIdHash}`);
          return;
        }
      // If the IDs are the same, then no need to do anything
      }
    }
    await fs.writeJson(sessionSeriesFilePath, sessionSeriesDataWithCorrectedEventSchedules);

    // Write ScheduledSessions to file
    for (const generatedScheduledSession of scheduledSessionsForEveryEventSchedule) {
      await linkScheduledSessionAndSessionSeriesAndWrite(generatedScheduledSession, sessionSeriesData);
    }
  }
}

/**
 * @param {string} url
 * @param {string} firehoseApiKey
 */
async function getFirehosePageWithExponentialBackoff(url, firehoseApiKey) { // eslint-disable-line consistent-return
  // the consistent-return eslint rule does not seem to understand that a
  // process.exit() call cannot be followed by any sort of return.
  try {
    return await backOff(async () => (
      await getFirehosePage(url, firehoseApiKey)
    ), {
      maxDelay: secondsToMilliseconds(RETRY_BACKOFF_MAX_SECONDS),
      startingDelay: secondsToMilliseconds(RETRY_BACKOFF_MIN_SECONDS),
      timeMultiple: RETRY_BACKOFF_EXPONENTIATION_RATE,
      numOfAttempts: 10,
      delayFirstAttempt: false,
      async retry(error) {
        if (error instanceof Http404Error) {
          await log('error', `URL ("${error.url}") not found. Please check the value for <rpde-endpoint>.`);
          logExitTime();
          process.exit(1);
        }
        const loggableError = (error && error.isAxiosError)
          ? loggableAxiosError(error)
          : error;
        await log('warn', `Retrying [page: "${url}"] due to error:`, loggableError);
        return true;
      },
    });
  } catch (error) {
    await log('error', `Cannot download Firehose pages [at page "${url}"], backoff limit reached, terminating script`);
    await log('error', error);
    logExitTime();
    process.exit(1);
  }
}

/**
 * @typedef {ScheduledSessionRpdeItem[]|SessionSeriesRpdeItem[]} Items
 * @param {string} firehosePageUrl
 * @param {string} firehoseApiKey
 * @param {(items: Items, segments: Segment[]) => void} processItemsFn
 * @param {Segment[]} segments
 */
async function downloadFirehosePageAndProcess(firehosePageUrl, firehoseApiKey, processItemsFn, segments) {
  // Get page from Firehose
  let nextUrl = firehosePageUrl;
  while (true) {
    const { nextNextUrl, items } = await getFirehosePageWithExponentialBackoff(nextUrl, firehoseApiKey);

    // Process items
    await processItemsFn(items, segments);

    // Have we reached the end of the feed?
    if (nextNextUrl === nextUrl) {
      break;
    }

    // If we haven't reached the end of the feed, carry on
    nextUrl = nextNextUrl;
    await wait(MIN_REQUEST_DELAY_SECONDS * 1000);
  }
}

/**
 * @param {ScheduledSessionRpdeItem[]} items
 */
async function processScheduledSessionItems(items) {
  for (const scheduledSessionItem of items) {
    // Filter deleted ScheduledSessions
    if (scheduledSessionItem.state === 'deleted') {
      return;
    }
    // If for some reason, the ScheduledSession doesn't have an ID, don't process it.
    if (!scheduledSessionItem.id) {
      return;
    }

    // Filter ScheduledSessions in the past
    if (!scheduledSessionItem.data.startDate || moment(scheduledSessionItem.data.startDate).isBefore(moment())) {
      return;
    }

    // Does ScheduledSession have downloaded superEvent?
    const scheduledSessionSuperEventIdHash = hashString(scheduledSessionItem.data.superEvent);
    const sessionSeriesFilePath = getSessionSeriesFilePath(scheduledSessionSuperEventIdHash);

    // Get SessionSeries
    const sessionSeries = await readJsonNullIfNotExists(sessionSeriesFilePath);
    if (!sessionSeries) { // File doesn't exist
      return;
    }

    // Check SessionSeries.id and ScheduledSession.superEvent match in case there has been some hash clash
    if (scheduledSessionItem.data.superEvent !== sessionSeries.id) {
      await log('warn', `ScheduledSession superEvent:${scheduledSessionItem.data.superEvent} and SessionSeries ID: ${sessionSeries.id} are not the same despite having the same hash: ${scheduledSessionSuperEventIdHash}`);
      return;
    }

    // Link ScheduledSession and SessionSeries, and write
    await linkScheduledSessionAndSessionSeriesAndWrite(scheduledSessionItem.data, sessionSeries);
  }
}

(async () => {
  // Load config file into memory
  const { firehoseBaseUrl, firehoseApiKey, segments } = await fs.readJson(CONFIG_FILE_PATH);

  // Create output/ directories
  await emptyOrMakeOutputDirectories(segments);

  await log('info', 'geoSegment() - starting..');

  // Download and process SessionSeries
  const sessionSeriesFirehoseUrl = `${firehoseBaseUrl}session-series`;
  await log('info', 'geoSegment() - downloading SessionSeries feed..');
  await downloadFirehosePageAndProcess(sessionSeriesFirehoseUrl, firehoseApiKey, processSessionSeriesItems, segments);
  await log('info', 'geoSegment() - downloaded SessionSeries feed');

  // Download and process ScheduledSessions into segment directories
  const scheduledSessionFirehoseUrl = `${firehoseBaseUrl}scheduled-sessions`;
  await log('info', 'geoSegment() - downloading ScheduledSession feed..');
  await downloadFirehosePageAndProcess(scheduledSessionFirehoseUrl, firehoseApiKey, processScheduledSessionItems, segments);
  await log('info', 'geoSegment() - downloaded ScheduledSession feed. Finished');
  logExitTime();
})();

process.on('uncaughtException', (error) => {
  log('error', 'Fatal exception', error);
  logExitTime();
});
