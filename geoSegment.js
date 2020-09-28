/* eslint-disable no-console */
const fsPromises = require('fs').promises;
const fs = require('fs-extra');
const geolib = require('geolib');
const Joi = require('joi');
const moment = require('moment-timezone');

const { generateDatesWithinTimeWindow } = require('./utils/generateDatesWithinTimeWindow');
const { getFirehosePage } = require('./utils/getFirehosePage');
const { wait } = require('./utils/wait');
const { loggableAxiosError } = require('./utils/loggableAxiosError');
const { emptyOrMakeDirectory } = require('./utils/emptyOrMakeDirectory');
const { Http404Error } = require('./utils/errors');

// File paths
const CONFIG_FILE_PATH = './config.json';
const SESSION_SERIES_DIRECTORY_PATH = './sessionseries';
const OUTPUT_DIRECTORY_PATH = './output';

// Parameters controlling retry backoff
const RETRY_BACKOFF_MIN = 1;
const RETRY_BACKOFF_MAX = 1024; // ~ 17 mins
const RETRY_BACKOFF_EXPONENTIATION_RATE = 2;
const MIN_REQUEST_DELAY_SECONDS = 0.1;

const SESSION_SERIES_FILE_PATHS = [];

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
 * @typedef {{
 * location?: {
 *  geo?: {
 *   latitude: number,
 *   longitude: number,
 *   }
 *  },
 * [k:string]: unknown
 * }} SessionSeriesData
 * @typedef {{
 *  id: string,
 *  state: 'updated' | 'deleted',
 *  kind: 'SessionSeries',
 *  data: SessionSeriesData
 * }} SessionSeriesItem
 * @typedef {{
 *  startDate?: string,
 *  superEvent?: string,
 *  [k:string]: unknown
 * }} ScheduledSessionData
 * @typedef {{
 *  id: string,
 *  state: 'updated' | 'deleted',
 *  kind: 'ScheduledSession',
 *  data: ScheduledSessionData
 * }} ScheduledSessionItem
 * @typedef {{
 *  latitude: number,
 *  longitude: number,
 *  radius: number,
 *  identifier: string
 * }} Segment
 */

/**
 * @param {Segment[]} segments
 */
async function createSegmentDirectories(segments) {
  // Create segment directories;
  segments.map((segment) => segment.identifier)
    .map(async (segmentIdentifier) => {
      const segmentDirectoryPath = `${OUTPUT_DIRECTORY_PATH}/${segmentIdentifier}`;
      await emptyOrMakeDirectory(segmentDirectoryPath);

      // Create index file
      await fsPromises.writeFile(`${OUTPUT_DIRECTORY_PATH}/${segmentIdentifier}/index.txt`, '');
    });
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
    const filename = `${encodeURIComponent(linkedScheduledSessionData.id)}.json`;
    await fs.writeJson(`${OUTPUT_DIRECTORY_PATH}/${segmentIdentifier}/${filename}`, linkedScheduledSessionData);
    await fs.appendFile(`${OUTPUT_DIRECTORY_PATH}/${segmentIdentifier}/index.txt`, `${filename}\r\n`);
  }
}

/**
 * There is a system filepath limit of 255 characters in most file systems.
 * All sessionseries filepaths with be of the form `./sessionseries/${encodedFilename}.json`
 * Therefore the encodedFilename has to be less than 234 characters in length (./sessionseries/.json is 21 characters)
 *
 * @param {string} sessionSeriesId
 */
function getSessionSeriesFilePath(sessionSeriesId) {
  let encodedFilename = encodeURIComponent(sessionSeriesId);
  if (encodedFilename.length > 234) {
    encodedFilename = encodedFilename.slice(0, 234);
  }
  return `./${SESSION_SERIES_DIRECTORY_PATH}/${encodedFilename}.json`;
}

/**
 *
 * @param {RpdeItem<SessionSeriesData>[]} items
 * @param {Segment[]} segments
 */
async function processSessionSeriesItems(items, segments) {
  items.forEach(async (item) => {
    // Filter deleted SessionSeries
    if (item.state === 'deleted') {
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
        if (eventSchedule.type !== SCHEDULE_TYPE) {
          continue;
        }
        // Validate
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

        scheduledSessionsForEveryEventSchedule.push(
          ...generateDatesWithinTimeWindow(eventSchedule),
        );
      }
    }

    // Write SessionSeries to file
    const sessionSeriesFilePath = getSessionSeriesFilePath(item.id);
    await fs.writeJson(sessionSeriesFilePath, sessionSeriesData);
    SESSION_SERIES_FILE_PATHS.push(sessionSeriesFilePath);

    // Write ScheduledSessions to file
    for (const generatedScheduledSession of scheduledSessionsForEveryEventSchedule) {
      await linkScheduledSessionAndSessionSeriesAndWrite(generatedScheduledSession, sessionSeriesData);
    }
  });
}

/**
 * @param {string} firehoseBaseUrl
 * @param {string} firehoseApiKey
 * @param {Segment[]} segments
 */
async function downloadSessionSeries(firehoseBaseUrl, firehoseApiKey, segments) {
  console.log('downloadAndWriteSessionSeries() - starting');
  // Get SessionSeries from Firehose
  let nextUrl = `${firehoseBaseUrl}session-series`;
  let backoffTimeInSeconds = RETRY_BACKOFF_MIN;
  while (true) {
    let nextNextUrl;
    let items;
    try {
      ({ nextNextUrl, items } = await getFirehosePage(nextUrl, firehoseApiKey));
    } catch (error) {
      if (error instanceof Http404Error) {
        console.error(`ERROR: URL ("${error.url}") not found. Please check the value for <rpde-endpoint>.`);
        process.exit(1);
      }
      const loggableError = (error && error.isAxiosError)
        ? loggableAxiosError(error)
        : error;
      console.warn(`WARN: Retrying [page: "${nextUrl}"] due to error:`, loggableError);
      if (backoffTimeInSeconds > RETRY_BACKOFF_MAX) {
        console.error('ERROR: Cannot download Firehose pages, backoff limit reached, terminating script');
        process.exit(1);
      }
      await wait(backoffTimeInSeconds * 1000);
      backoffTimeInSeconds *= RETRY_BACKOFF_EXPONENTIATION_RATE;
      continue;
    }

    // Process SessionSeries items
    await processSessionSeriesItems(items, segments);

    // Have we reached the end of the feed?
    if (nextNextUrl === nextUrl) {
      break;
    }

    // If we haven't reached the end of the feed, carry on
    nextUrl = nextNextUrl;
    backoffTimeInSeconds = RETRY_BACKOFF_MIN;
    await wait(MIN_REQUEST_DELAY_SECONDS * 1000);
  }
}

/**
 * @param {ScheduledSessionItem[]} items
 */
async function processScheduledSessionItems(items) {
  items.forEach(async (scheduledSessionItem) => {
    // Filter deleted ScheduledSessions
    if (scheduledSessionItem.state === 'deleted') {
      return;
    }

    // Filter ScheduledSessions in the past
    if (!scheduledSessionItem.data.startDate || moment(scheduledSessionItem.data.startDate).isBefore(moment())) {
      return;
    }

    // Does ScheduledSession have downloaded superEvent?
    const scheduledSessionSuperEventId = scheduledSessionItem.data.superEvent;
    const sessionSeriesFilePath = getSessionSeriesFilePath(scheduledSessionSuperEventId);
    if (!SESSION_SERIES_FILE_PATHS.includes(sessionSeriesFilePath)) {
      return;
    }

    // Get SessionSeries
    const sessionSeries = await fs.readJson(sessionSeriesFilePath);

    // Link ScheduledSession and SessionSeries, and write
    await linkScheduledSessionAndSessionSeriesAndWrite(scheduledSessionItem.data, sessionSeries);
  });
}

async function downloadScheduledSessions(firehoseBaseUrl, firehoseApiKey) {
  console.log('downloadAndWriteScheduledSessions() - starting');
  // Get ScheduledSessions from Firehose
  let nextUrl = `${firehoseBaseUrl}scheduled-sessions`;
  let backoffTimeInSeconds = RETRY_BACKOFF_MIN;
  while (true) {
    let nextNextUrl;
    let items;
    try {
      ({ nextNextUrl, items } = await getFirehosePage(nextUrl, firehoseApiKey));
    } catch (error) {
      if (error instanceof Http404Error) {
        console.error(`ERROR: URL ("${error.url}") not found. Please check the value for <rpde-endpoint>.`);
        process.exit(1);
      }
      const loggableError = (error && error.isAxiosError)
        ? loggableAxiosError(error)
        : error;
      console.warn(`WARN: Retrying [page: "${nextUrl}"] due to error:`, loggableError);
      if (backoffTimeInSeconds > RETRY_BACKOFF_MAX) {
        console.error('ERROR: Cannot download Firehose pages, backoff limit reached, terminating script');
        process.exit(1);
      }
      await wait(backoffTimeInSeconds * 1000);
      backoffTimeInSeconds *= RETRY_BACKOFF_EXPONENTIATION_RATE;
      continue;
    }

    // Process SessionSeries items
    await processScheduledSessionItems(items);

    // Have we reached the end of the feed?
    if (nextNextUrl === nextUrl) {
      break;
    }

    // If we haven't reached the end of the feed, carry on
    nextUrl = nextNextUrl;
    backoffTimeInSeconds = RETRY_BACKOFF_MIN;
    await wait(MIN_REQUEST_DELAY_SECONDS * 1000);
  }
}

(async () => {
  console.log('geoSegment() - starting..');

  // Load config file into memory
  const { firehoseBaseUrl, firehoseApiKey, segments } = await fs.readJson(CONFIG_FILE_PATH);

  // Create SessionSeries directory
  await emptyOrMakeDirectory(SESSION_SERIES_DIRECTORY_PATH);

  // Create output directory
  await emptyOrMakeDirectory(OUTPUT_DIRECTORY_PATH);

  // Create Segment directories
  await createSegmentDirectories(segments);

  // Download and write SessionSeries
  await downloadSessionSeries(firehoseBaseUrl, firehoseApiKey, segments);

  // Download and write ScheduledSessions into segment directories
  await downloadScheduledSessions(firehoseBaseUrl, firehoseApiKey);

  console.log('geoSegment() - finished');
  process.exit(0);
})();
