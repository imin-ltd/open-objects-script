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
const { hashString } = require('./utils/hashString');

// File paths
const CONFIG_FILE_PATH = './config.json';
const SESSION_SERIES_DIRECTORY_PATH = './sessionseries';
const OUTPUT_DIRECTORY_PATH = './output';

// Parameters controlling retry backoff
const RETRY_BACKOFF_MIN = 1;
const RETRY_BACKOFF_MAX = 1024; // ~ 17 mins
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
 * }} SessionSeriesItem
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
 * }} ScheduledSessionItem
 * @typedef {{
 *  latitude: number,
 *  longitude: number,
 *  radius: number,
 *  identifier: string
 * }} Segment
 */

/**
 * @param {string} segmentIdentifier
 */
function getSegmentIndexFilePath(segmentIdentifier) {
  return `${OUTPUT_DIRECTORY_PATH}/${segmentIdentifier}/index.txt`;
}

/**
 * @param {string} sessionSeriesIdHash
 */
function getSessionSeriesFilePath(sessionSeriesIdHash) {
  return `${SESSION_SERIES_DIRECTORY_PATH}/${sessionSeriesIdHash}.json`;
}

/**
 * @param {string} segmentIdentifier
 * @param {string} scheduledSessionIdHash
 */
function getScheduledSessionFilePath(segmentIdentifier, scheduledSessionIdHash) {
  return `${OUTPUT_DIRECTORY_PATH}/${segmentIdentifier}/${scheduledSessionIdHash}.json`;
}

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
      await fsPromises.writeFile(getSegmentIndexFilePath(segmentIdentifier), '');
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
    const scheduledSessionIdHash = hashString(linkedScheduledSessionData.id);
    const scheduledSessionFilePath = getScheduledSessionFilePath(segmentIdentifier, scheduledSessionIdHash);

    // Check if the ScheduledSession already exists
    {
      const existingScheduledSession = await fs.readJson(scheduledSessionFilePath, { throws: false });
      if (existingScheduledSession) {
        if (existingScheduledSession.id !== linkedScheduledSessionData.id) {
          // Hash clash
          console.warn(`Already downloaded ScheduledSession:${existingScheduledSession.id} and newly received ScheduledSession: ${linkedScheduledSessionData.id} are not the same despite having the same hash: ${scheduledSessionIdHash}`);
          continue;
        } else {
          // existingScheduledSession and linkedScheduledSessionData are the same and it has already been processed and saved, so do nothing
          continue;
        }
      }
    }
    // ScheduledSession does not already exist, so save it
    await fs.writeJson(scheduledSessionFilePath, linkedScheduledSessionData);

    // Check if the ID has been written to the index file/write the ID to the index file
    const indexFilePath = getSegmentIndexFilePath(segmentIdentifier);
    const existingIndex = await fs.readFile(indexFilePath, 'utf8');
    if (!existingIndex.includes(scheduledSessionIdHash)) {
      await fs.appendFile(indexFilePath, `${scheduledSessionIdHash}\r\n`);
    }
  }
}

/**
 * @param {SessionSeriesItem[]} items
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
      const existingSessionSeries = await fs.readJson(sessionSeriesFilePath, { throws: false });
      if (existingSessionSeries) {
        // Hash clash
        if (existingSessionSeries && existingSessionSeries.id !== item.id) {
          console.warn(`Already downloaded SessionSeries:${existingSessionSeries.id} and newly received SessionSeries: ${item.id} are not the same despite having the same hash: ${sessionSeriesIdHash}`);
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
 * @typedef {ScheduledSessionItem[]|SessionSeriesItem[]} Items
 * @param {string} firehosePageUrl
 * @param {string} firehoseApiKey
 * @param {(items: Items, segments: Segment[]) => void} processItemsFn
 * @param {Segment[]} segments
 */
async function downloadFirehosePageAndProcess(firehosePageUrl, firehoseApiKey, processItemsFn, segments) {
  // Get page from Firehose
  let nextUrl = firehosePageUrl;
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

    // Process items
    await processItemsFn(items, segments);

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
    const sessionSeries = await fs.readJson(sessionSeriesFilePath, { throws: false });
    if (!sessionSeries) { // File doesn't exist
      return;
    }

    // Check SessionSeries.id and ScheduledSession.superEvent match in case there has been some hash clash
    if (scheduledSessionItem.data.superEvent !== sessionSeries.id) {
      console.warn(`ScheduledSession superEvent:${scheduledSessionItem.data.superEvent} and SessionSeries ID: ${sessionSeries.id} are not the same despite having the same hash: ${scheduledSessionSuperEventIdHash}`);
      return;
    }

    // Link ScheduledSession and SessionSeries, and write
    await linkScheduledSessionAndSessionSeriesAndWrite(scheduledSessionItem.data, sessionSeries);
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

  // Download and process SessionSeries
  const sessionSeriesFirehoseUrl = `${firehoseBaseUrl}session-series`;
  await downloadFirehosePageAndProcess(sessionSeriesFirehoseUrl, firehoseApiKey, processSessionSeriesItems, segments);

  // Download and process ScheduledSessions into segment directories
  const scheduledSessionFirehoseUrl = `${firehoseBaseUrl}scheduled-sessions`;
  await downloadFirehosePageAndProcess(scheduledSessionFirehoseUrl, firehoseApiKey, processScheduledSessionItems, segments);

  console.log('geoSegment() - finished');
})();
