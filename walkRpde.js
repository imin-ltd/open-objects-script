/**
 * N.B. the output-file-path directory will be removed when this script is run,
 * so take care not to set it to `/`. The directory should only hold files
 * outputted by this script and nothing else.
 */
const { default: axios } = require('axios');
const fsPromises = require('fs').promises;
const fs = require('fs-extra');
const { performance } = require('perf_hooks');
const util = require('util');

/**
 * Set this to true in order to log timings of tasks within the script
 */
const DO_DEBUG_PERFORMANCE = false;

// Parameters controlling retry backoff
const RETRY_BACKOFF_MIN = 1;
const RETRY_BACKOFF_MAX = 1024; // ~ 17 mins
const RETRY_BACKOFF_EXPONENTIATION_RATE = 2;

const MIN_REQUEST_DELAY_SECONDS = 0.1;

class IminValidationError extends Error { }

class Http404Error extends Error {
  /**
   * @param {string} url 
   */
  constructor(url) {
    super(`Page not found: "${url}"`);
    this.url = url;
  }
}

/**
 * @param {number} ms
 */
function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * @param {string} msg
 */
function logPerformance(msg) {
  if (DO_DEBUG_PERFORMANCE) {
    console.log(msg);
  }
}

/**
 * Axios errors have a gargantuan amount of barely relevant data that makes
 * them unserializable. Here, it is simplified so that it can be logged.
 *
 * @param {import('axios').AxiosError} error
 */
function loggableAxiosError(error) {
  return {
    message: error.message,
    url: error.config.url,
    response: error.response && {
      status: error.response.status,
      headers: error.response.headers,
      data: error.response.data,
    },
  };
}

/**
 * @param {object} args
 * @param {string} args.apiKey
 * @param {string} args.outputFilePath
 * @param {string} args.indexFilePrefix
 * @param {string} args.indexFilename
 * @param {string} args.datestamp
 * @param {number} pageNum
 * @param {string} pageUrl
 * @returns {Promise<string>} url for next page
 */
async function downloadPage({ apiKey, outputFilePath, indexFilePrefix, indexFilename, datestamp }, pageNum, pageUrl) {
  // ## Download the page
  logPerformance(`[${performance.now()}] Downloading ${pageUrl}..`);
  const rpdePage = await axios(pageUrl, {
    validateStatus: (status) =>
      // HTTP 404 is allowed so that we can assign special treatment
      (status >= 200 && status < 300) || status === 404,
    headers: { 'x-api-key': apiKey },
  });
  if (rpdePage.status === 404) {
    throw new Http404Error(pageUrl);
  }
  // ## Validate the page
  if (typeof rpdePage.data.next !== 'string' || !rpdePage.data.next) {
    throw new IminValidationError(`RPDE .next should be a non-empty string. Value: ${util.inspect(rpdePage.data.next)}`);
  }
  if (!Array.isArray(rpdePage.data.items)) {
    throw new IminValidationError(`RPDE .items should be an array. Value: ${util.inspect(rpdePage.data.items)}`);
  }
  // use findIndex() rather than find() because find() returns `undefined`
  // for "not found", which is indistinguishable from "i found one and it is
  // undefined". We also want to validate that no items are undefined
  const invalidItemIndex = rpdePage.data.items.findIndex(item => !item || (item.state !== 'updated' && item.state !== 'deleted'));
  if (invalidItemIndex > 0) {
    const invalidItem = rpdePage.data.items[invalidItemIndex];
    const invalidItemId = invalidItem && invalidItem.id;
    throw new IminValidationError(`item [index: "${invalidItemIndex}" ; id: "${invalidItemId}"] should be non-null and have state=updated|deleted. It does not`);
  }
  logPerformance(`[${performance.now()}] Downloaded`);
  // ## Split the page into files - one file for each item
  const filenames = [];
  for (const i in rpdePage.data.items) {
    logPerformance(`[${performance.now()}] Processing item ${i}..`);
    const item = rpdePage.data.items[i];
    // ### Exclude deleted records
    if (item.state === 'updated') {
      logPerformance(`[${performance.now()}] Saving item ${i}..`);
      // ### Save file
      const filename = `${indexFilePrefix}-rpde-${pageNum}-${i}.json`;
      const filePath = `${outputFilePath}${filename}`;
      item.datestamp = datestamp; // ! mutate item
      await fsPromises.writeFile(filePath, JSON.stringify(item));
      filenames.push(filename);
      logPerformance(`[${performance.now()}] Saved item ${i}`);
    }
    logPerformance(`[${performance.now()}] Processed item ${i}`);
  }
  // ## Save file entries to index
  const indexEntries = filenames.map(filename => `<a href="/${filename}">file</a>`).join('');
  await fsPromises.appendFile(indexFilename, indexEntries);
  const nextPageUrl = rpdePage.data.next;
  const numItems = rpdePage.data.items.length;
  console.log(`got page: ${pageNum}, with next url: ${nextPageUrl}, num items: ${numItems}`);
  return nextPageUrl;
}

(async () => {
  if (process.argv.length !== 7) {
    console.log(`Usage: node walkRpde.js <rpde-endpoint> <api-key> <output-file-path> <index-file-prefix> <request-delay-seconds>\n'`);
    process.exit(1);
  }

  const [rpdeEndpoint, apiKey, outputFilePath, indexFilePrefix, requestDelaySecondsStr] = process.argv.slice(2);

  if (outputFilePath === '/') {
    console.error('Cannot delete "/"');
    process.exit(1);
  }
  const date = new Date();
  const datestamp = `${date.getDate().toString().padStart(2, '0')}/${date.getMonth().toString().padStart(2, '0')}/${date.getFullYear()}`;
  const indexFilename = `${outputFilePath}${indexFilePrefix}-index.html`;
  const requestDelaySeconds = (() => {
    // Request delay seconds has a minimum value
    const initialValue = Number(requestDelaySecondsStr);
    if (Number.isNaN(initialValue)) { return MIN_REQUEST_DELAY_SECONDS; }
    return Math.max(initialValue, MIN_REQUEST_DELAY_SECONDS);
  })();


  // ## Wipe existing output directory
  if (await fs.pathExists(outputFilePath)) {
    await fs.emptyDir(outputFilePath);
  } else {
    await fsPromises.mkdir(outputFilePath);
  }

  // ## Create empty index file
  await fsPromises.writeFile(indexFilename, '');

  // ## Download entire feed
  let pageNum = 0;
  let nextUrl = rpdeEndpoint;
  let backoffTimeInSeconds = RETRY_BACKOFF_MIN;
  while (true) {
    /** @type {string} */
    let nextNextUrl;
    try {
      nextNextUrl = await downloadPage({ apiKey, outputFilePath, indexFilePrefix, indexFilename, datestamp }, pageNum, nextUrl);
      if (nextNextUrl === nextUrl) {
        // We have reached the end of the feed
        break;
      }
      nextUrl = nextNextUrl;
      pageNum += 1;
      backoffTimeInSeconds = RETRY_BACKOFF_MIN;
      await wait(requestDelaySeconds * 1000);
    } catch (error) {
      if (error instanceof Http404Error) {
        console.error(`ERROR: URL ("${error.url}") not found. Please check the value for <rpde-endpoint>.`)
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
      backoffTimeInSeconds = backoffTimeInSeconds * RETRY_BACKOFF_EXPONENTIATION_RATE;
    }
  }
})();
