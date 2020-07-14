/**
 * N.B. the output-file-path directory will be removed when this script is run,
 * so take care not to set it to `/`. The directory should only hold files
 * outputted by this script and nothing else.
 */
const { default: axios } = require('axios');
const fsPromises = require('fs').promises;
const { performance } = require('perf_hooks');

/**
 * Set this to true in order to log timings of tasks within the script
 */
const DO_DEBUG_PERFORMANCE = false;

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
  const rpdePage = await axios(pageUrl, { headers: { 'x-api-key': apiKey }});
  logPerformance(`[${performance.now()}] Downloaded`);
  // ## Split the page into files - one file for each item
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
      // ### Save file entry to index
      await fsPromises.appendFile(indexFilename, `<a href="/${filename}">file</a>`);
      logPerformance(`[${performance.now()}] Saved item ${i}`);
    }
    logPerformance(`[${performance.now()}] Processed item ${i}`);
  }
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
  const requestDelaySeconds = Number(requestDelaySecondsStr);

  // ## Wipe existing output directory
  await fsPromises.rmdir(outputFilePath, { recursive: true });
  await fsPromises.mkdir(outputFilePath);

  // ## Create empty index file
  await fsPromises.writeFile(indexFilename, '');

  // ## Download entire feed
  let pageNum = 0;
  let nextUrl = rpdeEndpoint;
  while (true) {
    const nextNextUrl = await downloadPage({ apiKey, outputFilePath, indexFilePrefix, indexFilename, datestamp }, pageNum, nextUrl);
    if (nextNextUrl === nextUrl) {
      // We have reached the end of the feed
      break;
    }
    nextUrl = nextNextUrl;
    pageNum += 1;
    await wait(requestDelaySeconds * 1000);
  }
})();
