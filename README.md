# open-objects-script

Script that Open Objects uses to download imin's Firehose feeds into files.

Note that this script is provided "as-is", and imin do not provide support for this script or maintenance for it ongoing. The script is designed to be compatible with [Realtime Paged Data Exchange 1.0](https://www.w3.org/2017/08/realtime-paged-data-exchange/).

# License

Copyright 2019 IMIN LTD

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Files

- `reference/original-walk-rpde`
  The RPDE script that imin gave to OpenObjects originally.
- `reference/oo-walk-rpde`
  OpenObjects made a couple modifications from imin's original script. This is what they used for a time.

  There's also been some performance measurements added to this script. These last two scripts were very slow.
- `walkRpde.js`
  A script that copies the functionality of `reference/oo-walk-rpde` but in node.js. Because JSON is only parsed once and large amounts of memory are not constantly copied, this is 100s of times faster than the bash scripts.
- `geoSegment.js`


## walkRpde.js
### How to use

**N.B.** the `output-file-path` directory **will be removed** when this script is run,
so take care not to set it to e.g. `/`. The directory should only hold files
outputted by this script and nothing else.

You will need Node.js ^12.18.2 (LTS as of July 2020)

1. `npm install`
2. Run script

  ```sh
  node walkRpde.js <rpde-endpoint> <api-key> <output-file-path> <index-file-prefix> <request-delay-seconds>
  ```

  e.g.

  ```sh
  node walkRpde.js 'https://firehose.imin.co/firehose/standard/session-series' <api-key> session-series/ rpde 0.1
  ```

## geoSegment.js
### How to run

You will need Node.js ^12.18.2 (LTS as of July 2020)

1. Edit the `config.js` file to your desired segments
2. `npm install`
3. Run script

  ```sh
  node geoSegment.js
  ```
The outputs will be in the `output` folder, with a folder of ScheduledSessions for each segment. Also in each segment folder will be a file called `index.txt` 
that contains the filename of each ScheduledSession in that folder.

### Known Limitations

- SHA-1 hashes of IDs are used as filenames for both SessionSeries and ScheduledSessions. It is very unlikely that these hashes will clash, but if they do
the newer file will be dropped.
- ScheduledSession's location is not used for geo-segmenting - only the SessionSeries's