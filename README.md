# open-objects-script

Script that Open Objects uses to download imin's Firehose feeds into files.

## How to use

N.B. the output-file-path directory will be removed when this script is run,
so take care not to set it to e.g. `/`. The directory should only hold files
outputted by this script and nothing else.

1. `npm install`
2. Run `node walkRpde.js` with command line args set e.g.

  ```sh
  node walkRpde.js 'https://firehose.imin.co/firehose/session-series' '<FIREHOSE API KEY>' session-series/ prefix 0.1
  ```

## Files

- `reference/original-walk-rpde`
  The RPDE script that imin gave to OpenObjects originally.
- `reference/oo-walk-rpde`
  OpenObjects made a couple modifications from imin's original script. This is what they used for a time.

  There's also been some performance measurements added to this script. These last two scripts were very slow.
- `walkRpde.js`
  A script that copies the functionality of `reference/oo-walk-rpde` but in node.js. Because JSON is only parsed once and large amounts of memory are not constantly copied, this is 100s of times faster than the bash scripts.
