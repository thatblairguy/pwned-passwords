
const fs                  = require('fs');
const fsp                 = require('fs/promises');
const readline            = require('readline');
const { writer } = require('repl');

const asyncPool           = require('tiny-async-pool');

const PREFIX_SIZE     = 5;
const MAX_WRITEBUFFER = 5;

async function processLineByLine() {

  const fileStream = fs.createReadStream('pwned-passwords-sha1-ordered-by-hash-v8.txt');

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  // Note: we use the crlfDelay option to recognize all instances of CR LF
  // ('\r\n') in input.txt as a single line break.

  let recordNumber = 0;
  let bulk = [];

  let lastPrefix = null;

  let writeBuffer = [];

  for await (let line of rl) {
    recordNumber++

    line = line.trim();
    let prefix = line.substring(0, PREFIX_SIZE);

    if(lastPrefix != prefix) {
      if(lastPrefix != null && bulk.length > 0){
        const write = WriteRecords(lastPrefix, bulk);
        writeBuffer.push(write);
      }
      bulk = [];
      lastPrefix = prefix
    }

    if(writeBuffer.length >= MAX_WRITEBUFFER) {
      await Promise.all(writeBuffer);
      writeBuffer = [];
    }

    let record = line.substring(PREFIX_SIZE);
    bulk.push(record);
  }

  if(bulk.length > 0) {
    writeBuffer.push(WriteRecords(lastPrefix, bulk));
  }

  if(writer.length > 0)
    await Promise.all(writeBuffer);

}

async function WriteRecords(prefix, records) {
  return fsp.writeFile(`data/${prefix}.txt`, records.join("\n"));
}

processLineByLine();