// Borrowed from: https://github.com/fanlia/ndjson-csv-xlsx/blob/main/index.js

import ndjson from 'ndjson'
import csv from 'fast-csv'
import xlsxparse from 'xlsx-parse-stream'
import XLSXWriteStream from '@atomictech/xlsx-write-stream'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import Stringer from 'stream-json/Stringer.js'
import Disassembler from 'stream-json/Disassembler.js'
import chain from 'stream-chain'

const XLSXStreamer = XLSXWriteStream.default

export default {
  ndjson: {
    parse: (...args) => ndjson.parse(...args),
    stringify: (...args) => ndjson.stringify(...args)
  },
  csv: {
    parse: (...args) => csv.parse(...args),
    stringify: (...args) => csv.format(...args)
  },
  xlsx: {
    parse: (...args) => xlsxparse(...args),
    stringify: (...args) => new XLSXStreamer(...args)
  },
  json: {
    parse: (...args) => chain([
      StreamArray.withParser(...args),
      data => data.value
    ]),
    stringify: (options, ...args) => chain([
      new Disassembler(),
      new Stringer({ ...options, makeArray: true })
    ])
  }
}
