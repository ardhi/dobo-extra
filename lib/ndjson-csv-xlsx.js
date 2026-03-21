// Borrowed from: https://github.com/fanlia/ndjson-csv-xlsx/blob/main/index.js

import ndj from 'ndjson'
import fastCsv from 'fast-csv'
import xlsxparse from 'xlsx-parse-stream'
import XLSXWriteStream from '@atomictech/xlsx-write-stream'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import Stringer from 'stream-json/Stringer.js'
import Disassembler from 'stream-json/Disassembler.js'
import chain from 'stream-chain'

const XLSXStreamer = XLSXWriteStream.default

export const ndjson = {
  parse: (...args) => ndj.parse(...args),
  stringify: (...args) => ndj.stringify(...args)
}

export const csv = {
  parse: (...args) => fastCsv.parse(...args),
  stringify: (...args) => fastCsv.format(...args)
}

export const xlsx = {
  parse: (...args) => xlsxparse(...args),
  stringify: (...args) => new XLSXStreamer(...args)
}

export const json = {
  parse: (...args) => chain([
    StreamArray.withParser(...args),
    data => data.value
  ]),
  stringify: (options, ...args) => chain([
    new Disassembler(),
    new Stringer({ ...options, makeArray: true })
  ])
}
