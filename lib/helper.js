import ndj from 'ndjson'
import fastCsv from 'fast-csv'
import xlsxparse from 'xlsx-parse-stream'
import XLSXWriteStream from '@atomictech/xlsx-write-stream'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import Stringer from 'stream-json/Stringer.js'
import Disassembler from 'stream-json/Disassembler.js'
import chain from 'stream-chain'

/**
 * Helper functions for Dobo Extra
 * @module Helper
 */

// Borrow the idea from: https://github.com/fanlia/ndjson-csv-xlsx/blob/main/index.js

const XLSXStreamer = XLSXWriteStream.default

/**
 * NDJSON parser and stringifier.
 *
 * To be able to use this helper, do something like this:
 * ```js
 * const { importModule } = this.app.bajo
 * const { ndjson } = await importModule('dobo-extra:/lib/helper.js', { asDefaultImport: false })
 * console.log(ndjson.parse('{"a":1}\n{"b":2}')) // [{a:1},{b:2}]
 * console.log(ndjson.stringify([{a:1},{b:2}])) // '{"a":1}\n{"b":2}\n'
 * ```
 * @type {object}
 * @property {function} parse - Parse NDJSON data
 * @property {function} stringify - Stringify data to NDJSON format
 */
export const ndjson = {
  parse: (...args) => ndj.parse(...args),
  stringify: (...args) => ndj.stringify(...args)
}

/**
 * CSV parser and stringifier.
 *
 * To be able to use this helper, do something like this:
 * ```js
 * const { importModule } = this.app.bajo
 * const { csv } = await importModule('dobo-extra:/lib/helper.js', { asDefaultImport: false })
 * ...
 * ```
 * @type {object}
 * @property {function} parse - Parse CSV data
 * @property {function} stringify - Stringify data to CSV format
 */
export const csv = {
  parse: (...args) => fastCsv.parse(...args),
  stringify: (...args) => fastCsv.format(...args)
}

/**
 * Excel XLSX parser and stringifier.
 *
 * To be able to use this helper, do something like this:
 * ```js
 * const { importModule } = this.app.bajo
 * const { xlsx } = await importModule('dobo-extra:/lib/helper.js', { asDefaultImport: false })
 * ...
 * ```
 * @type {object}
 * @property {function} parse - Parse XLSX data
 * @property {function} stringify - Stringify data to XLSX format
 */
export const xlsx = {
  parse: (...args) => xlsxparse(...args),
  stringify: (...args) => new XLSXStreamer(...args)
}

/**
 * JSON parser and stringifier. Use stream to handle large JSON files.
 *
 * To be able to use this helper, do something like this:
 * ```js
 * const { importModule } = this.app.bajo
 * const { json } = await importModule('dobo-extra:/lib/helper.js', { asDefaultImport: false })
 * ...
 * ```
 * @type {object}
 * @property {function} parse - Parse JSON data
 * @property {function} stringify - Stringify data to JSON format
 */
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
