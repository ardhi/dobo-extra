import path from 'path'
import { json, ndjson, csv, xlsx } from './lib/helper.js'
import { createGunzip, createGzip } from 'zlib'
import scramjet from 'scramjet'
import config from './lib/config.js'

const { DataStream } = scramjet
const exts = ['.json', '.jsonl', '.ndjson', '.csv', '.xlsx', '.tsv']

/**
 * Plugin factory.
 *
 * **Never** call this function directly!!! It's only-meant to be called by the {@link https://ardhi.github.io/bajo|Bajo framework} during plugin initialization.
 *
 * @param {string} pkgName - NPM package name
 * @returns {DoboExtra}
 */
async function factory (pkgName) {
  const me = this

  /**
   * DoboExtra class definition
   *
   * This class provides more functionality to the Dobo plugin including:
   * - new additional `ndjson` format for Bajo's configHandlers
   * - data import/export from/to file system in various formats (JSON, NDJSON, CSV, TSV, XLSX)
   *
   *  @class
   */
  class DoboExtra extends this.app.baseClass.Base {
    constructor () {
      /**
       * Constructor
       */
      super(pkgName, me.app)

      /**
       * Configuration object
       * @type {TConfig}
       */
      this.config = config
    }

    /**
     * Import data from a file into a Dobo model
     *
     * @async
     * @method
     * @param {string} source - Source file path (absolute or relative to plugin data dir)
     * @param {string|boolean} dest - Destination model name or `false`. If `false`, the data will be returned instead of being imported into a model.
     * @param {object} options - Import options
     * @param {boolean} [options.trashOld=true] - Whether to clear the destination model before importing
     * @param {number} [options.batch=100] - Number of records to import in a single batch.
     * @param {function} [options.progressFn] - Callback function to report progress
     * @param {function} [options.converterFn] - Callback function to convert each record before importing
     * @param {boolean} [options.useHeader=true] - Whether to use the first row as header (for CSV/TSV/XLSX)
     * @param {string} [options.fileType] - File type (json, ndjson, csv, tsv, xlsx)
     * @returns {Promise<object|array>} - Imported data or summary report including file path and record count affected
     */
    importFrom = async (source, dest, options = {}) => {
      let {
        trashOld = true, batch = 100, progressFn, converterFn, useHeader = true,
        fileType, createOpts = {}, parserOpts = {}
      } = options
      const { merge } = this.app.lib._
      const { fs } = this.app.lib
      const { getModel } = this.app.dobo

      let dmodel
      if (dest !== false) dmodel = getModel(dest) // make sure dest model is valid
      let file
      if (path.isAbsolute(source)) file = source
      else {
        file = `${this.app.getPluginDataDir(this.ns)}/import/${source}`
        fs.ensureDirSync(path.dirname(file))
      }
      if (!fs.existsSync(file)) throw this.error('sourceFileNotExists%s', file)
      let ext = fileType ? `.${fileType}` : path.extname(file)
      let decompress = false
      if (ext === '.gz') {
        ext = path.extname(path.basename(file, '.gz'))
        decompress = true
      }
      if (!exts.includes(ext)) throw this.error('unsupportedFormat%s', ext.slice(1))
      if (trashOld && dest !== false) await dmodel.clearRecord()
      const reader = fs.createReadStream(file)
      batch = parseInt(batch) || 100
      if (batch > this.config.import.maxBatch) batch = this.config.import.maxBatch
      if (batch < 0) batch = 1
      let count = 0
      const pipes = [reader]
      if (decompress) pipes.push(createGunzip())
      if (ext === '.json') pipes.push(json.parse(parserOpts))
      else if (['.ndjson', '.jsonl'].includes(ext)) pipes.push(ndjson.parse(parserOpts))
      else if (ext === '.csv') pipes.push(csv.parse(merge({}, { headers: useHeader }, parserOpts)))
      else if (ext === '.tsv') pipes.push(csv.parse(merge({}, { headers: useHeader }, merge({}, parserOpts, { delimiter: '\t' }))))
      else if (ext === '.xlsx') pipes.push(xlsx.parse(merge({}, { header: useHeader }, parserOpts)))

      const stream = DataStream.pipeline(...pipes)
      let batchNo = 1
      const data = []
      await stream
        .batch(batch)
        .map(async items => {
          if (items.length === 0) return null
          const batchStart = new Date()
          for (let item of items) {
            count++
            item = converterFn ? await converterFn.call(this, item) : item
            if (dest !== false) await dmodel.createRecord(item, createOpts)
            else data.push(item)
          }
          if (progressFn) await progressFn.call(this, { batchNo, data: items, batchStart, batchEnd: new Date() })
          batchNo++
        })
        .run()

      return dest === false ? data : { file, count }
    }

    /**
     * Export data from a Dobo model into a file
     * @async
     * @method
     * @param {string} source - Source model name
     * @param {string} dest - Destination file path (absolute or relative to plugin data dir)
     * @param {object} options - Export options
     * @param {object} [options.filter={}] - Filter object to select records to export
     * @param {boolean} [options.useHeader=true] - Whether to include the header row (for CSV/TSV/XLSX)
     * @param {number} [options.batch=500] - Number of records to export in a single batch
     * @param {function} [options.progressFn] - Callback function to report progress
     * @param {Array<string>} [options.fields] - List of fields to include in the export
     * @param {object} [options.parserOpts={}] - Options for the parser (e.g., CSV delimiter)
     * @returns {Promise<object>} - Export summary including file path and record count affected
     */
    exportTo = (source, dest, options = {}) => {
      let {
        filter = {}, useHeader = true, batch = 500, opts = {},
        progressFn, fields, parserOpts = {}, exportOpts = {}
      } = options
      const { getDownloadDir } = this.app.bajo
      const { fs } = this.app.lib
      const { merge, omit, pick } = this.app.lib._
      const { getModel } = this.app.dobo
      const { generateId } = this.app.lib.aneka

      const i18n = pick(opts, ['lang', 'dateStyle', 'timeStyle', 'timeZone'])
      const params = { dataOnly: false, fields, fmt: true, refs: '*', noCache: true, i18n }

      const getFile = async () => {
        let ext = path.extname(dest)
        const file = `${getDownloadDir()}/${generateId()}${ext}`
        let compress = false
        if (ext === '.gz') {
          compress = true
          ext = path.extname(dest.slice(0, -3))
          // file = file.slice(0, file.length - 3)
        }
        if (!exts.includes(ext)) throw this.error('unsupportedFormat%s', ext.slice(1))
        return { file, ext, compress }
      }

      const getData = async (options = {}) => {
        const { source, filter, count, stream, progressFn } = options
        let cnt = count ?? 0
        const { find } = this.app.lib._
        const { getModel } = this.app.dobo
        const { maxLimit, hardCap } = this.app.dobo.config.default.filter
        filter.limit = maxLimit
        let sort
        const model = getModel(source)
        const idField = find(model.properties, { name: 'id' }).name
        for (const name of ['createdAt', 'updatedAt', 'ts', 'dt']) {
          const field = find(model.properties, { name })
          if (field) {
            sort = field.name
            break
          }
        }
        filter.sort = `${sort ?? idField}:1`
        for (;;) {
          const batchStart = new Date()
          const { data: rows, page } = await model.findRecord(filter, params)
          const data = rows.map(item => {
            let _item = exportOpts.includes('fvalue') ? item._fmt : omit(item, ['_immutable', '_fmt', '_ref'])
            if (exportOpts.includes('fkey')) {
              const newItem = {}
              for (const key in _item) {
                newItem[i18n.lang ? this.t(`field.${key}`, { lang: i18n.lang }) : key] = _item[key]
              }
              _item = newItem
            }
            return _item
          })
          if (data.length === 0) break
          if (cnt + data.length > hardCap) {
            const sliced = data.slice(0, hardCap - cnt)
            await stream.pull(sliced)
            cnt += sliced.length
            if (progressFn) await progressFn.call(this, { batchNo: page, data: sliced, batchStart, batchEnd: new Date() })
            break
          }
          cnt += data.length
          await stream.pull(data)
          if (progressFn) await progressFn.call(this, { batchNo: page, data, batchStart, batchEnd: new Date() })
          filter.page++
        }
        await stream.end()
        return cnt
      }

      filter.page = 1
      batch = parseInt(batch) ?? 500
      if (batch > this.config.export.maxBatch) batch = this.config.export.maxBatch
      if (batch < 0) batch = 1
      filter.limit = batch

      return new Promise((resolve, reject) => {
        let count = 0
        let file
        let ext
        let stream
        let compress
        let writer
        getModel(source)
        getFile()
          .then(res => {
            file = res.file
            ext = res.ext
            compress = res.compress
            writer = fs.createWriteStream(file)
            writer.on('error', err => {
              reject(err)
            })
            writer.on('finish', () => {
              resolve({ file, count })
            })
            stream = new DataStream()
            stream = stream.flatMap(items => (items))
            const pipes = []
            if (ext === '.json') pipes.push(json.stringify(parserOpts))
            else if (['.ndjson', '.jsonl'].includes(ext)) pipes.push(ndjson.stringify(parserOpts))
            else if (ext === '.csv') pipes.push(csv.stringify(merge({}, { headers: useHeader }, parserOpts)))
            else if (ext === '.tsv') pipes.push(csv.stringify(merge({}, { headers: useHeader }, merge({}, parserOpts, { delimiter: '\t' }))))
            else if (ext === '.xlsx') pipes.push(xlsx.stringify(merge({}, { header: useHeader }, parserOpts)))
            if (compress) pipes.push(createGzip())
            DataStream.pipeline(stream, ...pipes).pipe(writer)
            return getData({ source, filter, count, stream, progressFn })
          })
          .then(cnt => {
            count = cnt
          })
          .catch(reject)
      })
    }
  }

  return DoboExtra
}

export default factory
