/**
 * @typedef TConfig
 * @memberof DoboExtra
 * @type {object}
 * @property {object} export - Export configuration
 * @property {number} export.maxBatch - Maximum number of records to export in a single batch
 * @property {object} export.stringify - Stringify options for exported data
 * @property {string} export.stringify.open - Opening string for exported data
 * @property {string} export.stringify.sep - Separator string for exported data
 * @property {string} export.stringify.close - Closing string for exported data
 * @property {object} import - Import configuration
 * @property {number} import.maxBatch - Maximum number of records to import in a single batch
 */
const config = {
  export: {
    maxBatch: 1000,
    stringify: {
      open: '[\n',
      sep: ',\n',
      close: '\n]\n'
    }
  },
  import: {
    maxBatch: 1000
  }
}

export default config
