import ndjson from 'ndjson'
import { Readable } from 'stream'
import fs from 'fs'

const ndjsonReadHandler = function (data, opts = {}) {
  return new Promise((resolve, reject) => {
    const reader = opts.readFromFile ? fs.createReadStream(data) : Readable.from(data)
    const results = []
    reader.pipe(ndjson.parse({ strict: opts.strict ?? true }))
      .on('data', data => {
        results.push(data)
      })
      .on('error', reject)
      .on('end', () => {
        resolve(results)
      })
  })
}

const ndjsonWriteHandler = function (data, opts = {}) {
  return new Promise((resolve, reject) => {
    const stream = ndjson.stringify()
    const results = []
    stream.on('data', line => {
      results.push(line)
    })
    stream.on('finish', () => {
      const items = results.join('\n')
      if (opts.writeToFile) {
        fs.writeFileSync(data, items, 'utf8')
        resolve()
      } else resolve(items)
    })
    stream.write(data)
    stream.end()
  })
}

export default [
  { ext: '.ndjson', readHandler: ndjsonReadHandler, writeHandler: ndjsonWriteHandler },
  { ext: '.jsonl', readHandler: ndjsonReadHandler, writeHandler: ndjsonWriteHandler }
]
