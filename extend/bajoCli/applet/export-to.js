import _path from 'path'

const batch = 100

function makeProgress (spin) {
  const { secToHms } = this.app.lib.aneka
  return async function ({ batchNo, data, batchStart, batchEnd } = {}) {
    if (data.length === 0) return
    spin.setText('batch%d%s', batchNo, secToHms(batchEnd.toTime() - batchStart.toTime(), true))
  }
}

async function exportTo (...args) {
  const { importPkg } = this.app.bajo
  const { dayjs } = this.app.lib
  const { isEmpty, map } = this.app.lib._
  const { getInfo, start } = this.app.dobo

  const [input, select] = await importPkg('bajoCli:@inquirer/input',
    'bajoCli:@inquirer/select')
  const schemas = map(this.app.dobo.schemas, 'name')
  if (isEmpty(schemas)) return this.print.fatal('notFound%s', this.t('field.schema'))
  let [model, dest, query] = args
  if (isEmpty(model)) {
    model = await select({
      message: this.t('chooseModel'),
      choices: map(schemas, s => ({ value: s }))
    })
  }
  if (isEmpty(dest)) {
    dest = await input({
      message: this.t('enterDestFile'),
      default: `${model}-${dayjs().format('YYYYMMDD')}.ndjson`,
      validate: (item) => !isEmpty(item)
    })
  }
  if (isEmpty(query)) {
    query = await input({
      message: this.t('enterQueryIfAny')
    })
  }
  const spin = this.print.spinner().start('exporting')
  const progressFn = makeProgress.call(this, spin)
  const { connection } = getInfo(model)
  await start(connection.name)
  try {
    const filter = { query }
    const result = await this.exportTo(model, dest, { filter, batch, progressFn })
    spin.succeed('exported%d%s', result.count, _path.resolve(result.file))
  } catch (err) {
    console.log(err)
    spin.fatal('error%s', err.message)
  }
}

export default exportTo
