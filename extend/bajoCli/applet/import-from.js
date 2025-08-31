import _path from 'path'

const batch = 100

function makeProgress (spin) {
  const { secToHms } = this.app.lib.aneka
  return async function ({ batchNo, data, batchStart, batchEnd } = {}) {
    spin.setText('batch%d%s', batchNo, secToHms(batchEnd.toTime() - batchStart.toTime(), true))
  }
}

async function importFrom (...args) {
  const { importPkg } = this.app.bajo
  const { isEmpty, map } = this.app.lib._
  const { getInfo, start } = this.app.dobo

  const [input, select, confirm] = await importPkg('bajoCli:@inquirer/input',
    'bajoCli:@inquirer/select', 'bajoCli:@inquirer/confirm')
  const schemas = map(this.app.dobo.schemas, 'name')
  if (isEmpty(schemas)) return this.print.fatal('notFound%s', this.t('field.schema'))
  let [dest, model] = args
  if (isEmpty(dest)) {
    dest = await input({
      message: this.t('enterSourceFile'),
      validate: (item) => !isEmpty(item)
    })
  }
  if (isEmpty(model)) {
    model = await select({
      message: this.t('chooseModel'),
      choices: map(schemas, s => ({ value: s }))
    })
  }
  const answer = await confirm({
    message: this.t('aboutToReplaceAllRecords'),
    default: false
  })
  if (!answer) return this.print.fatal('aborted')
  const spin = this.print.spinner({ showCounter: true }).start('importing')
  const progressFn = makeProgress.call(this, spin)
  const { connection } = getInfo(model)
  await start(connection.name)
  try {
    const result = await importFrom(dest, model, { batch, progressFn })
    spin.succeed('recordsImported%d%s', result.count, _path.resolve(result.file))
  } catch (err) {
    spin.fatal('Error: %s', err.message)
  }
}

export default importFrom
