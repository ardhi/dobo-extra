import path from 'path'

const batch = 100

function makeProgress (spin) {
  const { secToHms } = this.app.lib.aneka
  return async function ({ batchNo, data, batchStart, batchEnd } = {}) {
    spin.setText('batch%d%s', batchNo, secToHms(batchEnd - batchStart, true))
  }
}

async function importFrom (appletPath, ...args) {
  const { importPkg } = this.app.bajo
  const { isEmpty, map } = this.app.lib._

  const [input, select, confirm] = await importPkg('bajoCli:@inquirer/input',
    'bajoCli:@inquirer/select', 'bajoCli:@inquirer/confirm')
  const models = map(this.app.dobo.models, 'name')
  if (isEmpty(models)) return this.print.fatal('notFound%s', this.t('field.model'))
  let [source, model] = args
  if (isEmpty(source)) {
    source = await input({
      message: this.t('enterSourceFile'),
      validate: (item) => !isEmpty(item)
    })
  }
  if (isEmpty(model)) {
    const choices = [{ value: false, name: this.t('_showOnScreen_') }].concat(map(models, s => ({ value: s })))
    model = await select({
      message: this.t('chooseModel'),
      choices
    })
  }
  model = model ?? false
  const append = this.app.argv._.append ?? false
  const answer = await confirm({
    message: this.t(append ? 'aboutToAppendAllRecords' : 'aboutToReplaceAllRecords'),
    default: false
  })
  if (!answer) return this.print.fatal('aborted')
  const spin = this.print.spinner({ showCounter: true }).start('importing')
  const progressFn = makeProgress.call(this, spin)
  await this.app.dobo.start()
  try {
    const result = await this.importFrom(source, model, { batch, progressFn, clear: !append })
    if (model) spin.succeed('recordsImported%d%s', result.count, path.resolve(result.file))
    else console.log(result)
  } catch (err) {
    spin.fatal(err)
  }
}

export default importFrom
