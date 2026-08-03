import path from 'path'

const batch = 100

function makeProgress (spin) {
  const { secToHms } = this.app.lib.aneka
  return async function ({ batchNo, data, batchStart, batchEnd } = {}) {
    if (data.length === 0) return
    spin.setText('batch%d%s', batchNo, secToHms(batchEnd - batchStart, true))
  }
}

async function exportTo (appletPath, ...args) {
  const { importPkg } = this.app.bajo
  const { dayjs, fs } = this.app.lib
  const { isEmpty, map } = this.app.lib._

  const [input, select] = await importPkg('bajoCli:@inquirer/input',
    'bajoCli:@inquirer/select')
  const models = map(this.app.dobo.models, 'name')
  if (isEmpty(models)) return this.print.fatal('notFound%s', this.t('field.model'))
  let [dest, model, query] = args
  if (isEmpty(model)) {
    model = await select({
      message: this.t('chooseModel'),
      choices: map(models, s => ({ value: s }))
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
  await this.app.dobo.start()
  try {
    const filter = { query }
    const dir = this.app.getPluginDataDir(this.ns) + '/export'
    fs.ensureDirSync(dir)
    const result = await this.exportTo(model, `${dir}/${dest}`, { noBaseName: false, filter, batch, progressFn })
    spin.succeed('exported%d%s', result.count, path.resolve(result.file))
  } catch (err) {
    spin.fatal(err)
  }
}

export default exportTo
