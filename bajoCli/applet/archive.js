async function move (task) {
  const { importPkg } = this.app.bajo
  const { dayjs } = this.app.bajo.lib
  const { set } = this.app.bajo.lib._
  const { formatInteger } = this.app.bajoExtra
  const { recordFind, recordCreate, recordRemove, statAggregate } = this.app.dobo
  const prompts = await importPkg('bajoCli:@inquirer/prompts')
  const { confirm } = prompts
  // get relevant record
  this.print.info('copying%s%s', task.source, task.destination)
  const mark = dayjs().subtract(task.maxAge, 'day').toDate()
  const query = set({}, task.sourceField, { $lt: mark })
  let count = await statAggregate(task.source, { query }, { aggregate: 'count' })
  count = count[0].count
  if (count === 0) {
    this.print.warn('notFoundSkipped')
    return
  }
  if (this.config.prompt !== false) {
    const answer = await confirm({
      message: this.print.write('recordsArchivedContinue%d', count),
      default: true
    })
    if (!answer) {
      this.print.warn('taskCancelled%s%s', task.source, task.destination)
      return
    }
  } else this.print.info('recordsArchived%d', count)
  let page = 1
  let total = 0
  let affected = 0
  let error = 0
  let isMax = false
  const ids = []
  const spin = this.print.spinner({ showCounter: true }).start()
  for (;;) {
    const results = await recordFind(task.source, { query, page, limit: 100 }, { noCache: true, noHook: true })
    if (results.length === 0) break
    if (this.bajo.config.tool && total % 1000 === 0 && total !== 0) this.print.succeed('milestoneCopied%s', formatInteger(total))
    for (const r of results) {
      if (task.maxRecords && task.maxRecords < total) {
        isMax = true
        break
      }
      total++
      try {
        await recordCreate(task.destination, r, { noSanitize: true, noHook: true, noResult: true, noCheckUnique: true })
        ids.push(r.id)
        spin.setText('id%s', r.id)
      } catch (err) {
        error++
      }
    }
    if (isMax) break
    affected = total - error
    page++
  }
  if (isMax) this.print.warn('maxReached%d', task.maxRecords)
  this.print.info('removing%s', task.source)
  for (const idx in ids) {
    const id = ids[idx]
    try {
      await recordRemove(task.source, id, { noHook: true, noResult: true })
      spin.setText('id%s', id)
      if (this.bajo.config.tool && idx % 1000 === 0 && idx !== '0') this.print.succeed('milestoneRemoved%s', formatInteger(idx))
    } catch (err) {}
  }
  spin.stop()
  this.print.info('archivingEnded%s%s%s%s', task.source, task.destination, formatInteger(affected), formatInteger(error))
}

async function archive (...args) {
  const { importPkg } = this.app.bajo
  const prompts = await importPkg('bajoCli:@inquirer/prompts')
  const { confirm } = prompts
  if (this.config.prompt !== false) {
    const answer = await confirm({
      message: this.print.write('manuallyRunTaskContinue'),
      default: false
    })

    if (!answer) this.print.fatal('aborted')
  }
  await this.app.dobo.start()
  if (this.archivers.length === 0) this.print.fatal('nothingToArchive')
  for (const t of this.archivers.tasks) {
    if (t.maxAge < 1) {
      this.print.warn('archiveDisabled%s%s', t.source, t.destination)
      continue
    }
    await move.call(this, t)
  }
}

export default archive
