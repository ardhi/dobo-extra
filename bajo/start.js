async function start () {
  if (this.config.archive.checkInterval === false || this.config.archive.checkInterval <= 0) {
    this.log.warn('autoArchiveDisabled')
    return
  }
  if (this.config.archive.runEarly) await this.archive()
  this.archiveIntv = setInterval(() => {
    this.archive().then().catch(err => {
      this.log.error('archiveError%s', err.message)
    })
  }, this.config.archive.checkInterval * 60 * 1000)
}

export default start
