async function factory (pkgName) {
  const me = this

  class DoboExtra extends this.app.pluginClass.base {
    static alias = 'dbx'
    static dependencies = ['dobo', 'bajo-extra']

    constructor () {
      super(pkgName, me.app)
      this.config = {
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
        },
        archive: {
          tasks: [],
          checkInterval: false,
          runEarly: true
        }
      }
    }

    init = async () => {
      const { buildCollections } = this.app.bajo
      const types = ['datetime', 'date', 'timestamp']

      async function handler ({ item }) {
        const { join } = this.app.bajo
        const { getSchema } = this.app.dobo
        const { has } = this.app.lib._
        for (const f of ['source', 'destination']) {
          if (!has(item, f)) throw this.error('taskMustHaveModel%s', f)
          const key = `${f}Field`
          item[key] = item[key] ?? 'createdAt'
          const schema = getSchema(item[f])
          const prop = schema.properties.find(p => p.name === item[key])
          if (!prop) throw this.error('unknownField%s%s', item[key], item[f])
          if (!types.includes(prop.type)) throw this.error('isNotSupported%s%s%s%s', item[key], item[f], prop.type, join(types))
        }
        if (item.source === item.destination) throw this.error('sourceDestMustBeDifferent')
        item.maxAge = item.maxAge ?? 1 // in days, less then 1 is ignored
      }

      this.archivers = await buildCollections({ ns: this.ns, handler, container: 'archive.tasks', dupChecks: ['source'], useDefaultName: false })
    }

    start = async () => {
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
  }

  return DoboExtra
}

export default factory
