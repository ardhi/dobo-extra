const types = ['datetime', 'date', 'timestamp']

async function handler ({ item }) {
  const { join } = this.app.bajo
  const { getSchema } = this.app.dobo
  const { has } = this.app.bajo.lib._
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

async function init () {
  const { buildCollections } = this.app.bajo
  this.archivers = await buildCollections({ ns: this.name, handler, container: 'archive.tasks', dupChecks: ['source'], useDefaultName: false })
}

export default init
