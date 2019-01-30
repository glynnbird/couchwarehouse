const filter = (str) => {
  return str.replace(/-/g, '')
}

const calculateUsableDbName = (opts, dbName, docType) => {
  return opts.split ? filter(dbName) + '_' + filter(docType) : filter(dbName)
}

module.exports = {
  filter: filter,
  calculateUsableDbName: calculateUsableDbName
}
