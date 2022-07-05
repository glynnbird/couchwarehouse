const filter = (str) => {
  return str.replace(/-/g, '')
}

const calculateUsableDbName = (opts, dbName, docType) => {
  if (docType) {
    return opts.split ? filter(dbName) + '_' + filter(docType) : filter(dbName)
  } else {
    return opts.split ? filter(dbName) : filter(dbName)
  }
}

module.exports = {
  filter,
  calculateUsableDbName
}
