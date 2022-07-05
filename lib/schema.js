const flatten = require('./flatten.js').flatten

// "discover" the schema of an array of documents
const discover = (doc) => {
  // flatten the doc
  doc = flatten(doc)

  // replace values with data types
  for (const i in doc) {
    doc[i] = typeof doc[i]
  }

  return doc
}

module.exports = {
  discover
}
