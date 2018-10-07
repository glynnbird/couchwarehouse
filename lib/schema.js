const flatten = require('./flatten.js').flatten

// "discover" the schema of an array of documents
const discover = (docs) => {
  let doc = null

  // loop through the documents
  for (let i in docs) {
    doc = docs[i]

    // if this isn't a design document
    if (!docs[i].doc._id.match(/^_design/)) {
      // this document will do
      doc = docs[i].doc
      break
    }
  }

  // return keys and values of data types
  if (doc) {
    // flatten the doc
    doc = flatten(doc)

    // replace values with data types
    for (let i in doc) {
      doc[i] = typeof doc[i]
    }
  }

  return doc
}

module.exports = {
  discover: discover
}
