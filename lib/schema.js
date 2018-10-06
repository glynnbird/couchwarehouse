const flatten = require('./flatten.js').flatten

const discover = (docs) => {
  let doc = null
  for (let i in docs) {
    doc = docs[i]
    if (!docs[i].doc._id.match(/^_design/)) {
      doc = docs[i].doc
      break
    }
  }

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
