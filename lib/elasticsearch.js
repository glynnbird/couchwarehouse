const elasticsearch = require('elasticsearch')
let db

const initialise = async (reset) => {
  const config = process.env.ESCONFIG || 'http://localhost:9200'
  db = new elasticsearch.Client({
    hosts: [config]
  })
}

const generateCreateTableSQL = (opts, docType, dbName, schema, reset) => {
  return []
}

// insert an array of changes into the database
const insertBulk = async (opts, createSQL, dbName, theSchema, batch) => {
  const actions = []

  batch.forEach(async b => {
    // ignore design docs
    if (!b.id.match(/^_design/) && b.doc) {
      // get the schema we're working with
      const docType = opts.split ? b.doc[opts.split] : 'default'

      // if this is a deletion
      if (b.deleted) {
        // delete from Elasticsearch
        actions.push({ delete: { _index: 'couchwarehouse', _type: docType, _id: b.doc._id } })
      } else {
        actions.push({ index: { _index: 'couchwarehouse', _type: docType, _id: b.doc._id } })
        delete b.doc._id
        delete b.doc._rev
        delete b.doc._attachments
        actions.push(b.doc)
      }
    }
  })

  if (actions.length > 0) {
    return db.bulk({ body: actions })
  } else {
    return null
  }
}

const query = async (sql, substitutions) => {
  return null
}

// write a checkpoint to keep track of where we got to
// with each table
const writeCheckpoint = async (tablename, seq) => {
  const actions = []
  actions.push({
    index: {
      _index: 'couchwarehousemeta',
      _type: tablename,
      _id: 'checkpoint'
    }
  })
  actions.push({ seq: seq })
  return db.bulk({ body: actions })
}

const getCheckpoint = async (tablename) => {
  try {
    const doc = await db.get({
      index: 'couchwarehousemeta',
      type: tablename,
      id: 'checkpoint'
    })
    return (doc && doc._source && doc._source.seq) ? doc._source.seq : null
  } catch (e) {
    return null
  }
}

const message = (opts) => {
  console.log('Spooling data into Elasticsearch')
  console.log('Use the API to access the data: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/_the_search_api.html')
  console.log('p.s Press ctrl-C to stop monitoring for further changes')
}

module.exports = {
  initialise,
  generateCreateTableSQL,
  insertBulk,
  query: query,
  writeCheckpoint: writeCheckpoint,
  getCheckpoint: getCheckpoint,
  message: message
}
