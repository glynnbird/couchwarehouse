const path = require('path')
const ChangesReader = require('changesreader')
const schema = require('./lib/schema.js')
const ProgressBar = require('progress')
const debug = require('debug')('couchwarehouse')
const util = require('./lib/util.js')
const axios = require('axios').default
let cr
let sqldb

// load the database driver code depending on databaseType
const loadDatabaseDriver = (opts) => {
  switch (opts.databaseType) {
    case 'postgresql':
      sqldb = require('./lib/postgresql.js')
      break
    case 'mysql':
      sqldb = require('./lib/mysql.js')
      break
    case 'elasticsearch':
      sqldb = require('./lib/elasticsearch.js')
      break
    case 'sqlite':
    default:
      sqldb = require('./lib/sqlite.js')
  }
}

// extract the sequence number from a token e.g 47-1abc2 --> 47
const extractSequenceNumber = (seq) => {
  return parseInt(seq.replace(/-.*$/, ''))
}

// apply user-supplied JavaScript transform and look out for
// new schemas in the incoming documents
const transformAndDiscoverSchema = (b, opts, theSchema) => {
  // array of SQL statements
  let createSQL = []

  // for each document in the batch
  for (const i in b) {
    // the document we're working with
    let doc = b[i].doc

    // apply transform function
    if (typeof opts.transform === 'function') {
      doc = opts.transform.apply(null, [doc])
    }
    b[i].doc = doc

    // calculate its document type
    const docType = doc && opts.split ? doc[opts.split] : '_default'

    // if not a design doc and not a document type we've seen before
    if (doc && !doc._id.match(/^_design/) && !theSchema[docType]) {
      // clone the doc
      doc = JSON.parse(JSON.stringify(doc))

      // discover the schema
      debug('Calculating the schema for ' + docType)
      const s = schema.discover(doc)
      theSchema[docType] = s
      debug('schema', JSON.stringify(s))

      // create the database
      debug('Calculating Create SQL for ' + docType)
      createSQL = createSQL.concat(sqldb.generateCreateTableSQL(opts, docType, opts.database, s, opts.reset))
    }
  }

  return createSQL
}

// download a whole changes feed in one long HTTP request
const spoolChanges = async (opts, theSchema, maxChange) => {
  let lastSeq = opts.since
  let bar

  // progress bar
  if (opts.verbose) {
    bar = new ProgressBar('downloading ' + opts.database + ' [:bar] :percent :etas', { total: maxChange, width: 30 })
  }

  // return a Promise
  return new Promise((resolve, reject) => {
    // start spooling changes
    const changesReader = new ChangesReader(opts.database, opts.url)
    let func
    const params = { since: opts.since, includeDocs: true }

    // if we're in slow mode we use changesReader.get to iteratively
    // poll the changes feed in batches
    if (opts.slow) {
      func = changesReader.get
      params.wait = true
    } else {
      // in fast mode we can spool the changes feed in one long poll
      // knowing that our database can keep up
      func = changesReader.spool
    }
    func.apply(changesReader, [params]).on('batch', async (b, done) => {
      if (b.length > 0) {
        // transform and get any new schema SQL statements
        const createSQL = transformAndDiscoverSchema(b, opts, theSchema)

        // perform database operation
        await sqldb.insertBulk(opts, createSQL, opts.database, theSchema, b)

        // update the progress bar
        if (opts.verbose) {
          bar.tick(b.length)
        }

        // write checkpoint
        await sqldb.writeCheckpoint(opts.database, lastSeq)

        // call the done callback if provided
        if (typeof done === 'function') {
          done()
        }
      }
    }).on('end', async (s) => {
      lastSeq = s
      // complete the progress bar
      if (opts.verbose) {
        bar.tick(bar.total - bar.curr)
      }

      // write checkpoint
      await sqldb.writeCheckpoint(opts.database, lastSeq)

      // pass back the last known sequence token
      resolve(lastSeq)
    }).on('error', reject)
  })
}

// monitor new changes using multiple "longpoll" HTTP requests
const monitorChanges = async function (opts, theSchema, lastSeq) {
  // return a Promise
  return new Promise((resolve, reject) => {
    // start monitoring the changes fees
    cr = new ChangesReader(opts.database, opts.url)
    cr.start({ since: lastSeq, includeDocs: true }).on('batch', async (b, done) => {
      if (opts.verbose) {
        process.stdout.write('.')
      }

      // transform and discover schema of incoming documents
      const createSQL = transformAndDiscoverSchema(b, opts, theSchema)

      // perform database operation
      await sqldb.insertBulk(opts, createSQL, opts.database, theSchema, b)

      // write a checkpoint
      const latestSeq = b[b.length - 1].seq
      await sqldb.writeCheckpoint(opts.database, latestSeq)

      // call the done callback if provided
      if (typeof done === 'function') {
        done()
      }
    }).on('error', reject)
    resolve()
  })
}

// tell the ChangesReader to stop
const stop = () => {
  if (cr) {
    cr.stop()
    cr = null
  }
}

// start spooling and monitoring the changes feed
const start = async (opts) => {
  // override defaults
  const theSchema = {}
  const defaults = {
    url: 'http://localhost:5984',
    since: '0',
    verbose: false,
    reset: false,
    transform: null,
    split: null,
    slow: false,
    databaseType: 'sqlite'
  }
  opts = Object.assign(defaults, opts)

  // if transform is present
  if (opts.transform) {
    opts.transform = require(path.resolve(process.cwd(), opts.transform))
  }

  // get latest revision token of the target database, to
  // give us something to aim for
  debug('Getting last change from CouchDB')
  const req = {
    baseURL: opts.url,
    url: opts.database + '/_changes',
    params: {
      since: 'now',
      limit: 1
    }
  }
  const response = await axios(req)
  const info = response.data
  const maxChange = extractSequenceNumber(info.last_seq)

  // initialse database
  if (opts.databaseType !== 'sqlite') {
    opts.slow = true
  }
  debug('Initalise database')
  loadDatabaseDriver(opts)
  await sqldb.initialise(opts.reset)

  // seeing where we got to last time
  if (!opts.reset) {
    const lastTime = await sqldb.getCheckpoint(opts.database)
    opts.since = lastTime || '0'
  }

  // spool changes
  debug('Spooling changes')
  if (opts.verbose) {
    opts.usableDbName = util.calculateUsableDbName(opts, opts.database, null)
    sqldb.message(opts)
  }
  const lastSeq = await spoolChanges(opts, theSchema, maxChange)

  // monitor changes
  monitorChanges(opts, theSchema, lastSeq)
}

module.exports = {
  start,
  stop
}
