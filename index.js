const ChangesReader = require('changesreader')
const schema = require('./lib/schema.js')
const sqldb = require('./lib/db.js')
const ProgressBar = require('progress')
const debug = require('debug')('couchwarehouse')
let nano
let cr

// extract the sequence number from a token e.g 47-1abc2 --> 47
const extractSequenceNumber = (seq) => {
  return parseInt(seq.replace(/-.*$/, ''))
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
    const changesReader = new ChangesReader(opts.database, nano.request)
    changesReader.spool({ since: opts.since, includeDocs: true }).on('batch', async (b) => {
      if (b.length > 0) {
        // get latest sequence token
        lastSeq = b[b.length - 1].seq

        // perform database operation
        await sqldb.insertBulk(opts.database, theSchema, b)

        // update the progress bar
        if (opts.verbose) {
          bar.tick(b.length)
        }
      }
    }).on('end', async () => {
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
    cr = new ChangesReader(opts.database, nano.request)
    cr.start({ since: lastSeq, includeDocs: true }).on('batch', async (b) => {
      if (opts.verbose) {
        process.stdout.write('.')
      }

      // insert the changes into the database
      await sqldb.insertBulk(opts.database, theSchema, b)

      // write a checkpoint
      const latestSeq = b[b.length - 1].seq
      await sqldb.writeCheckpoint(opts.database, latestSeq)
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
  let defaults = {
    url: 'http://localhost:5984',
    since: '0',
    verbose: false,
    reset: false
  }
  opts = Object.assign(defaults, opts)

  // setup nano
  nano = require('nano')(opts.url)
  let maxChange

  // get latest revision token of the target database, to
  // give us something to aim for
  debug('Getting last change from CouchDB')
  const req = { db: opts.database, path: '_changes', qs: { since: 'now', limit: 1 } }
  const info = await nano.request(req)
  maxChange = extractSequenceNumber(info.last_seq)

  // get 50 documents from the database for schema discovery
  debug('Getting docs for schema discovery')
  const db = nano.db.use(opts.database)
  const exampleDocs = await db.list({ limit: 50, include_docs: true })

  // calculate the schema from the example docs
  debug('Calculating the schema')
  const theSchema = schema.discover(exampleDocs.rows)
  if (!theSchema) {
    throw new Error('Unable to infer the schema on database ' + opts.database)
  }
  debug('schema', JSON.stringify(theSchema))

  // setup the local database
  debug('Setting up the local database')
  await sqldb.setup(opts.database, theSchema, opts.reset)

  // seeing where we got to last time
  if (!opts.reset) {
    const lastTime = await sqldb.getCheckpoint(opts.database)
    opts.since = lastTime || '0'
  }

  // spool changes
  debug('Spooling changes')
  if (opts.verbose) {
    console.log('Run the following command to query your data warehouse:')
    console.log('\n  $ sqlite3 couchwarehouse.sqlite\n')
    console.log('Then in sqlite3, you can run queries e.g.:')
    console.log('\n  sqlite3> SELECT * FROM ' + opts.database + ' LIMIT 10;\n')
    console.log('Have fun!')
    console.log('p.s Press ctrl-C to stop monitoring for further changes')
  }
  const lastSeq = await spoolChanges(opts, theSchema, maxChange)

  // monitor changes
  monitorChanges(opts, theSchema, lastSeq)
}

module.exports = {
  start: start,
  stop: stop
}
