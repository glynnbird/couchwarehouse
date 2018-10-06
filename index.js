const ChangesReader = require('changesreader')
const schema = require('./lib/schema.js')
const sqldb = require('./lib/db.js')
let nano

const extractSequenceNumber = (seq) => {
  return parseInt(seq.replace(/-.*$/, ''))
}

const progressBar = (currentChange, maxChange, lastOne) => {
  const percentDone = Math.floor(100 * (currentChange / maxChange))
  const equals = Math.floor(percentDone / 5)
  const dash = 20 - equals
  const crlf = (lastOne) ? '\r' : '\n'
  process.stdout.write(' [' + '='.repeat(equals) + '-'.repeat(dash) + '] ' + percentDone + '  ' + crlf)
}

const spoolChanges = async (opts, theSchema, maxChange) => {
  let currentChange = 0
  let lastSeq

  return new Promise((resolve, reject) => {
    const changesReader = new ChangesReader(opts.database, nano.request)
    changesReader.spool({ since: opts.since, includeDocs: true }).on('batch', async (b) => {
      if (b.length > 0) {
        // get latest sequence token
        lastSeq = b[b.length - 1].seq
        currentChange = extractSequenceNumber(lastSeq)

        // perform database operation
        await sqldb.insertBulk(opts.database, theSchema, b)

        // update the progress bar
        progressBar(currentChange, maxChange)
      }
    }).on('end', () => {
      progressBar(currentChange, maxChange, true)
      console.log('changes feed monitoring has completed')
      console.log('Run the following command to query your database:')
      console.log('\n  $ sqlite3 couchwarehouse.sqlite\n')
      console.log('Then in sqlite3, you can run queries e.g.:')
      console.log('\n  sqlite3> SELECT * FROM ' + opts.database + ' LIMIT 10;\n')
      console.log('Have fun!')
      resolve(lastSeq)
    }).on('error', reject)
  })
}

const monitorChanges = async function (opts, theSchema, lastSeq) {
  return new Promise((resolve, reject) => {
    const changesReader = new ChangesReader(opts.database, nano.request)
    changesReader.start({ since: lastSeq }).on('batch', async (b) => {
      process.stdout.write('.')
      await sqldb.insertBulk(opts.database, theSchema, b)
    }).on('error', reject)
  })
}

const start = async (opts) => {
  nano = require('nano')(opts.url)
  let maxChange

  // get latest revision token of the target database, to
  // give us something to aim for
  console.log('Getting last change')
  const req = { db: opts.database, path: '_changes', qs: { since: 'now', limit: 1 } }
  const info = await nano.request(req)
  maxChange = extractSequenceNumber(info.last_seq)

  // get 50 documents from the database for schema discovery
  console.log('Getting docs for schema discovery')
  const db = nano.db.use(opts.database)
  const exampleDocs = await db.list({ limit: 50, include_docs: true })

  // calculate the schema from the example docs
  console.log('Calculating the schema')
  const theSchema = schema.discover(exampleDocs.rows)
  console.log('schema', theSchema)

  // setup the local database
  console.log('Setting up the local database')
  await sqldb.setup(opts.database, theSchema)

  // spool changes
  console.log('Spooling changes')
  const lastSeq = await spoolChanges(opts, theSchema, maxChange)

  // monitor changes
  console.log('monitoring for further changes')
  console.log('p.s Press ctrl-C to stop monitoring for further changes')
  monitorChanges(opts, theSchema, lastSeq)
}

module.exports = {
  start: start
}
