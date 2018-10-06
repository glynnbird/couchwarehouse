const sqlite3 = require('sqlite3').verbose()
const flatten = require('./flatten').flatten
let db

// create the database tables and indicies
const setup = (dbName, schema) => {
  db = new sqlite3.Database('couchwarehouse.sqlite')

  // construct fields for CREATE TABLE query
  let fields = ''
  const mapping = {
    'string': 'TEXT',
    'number': 'REAL',
    'boolean': 'INTEGER'
  }
  for (var i in schema) {
    const column = i
    const dataType = schema[i]
    if (fields.length > 0) {
      fields += ', '
    }
    fields += column + ' ' + mapping[dataType]
    if (column === 'id') {
      fields += ' PRIMARY KEY'
    }
  }

  // new database
  // change_id INTEGER PRIMARY KEY AUTOINCREMENT, seq_num, INTEGER, seq TEXT, user TEXT, id TEXT, changes TEXT, deleted BOOLEAN DEFAULT true
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // create table
      const sql = 'CREATE TABLE IF NOT EXISTS ' + dbName + ' (' + fields + ')'
      db.run(sql, err => {
        if (err) {
          return reject(err)
        }
        resolve()
      })
    })
  })
}

// insert an array of changes into the changes database
const insertBulk = (dbName, schema, batch) => {
  // get list of fields
  const fields = Object.keys(schema)
  const replacements = []
  fields.forEach((f) => { replacements.push('$' + f) })

  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // create a new transaction
      db.run('begin transaction')

      // for each change in the array, insert a row into the database
      const sql = 'REPLACE INTO ' + dbName + ' (' + fields.join(',') + ') VALUES (' + replacements.join(',') + ')'
      const deleteSQL = 'DELETE FROM ' + dbName + ' WHERE id = $id'
      const stmt = db.prepare(sql)
      const deleteStmt = db.prepare(deleteSQL)
      batch.forEach(b => {
        // ignore design docs
        if (!b.id.match(/^_design/)) {
          // console.log(JSON.stringify(b))
          if (b.doc) {
            b.doc = flatten(b.doc)
            for (var i in b.doc) {
              b.doc['$' + i] = b.doc[i]
              delete b.doc[i]
            }
          }
          if (b.deleted) {
            deleteStmt.run({ '$id': b.id }, (err) => {
              if (err) console.log('Deletion error', err, b.doc)
            })
          } else {
            stmt.run(b.doc, (err) => {
              if (err) console.log('ERROR', err, b.doc)
            })
          }
        }
      })

      // commit the transaction
      db.run('commit', err => {
        if (err) {
          console.log('ERR', err)
          return reject(err)
        }

        resolve()
      })
    })
  })
}

/*
// simulate a changes feed, segmented by user,
// given a 'since' and a 'user' and a 'limit'
const changes = (opts) => {
  return new Promise((resolve, reject) => {
    const changeResults = {
      results: [],
      last_seq: '0',
      pending: 0
    }

    if (typeof opts.since === 'undefined') {
      opts.since = 0
    } else {
      const bits = opts.since.split('-')
      if (bits.length > 0) {
        opts.since = parseInt(bits[0])
      } else {
        opts.since = 0
      }
    }
    if (typeof opts.limit === 'undefined') {
      opts.limit = 100
    }
    let select = 'SELECT seq, id,  changes, deleted from changes where seq_num > $since AND user = $user ORDER BY change_id ASC LIMIT $limit'

    // run the query exchanging placeholders for passed-in values
    const params = {
      '$since': opts.since,
      '$user': opts.user,
      '$limit': opts.limit
    }
    db.each(select, params, (err, row) => {
      if (err) {
        return reject(err)
      }
      changeResults.last_seq = row.seq

      // simulate CouchDB changes array
      const change = {
        seq: row.seq,
        id: row.id,
        changes: JSON.parse(row.changes),
        user: row.user
      }

      if (row.deleted === 1) {
        change.deleted = true
      }

      changeResults.results.push(change)
    }, (err) => {
      if (err) {
        return reject(err)
      }

      resolve(changeResults)
    })
  })
}

// get the latest change from the changes feed database
const getLatest = (opts) => {
  let since = '0'
  return new Promise((resolve, reject) => {
    const select = 'SELECT seq from changes ORDER BY seq_num DESC LIMIT 1'

    db.each(select, (err, row) => {
      if (err) {
        return reject(err)
      }
      if (row && row.seq) {
        since = row.seq
      }
    }, (err) => {
      if (err) {
        return reject(err)
      }
      resolve(since)
    })
  })
}
*/

module.exports = {
  setup,
  insertBulk
  /*,
  changes,
  insertBulk,
  getLatest */
}
