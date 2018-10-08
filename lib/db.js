const sqlite3 = require('sqlite3').verbose()
const flatten = require('./flatten').flatten
let db

// create the database tables
const setup = async (dbName, schema) => {
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
      // create table to hold replication "checkpoints"
      const checkpointSQL = 'CREATE TABLE IF NOT EXISTS couchwarehouse_checkpoints (id INTEGER PRIMARY KEY AUTOINCREMENT, tablename TEXT, seq TEXT)'
      db.run(checkpointSQL, err => {
        if (err) {
          reject(err)
        }
      })

      // create table to hold the data itself
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

// insert an array of changes into the database
const insertBulk = async (dbName, schema, batch) => {
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
          // flatten the document and swap out the keys for $ placeholders
          if (b.doc) {
            b.doc = flatten(b.doc)
            for (var i in b.doc) {
              b.doc['$' + i] = b.doc[i]
              delete b.doc[i]
            }
          }

          // if this is a deletion
          if (b.deleted) {
            // use the DELETE prepared statement
            deleteStmt.run({ '$id': b.id }, (err) => {
              if (err) console.log('Deletion error', err, b.doc)
            })
          } else {
            // use the REPLACE INTO prepared statement
            stmt.run(b.doc, (err) => {
              if (err) {
                console.error('ERROR', err, b.doc)
              }
            })
          }
        }
      })

      // commit the transaction
      db.run('commit', err => {
        if (err) {
          console.error('ERR', err)
          return reject(err)
        }
        resolve()
      })
    })
  })
}

const query = async (sql, substitutions) => {
  substitutions = substitutions || []
  return new Promise((resolve, reject) => {
    db.all(sql, substitutions, (err, rows) => {
      if (err) {
        return reject(err)
      }
      resolve(rows)
    })
  })
}

// write a checkpoint to keep track of where we got to
// with each table
const writeCheckpoint = async (tablename, seq) => {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // create a new transaction
      db.run('begin transaction')

      // write checkpoint
      const sql = 'INSERT INTO couchwarehouse_checkpoints (tablename,seq) VALUES ($tablename,$seq)'
      const stmt = db.prepare(sql)
      stmt.run({ '$tablename': tablename, '$seq': seq }, (err) => {
        if (err) console.log('Checkpoint error', err)
      })

      // commit the transaction
      db.run('commit', err => {
        if (err) {
          console.error('ERR', err)
          return reject(err)
        }
        resolve()
      })
    })
  })
}

const getCheckpoint = async (tablename) => {
  const data = await query('SELECT seq FROM couchwarehouse_checkpoints WHERE tablename=? ORDER BY id DESC LIMIT 1', [ tablename ])
  if (data.length === 1) {
    return data[0].seq
  } else {
    return null
  }
}

module.exports = {
  setup,
  insertBulk,
  query: query,
  writeCheckpoint: writeCheckpoint,
  getCheckpoint: getCheckpoint
}
