const sqlite3 = require('sqlite3').verbose()
const flatten = require('./flatten').flatten
const util = require('./util.js')
let db

const initialise = async (reset) => {
  db = new sqlite3.Database('couchwarehouse.sqlite')
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // create table to hold replication "checkpoints"
      const checkpointSQL = 'CREATE TABLE IF NOT EXISTS couchwarehouse_checkpoints (id INTEGER PRIMARY KEY AUTOINCREMENT, tablename TEXT, seq TEXT)'
      db.run(checkpointSQL, err => {
        if (err) {
          reject(err)
        }
        resolve()
      })
    })
  })
}

// create the database tables
const generateCreateTableSQL = (opts, docType, dbName, schema, reset) => {
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

  // calculate database name
  const usableDbName = util.calculateUsableDbName(opts, dbName, docType)

  const sql = []
  if (reset) {
    sql.push('DROP TABLE IF EXISTS ' + usableDbName)
  }
  sql.push('CREATE TABLE IF NOT EXISTS ' + usableDbName + ' (' + fields + ')')
  return sql
}

// insert an array of changes into the database
const insertBulk = async (opts, createSQL, dbName, theSchema, batch) => {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // create a new transaction
      db.run('begin transaction')

      // run create SQL first
      if (createSQL.length > 0) {
        for (var j in createSQL) {
          db.run(createSQL[j])
        }
      }

      batch.forEach(b => {
        // ignore design docs
        if (!b.id.match(/^_design/)) {
          // get the schema we're working with
          const docType = opts.split ? b.doc[opts.split] : '_default'
          const schema = theSchema[docType]

          // get list of fields
          const fields = Object.keys(schema)
          const replacements = []
          fields.forEach((f) => { replacements.push('$' + f) })

          // for each change in the array, insert a row into the database
          const usableDbName = util.calculateUsableDbName(opts, dbName, docType)
          const sql = 'REPLACE INTO ' + usableDbName + ' (' + fields.join(',') + ') VALUES (' + replacements.join(',') + ')'
          const deleteSQL = 'DELETE FROM ' + usableDbName + ' WHERE id = $id'

          // flatten the document and swap out the keys for $ placeholders
          const sqlDoc = {}
          if (b.doc) {
            b.doc = flatten(b.doc)
            for (var i in schema) {
              sqlDoc['$' + i] = typeof b.doc[i] !== 'undefined' ? b.doc[i] : null
            }
          }

          // if this is a deletion
          if (b.deleted) {
            // use the DELETE prepared statement
            const deleteStmt = db.prepare(deleteSQL)
            deleteStmt.run({ '$id': b.id })
          } else {
            // use the REPLACE INTO prepared statement
            const stmt = db.prepare(sql)
            stmt.run(sqlDoc, (err) => {
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
        console.error(err)
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
        if (err) console.error('Checkpoint error', err)
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

const message = (opts) => {
  console.log('Run the following command to query your data warehouse:')
  console.log('\n  $ sqlite3 couchwarehouse.sqlite\n')
  console.log('Then in sqlite3, you can run queries e.g.:')
  console.log('\n  sqlite3> SELECT * FROM ' + opts.usableDbName + ' LIMIT 10;\n')
  console.log('Have fun!')
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
