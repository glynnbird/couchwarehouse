/* PGUSER=dbuser \
  PGHOST=database.server.com \
  PGPASSWORD=secretpassword \
  PGDATABASE=mydb \
  PGPORT=3211 */

const mysql = require('mysql')
const flatten = require('./flatten').flatten
let db

class Database {
  constructor (config) {
    this.connection = mysql.createConnection(config)
  }
  query (sql, args) {
    return new Promise((resolve, reject) => {
      this.connection.query(sql, args, (err, rows) => {
        if (err) { return reject(err) }
        resolve(rows)
      })
    })
  }
  close () {
    return new Promise((resolve, reject) => {
      this.connection.end(err => {
        if (err) {
          return reject(err)
        }
        resolve()
      })
    })
  }
}

const initialise = async (reset) => {
  const config = process.env.MYSQLCONFIG || 'mysql://root:@localhost:3306/couchwarehouse'
  db = new Database(config)
  await db.query('CREATE TABLE IF NOT EXISTS couchwarehouse_checkpoints (id SERIAL PRIMARY KEY, tablename TEXT, seq TEXT)')
}

const generateCreateTableSQL = (opts, docType, dbName, schema, reset) => {
  // construct fields for CREATE TABLE query
  let fields = ''
  const mapping = {
    'string': 'TEXT',
    'number': 'FLOAT',
    'boolean': 'TINYINT'
  }
  for (var i in schema) {
    const column = i
    const dataType = schema[i]
    if (fields.length > 0) {
      fields += ', '
    }
    if (column === 'id') {
      fields += column + ' VARCHAR(255)'
      fields += ' PRIMARY KEY'
    } else {
      fields += column + ' ' + mapping[dataType]
    }
  }

  // calculate database name
  const usableDbName = opts.split ? dbName + '_' + docType : dbName

  const sql = []
  if (reset) {
    sql.push('DROP TABLE IF EXISTS ' + usableDbName)
  }
  sql.push('CREATE TABLE IF NOT EXISTS ' + usableDbName + ' (' + fields + ')')
  return sql
}

// insert an array of changes into the database
const insertBulk = async (opts, createSQL, dbName, theSchema, batch) => {
  await db.query('BEGIN')

  // run create SQL first
  if (createSQL.length > 0) {
    for (var j in createSQL) {
      await db.query(createSQL[j])
    }
  }

  batch.forEach(async b => {
    // ignore design docs
    if (!b.id.match(/^_design/)) {
      // get the schema we're working with
      const docType = opts.split ? b.doc[opts.split] : '_default'
      const schema = theSchema[docType]

      // get list of fields
      const fields = Object.keys(schema)
      const replacements = []
      fields.forEach((f) => {
        replacements.push('?')
      })

      // for each change in the array, insert a row into the database
      const usableDbName = opts.split ? dbName + '_' + b.doc[opts.split] : dbName
      let sql = 'REPLACE INTO ' + usableDbName + ' (' + fields.join(',') + ') VALUES (' + replacements.join(',') + ')'
      let deleteSQL = 'DELETE FROM ' + usableDbName + ' WHERE id = ?'

      // flatten the document and swap out the keys for $ placeholders
      const values = []
      if (b.doc) {
        b.doc = flatten(b.doc)
        for (var i in schema) {
          let v = typeof b.doc[i] !== 'undefined' ? b.doc[i] : null
          values.push(v)
        }
        values.push(b.id)
      }

      // if this is a deletion
      try {
        if (b.deleted) {
          // use the DELETE prepared statement
          deleteSQL = mysql.format(deleteSQL, [b.id])
          await db.query(deleteSQL)
        } else {
          // use the REPLACE INTO prepared statement
          sql = mysql.format(sql, values)
          await db.query(sql)
        }
      } catch (e) {
        console.error('ERROR', e)
      }
    }
  })

  // commit the transaction
  try {
    await db.query('COMMIT')
  } catch (e) {
    console.error('ERROR', e)
  }
}

const query = async (sql, substitutions) => {
  const data = await db.query(sql, substitutions)
  return data
}

// write a checkpoint to keep track of where we got to
// with each table
const writeCheckpoint = async (tablename, seq) => {
  var sql = 'INSERT INTO couchwarehouse_checkpoints (tablename,seq) VALUES (?,?)'
  sql = mysql.format(sql, [tablename, seq])
  await db.query(sql)
}

const getCheckpoint = async (tablename) => {
  let sql = 'SELECT seq FROM couchwarehouse_checkpoints WHERE tablename=? ORDER BY id DESC LIMIT 1'
  let retval = null
  sql = mysql.format(sql, [tablename])
  const data = await db.query(sql)
  if (data.length > 0) {
    retval = data[0].seq
  }
  return retval
}

const message = (opts) => {
  console.log('Run the following command to query your data warehouse in MySQL:')
  console.log('\n  $ mysql -u root\n')
  console.log('Then in shell, you can run queries e.g.:')
  console.log('\n  mysql> connect couchwarehouse;')
  console.log('  mysql> SELECT * FROM ' + opts.database + ' LIMIT 10;\n')
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
