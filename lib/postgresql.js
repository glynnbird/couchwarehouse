/* PGUSER=dbuser \
  PGHOST=database.server.com \
  PGPASSWORD=secretpassword \
  PGDATABASE=mydb \
  PGPORT=3211 */

const { Client } = require('pg')
const client = new Client()
const flatten = require('./flatten').flatten
const util = require('./util.js')

const initialise = async (reset) => {
  await client.connect()
  await client.query('BEGIN')
  await client.query('CREATE TABLE IF NOT EXISTS couchwarehouse_checkpoints (id SERIAL PRIMARY KEY, tablename TEXT, seq TEXT)')
  await client.query('COMMIT')
}

const generateCreateTableSQL = (opts, docType, dbName, schema, reset) => {
  // construct fields for CREATE TABLE query
  let fields = ''
  const mapping = {
    string: 'TEXT',
    number: 'REAL',
    boolean: 'BOOLEAN'
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
  await client.query('BEGIN')

  // run create SQL first
  if (createSQL.length > 0) {
    for (var j in createSQL) {
      await client.query(createSQL[j])
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
      const pairs = []
      let j = 1
      fields.forEach((f) => {
        replacements.push('$' + j)
        const pair = f + ' = $' + j
        pairs.push(pair)
        j++
      })

      // for each change in the array, insert a row into the database
      const usableDbName = util.calculateUsableDbName(opts, dbName, docType)
      const sql = 'INSERT INTO ' + usableDbName + ' (' + fields.join(',') + ') VALUES (' + replacements.join(',') + ') ON CONFLICT (id) DO UPDATE SET ' + pairs.join(',') + ' WHERE ' + usableDbName + '.id = $' + (fields.length + 1)
      const deleteSQL = 'DELETE FROM ' + usableDbName + ' WHERE id = $1'

      // flatten the document and swap out the keys for $ placeholders
      const values = []
      if (b.doc) {
        b.doc = flatten(b.doc)
        for (var i in schema) {
          const v = typeof b.doc[i] !== 'undefined' ? b.doc[i] : null
          values.push(v)
        }
        values.push(b.id)
      }

      // if this is a deletion
      try {
        if (b.deleted) {
          // use the DELETE prepared statement
          await client.query(deleteSQL, [b.id])
        } else {
          // use the REPLACE INTO prepared statement
          await client.query(sql, values)
        }
      } catch (e) {
        console.error('ERROR', e)
      }
    }
  })

  // commit the transaction
  try {
    await client.query('COMMIT')
  } catch (e) {
    console.error('ERROR', e)
  }
}

const query = async (sql, substitutions) => {
  const data = await client.query(sql, substitutions)
  return data
}

// write a checkpoint to keep track of where we got to
// with each table
const writeCheckpoint = async (tablename, seq) => {
  const sql = 'INSERT INTO couchwarehouse_checkpoints (tablename,seq) VALUES ($1,$2)'
  await client.query(sql, [tablename, seq])
}

const getCheckpoint = async (tablename) => {
  const sql = 'SELECT seq FROM couchwarehouse_checkpoints WHERE tablename=$1 ORDER BY id DESC LIMIT 1'
  let retval = null
  const data = await client.query(sql, [tablename])
  if (data.rows.length > 0) {
    retval = data.rows[0].seq
  }
  return retval
}

const message = (opts) => {
  console.log('Run the following command to query your data warehouse in PostgreSQL:')
  console.log('\n  $ psql\n')
  console.log('Then in shell, you can run queries e.g.:')
  console.log('\n  user=# SELECT * FROM ' + opts.usableDbName + ' LIMIT 10;\n')
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
