#!/usr/bin/env node

// command-line args
const url = process.env.COUCH_URL || 'http://localhost:5984'
const db = process.env.COUCH_DATABASE
const args = require('yargs')
  .option('url', { alias: 'u', describe: 'CouchDB URL', default: url })
  .option('database', { alias: 'db', describe: 'CouchDB database name', demandOption: !db, default: db })
  .option('since', { alias: 's', describe: 'Last known CouchDB changes feed token', demandOption: false, default: '0' })
  .help('help')
  .argv

const couchwarehouse = require('../')

couchwarehouse.start(args)
