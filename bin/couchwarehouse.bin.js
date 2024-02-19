#!/usr/bin/env node

// command-line args
const url = process.env.COUCH_URL || 'http://localhost:5984'
const db = process.env.COUCH_DATABASE
const args = require('yargs')
  .option('url', { alias: 'u', describe: 'CouchDB URL', default: url })
  .option('database', { alias: ['db', 'd'], describe: 'CouchDB database name', demandOption: !db, default: db })
  .option('databaseType', { alias: ['dt'], describe: 'Target database type (postgresql,mysql,sqlite,elasticsearch)', default: 'sqlite' })
  .option('verbose', { describe: 'Show instructions and progress in the output', default: true })
  .option('reset', { alias: 'r', describe: 'Ignore previously downloaded data and start again', default: false })
  .option('transform', { alias: 't', describe: 'Path to a JavaScript transformation function', default: process.env.COUCH_TRANSFORM ? process.env.COUCH_TRANSFORM : null })
  .option('split', { alias: 's', describe: 'Document field name used to split documents into separate tables', default: null })
  .option('query', { alias: 'q', describe: 'Use Mango Query to filter data', default: null })
  .help('help')
  .argv

// start the data warehouse
const couchwarehouse = require('../')
couchwarehouse.start(args)
