# couchwarehouse

*couchwarehouse* is a command-line tool that turns your [Apache CouchDB](http://couchdb.apache.org/) database(s) into a local data warehouse. It works by:

- discovering the "schema" of your CouchDB database.
- creating a new local [SQLite](https://www.sqlite.org/index.html) table to match the schema.
- downloading all the documents (except design documents) and inserting one row per document into SQLite.
- continuously monitoring CouchDB for new documents, updates to existing documents and deletions.

![](img/couchwarehouse.png)

Once downloaded your database can be queried using SQL.

## Installation

[Node.js version 8 or above and npm](https://nodejs.org/en/) are required:

```sh
npm install -g couchwarehouse
```

## Running

By default, your CouchDB installation is expected to be on "http://localhost:5984". Override this with the `--url`/`-u` parameter and specify the database name with `--database`/`-db`:

```sh
$ couchwarehouse --url https://U:P@host.cloudant.com --db mydb
Run the following command to query your data warehouse:

  $ sqlite3 couchwarehouse.sqlite

Then in sqlite3, you can run queries e.g.:

  sqlite3> SELECT * FROM cities LIMIT 10;

Have fun!
p.s Press ctrl-C to stop monitoring for further changes
downloading mydb [======------------------------] 20% 27.7s
```

After downloading is complete, *couchwarehouse* will continuously poll the source database for any changes and update the local database accordingly. 

Press "Ctrl-C" to exit.

## Accessing the data warehouse

In another terminal, simply run the `sqlite3` command-line tool (which may be pre-installed on your computer, otherwise [download here](https://www.sqlite.org/download.html)).

```sh
$ sqlite3 couchwarehouse.sqlite
sqlite3> SELECT name,latitude,longitude,country,population FROM mydb LIMIT 10;
name                    latitude    longitude   country     population
----------------------  ----------  ----------  ----------  ----------
Brejo da Madre de Deus  -8.14583    -36.37111   BR          27369.0   
Pindaré Mirim           -3.60833    -45.34333   BR          22933.0   
Moju                    -1.88389    -48.76889   BR          21510.0   
Matriz de Camaragibe    -9.15167    -35.53333   BR          18705.0   
Fatikchari              22.68768    91.78123    BD          33200.0   
Picos                   -7.07694    -41.46694   BR          57495.0   
Balsas                  -7.5325     -46.03556   BR          68056.0   
Jaguaruana              -4.83389    -37.78111   BR          21790.0   
Pilar                   -9.59722    -35.95667   BR          30617.0   
Patos                   -7.02444    -37.28      BR          92575.0 
```

SQLite has an [extensive query language](https://www.sqlite.org/lang.html) including aggregations, joins and much more. You may create warehouses from multiple CouchDB databases to create multiple SQLite tables and join them with queries!

## Command-line parameter reference

- `--url`/`-u` - the URL of the CouchDB instance e.g. `http://localhost:5984`
- `--database`/`--db`/`-d` - the name of the CouchDB database to work with
- `--verbose` - whether to show progress on the terminal (default: true)
- `--reset`/`-r` - reset the data. Delete existing data and start from scratch (default: false)
- `--transform`/`-t` - transform each document with a supplied JavaScript function (default: null)
- `--version` - show version number
- `--help` - show help

The CouchDB URL can also be specified with the `COUCH_URL` environment variable e.g.

```sh
export COUCH_URL="https://USER:PASS@host.cloudant.com"
couchwarehouse --db mydb
```

## Transforming documents

If you need to format the data prior to it being stored in the SQLite database, you may optionally supply
a JavaScript transformation function with the `--transform`/`-t` parameter.

Create a JavaScript file, in this case called `transform.js`:

```js
const f = (doc) => {
  // remove the basket array
  delete doc.basket

  // trim whitespace from the category
  doc.category = doc.category.trim()
  
  // combine the title/firstname/surname into one field
  doc.name = [doc.title, doc.firstname, doc.surname].join(' ') 
  delete doc.title
  delete doc.firstname
  delete doc.surname

  // return the transformed document
  return doc
}

// export the function
module.exports = f
```

Then instruct *couchwarehouse* to use the function:

```sh
couchwarehouse --db mydb --transform './transform.js' --reset true
```

## Schema discovery

CouchDB is a JSON document store and as such, the database does not have a fixed schema. The *couchwarehouse* utility takes a look at a handful of documents and *infers* a schema from what it sees. This is clearly only of use if your CouchDB documents that have similar documents.

Let's take a typical document that looks like this:

```js
{
  "_id": "afcc37fbe6ff4dd35ecf06be51e45724",
  "_rev": "1-d076609f1a507282af4e4eb52da6f4f1",
  "name": "Bob",
  "dob": "2000-05-02",
  "employed": true,
  "grade": 5.6,
  "address": {
    "street": "19 Front Street, Durham",
    "zip": "88512",
    "map": {
      "latitude": 54.2,
      "longitude": -1.5
    }
  },
  "tags": [
    "dev",
    "front end"
  ]
}
```

*couchwarehouse* will infer the following schema:

```js
{
  "id": "string",
  "rev": "string",
  "name": "string",
  "dob": "string",
  "employed": "boolean",
  "grade": "number",
  "address_street": "string",
  "address_zip": "string",
  "address_map_latitude": "number",
  "address_map_longitude": "number",
  "tags": "string"
}
```

Notice how:

- the sub-objects are "flattened" e.g. `address.map.latitude` --> `address_map_latitude`
- arrays are turned into strings
- _id/_rev become id/rev

The keys of the schema become the column names of the SQLite table.

## Removing unwanted SQLite tables

Unwanted tables can be easily removed using the `sqlite3` prompt:

```sh
sqlite> DROP TABLE mydb;
```

The whole SQLite database can be removed by deleting the `couchwarehouse.sqlite` file 
from your file system.

## What's the catch?

- you need enough memory and hard disk space to store the entire database on your machine
- conflicted document bodies are ignored
- objects are flattened
- arrays are stored as their JSON representation
- this approach is only suitable where each document in the database has the same schema. If you are storing different object types in the same database (e.g. `type: "Person", type: "Order"`), then consider using [Filtered Replication](http://docs.couchdb.org/en/stable/replication/intro.html) to separte the objects into their own database before using this tool

## Using programmatically

This library can be used programmatically too:

```js
const couchwarehouse = require('couchwarehouse')

// configuration
const opts = {
  url: 'https://USER:PASS@host.cloudant.com',
  database: 'mydb'
}

const main = async () => {
  // start downloading data - wait until changes feed is complete
  await couchwarehouse.start(opts)

  // query the database
  couchwarehouse.query('SELECT * FROM mydb').then((data) => {
    console.log(data)
  })
}
main()
```

The `opts` object passed to `couchwarehouse.start` can contain:

- `url` - the URL of the CouchDB instance e.g. `http://localhost:5984`
- `database` - the name of the CouchDB database to work with
- `verbose` - whether to show progress on the terminal (default: true)
- `reset` - reset the data. Delete existing data and start from scratch (default: false)

## Debugging

Starting *couchwarehouse* with the `DEBUG` environment variable set will produce extra output e.g.

```sh
DEBUG=* couchwarehouse --db mydb
```
