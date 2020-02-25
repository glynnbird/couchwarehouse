const flatten = require('../lib/flatten.js').flatten
const assert = require('assert')

describe('flatten', () => {
  it('should flatten a document correctly', (done) => {
    const doc = {
      _id: '001g6oKR0vDE2w0VHHGR0cWpbd4TxhKZ',
      _rev: '1-24e48b12e537a24e9c893054f996b67b',
      name: 'Jina Pulley',
      age: 30,
      verified: false,
      description: 'suited internal sussex hotmail shots deficit meal outcome date congratulations',
      address: {
        street: '6343 Boden Road,New York City,Texas,69487',
        location: {
          long: -80.5833,
          lat: 35.2358
        }
      },
      tags: [
        'feet',
        'sections',
        'increased'
      ]
    }
    // flattena clone of the original doc
    const odoc = JSON.parse(JSON.stringify(doc))
    const fdoc = flatten(odoc)
    assert.strictEqual(doc._id, fdoc.id)
    assert.strictEqual(doc._rev, fdoc.rev)
    assert.strictEqual(doc.name, fdoc.name)
    assert.strictEqual(doc.age, fdoc.age)
    assert.strictEqual(doc.verified, fdoc.verified)
    assert.strictEqual(doc.description, fdoc.description)
    assert.strictEqual(doc.address.street, fdoc.address_street)
    assert.strictEqual(doc.address.location.long, fdoc.address_location_long)
    assert.strictEqual(doc.address.location.lat, fdoc.address_location_lat)
    assert.strictEqual(JSON.stringify(doc.tags), fdoc.tags)
    done()
  })
})
