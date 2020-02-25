const schema = require('../lib/schema.js')
const assert = require('assert')

describe('schema', () => {
  it('should discover a schema correctly', (done) => {
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
    const s = schema.discover(doc)

    const idealSchema = {
      name: 'string',
      age: 'number',
      verified: 'boolean',
      description: 'string',
      tags: 'string',
      id: 'string',
      rev: 'string',
      address_street: 'string',
      address_location_long: 'number',
      address_location_lat: 'number'
    }
    assert.deepStrictEqual(s, idealSchema)
    done()
  })
})
