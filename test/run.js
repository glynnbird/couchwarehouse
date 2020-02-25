const assert = require('assert')
const couchwarehouse = require('..')
const nock = require('nock')
const path = require('path')
const sqlite3 = require('sqlite3').verbose()
let db

const wait = (ms) => {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}
describe('couchwarehouse', () => {
  // remove database
  before((done) => {
    // delete the sampledata sqlite database
    db = new sqlite3.Database('couchwarehouse.sqlite')
    db.serialize(() => {
      const sql = 'DROP TABLE IF EXISTS sampledata'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      const sql2 = 'DELETE FROM couchwarehouse_checkpoints WHERE tablename="sampledata"'
      db.run(sql2, err => {
        if (err) { }
        done()
      })
    })
  })

  // consume changes feed
  it('should turn a changes feed into a database', async () => {
    // create mocks
    const n = nock('http://localhost:5984')
    n.get('/sampledata/_changes?since=now&limit=1')
      .reply(200, { results: [], last_seq: '104-g1AAAA', pending: 0 })
      .post('/sampledata/_changes?since=0&include_docs=true&seq_interval=100')
      .replyWithFile(200, path.join(__dirname, 'samplechanges.txt'), { 'Content-Type': 'application/json' })
      .get('/sampledata/_changes?feed=longpoll&timeout=60000&include_docs=true&limit=100&since=104-g1AAAAfLeJy91c1NwzAYBuCoLZSfA90AriCl2I3jLz7RDWAD8F9UVW2DaHuGDWAD2AA2gA1gA9gANihxvoiSEw2Se3GkyPkevXrjZBQEQWfQNMGhUTq7sn2joq4ah1qH82k4zeazQUhpV4-yuZGTWXdiZ6P8kYYM1N5isRgOmrI9zm-0YyDMGltnkGOPfnZTsoqrOvmq9kt6q6ANEE0kqzWsGhlWog8cfVxJnZgoTqup_5pVlflKMnHySSm3CllzwXVs68z6R82q7-TTUm4UMok5B556b_rM0eclvVPQVggR0cR70xeOzkp6o2zaSs69N33p5OvK6x0JEKCJ56YnrXwNbvJLjt8uc8seALfKd9vI3yF_7_gmhk9YYoH6bhz5B-Qfl7zSGlIjPbeO-hPqz8vmQXHKxXqaf0H99ddnlcU20fF6mn9D_t3xmxjeUGGY97OO_Afyn47fxf9Z2pNRAmtp_gv14sxvFzoXEhijdeYNvwH6rHtw')
      .delay(1000)
      .reply(500)

    // go warehouse
    const opts = { database: 'sampledata', since: '0', url: 'http://localhost:5984' }
    await couchwarehouse.start(opts)
    couchwarehouse.stop()
    await wait(1200)
    assert(true)
  })

  it('should save and retrieve the data correctly', (done) => {
    const select = 'SELECT * FROM sampledata ORDER BY id'
    db.all(select, [], (err, rows) => {
      assert.strictEqual(err, null)
      assert.deepStrictEqual(rows, require('./samplequery.json'))
      done()
    })
  })

  after((done) => {
    // delete the sampledata sqlite database
    db = new sqlite3.Database('couchwarehouse.sqlite')
    db.serialize(() => {
      const sql = 'DROP TABLE IF EXISTS sampledata'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      const sql2 = 'DELETE FROM couchwarehouse_checkpoints WHERE tablename="sampledata"'
      db.run(sql2, err => {
        assert.strictEqual(err, null)
        done()
      })
    })
  })
})
