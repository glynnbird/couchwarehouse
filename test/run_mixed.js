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
describe('couchwarehouse split mode', () => {
  // remove database
  before((done) => {
    // delete the sampledata sqlite database
    db = new sqlite3.Database('couchwarehouse.sqlite')
    db.serialize(() => {
      let sql = 'DROP TABLE IF EXISTS sampledata_product'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      sql = 'DROP TABLE IF EXISTS sampledata_user'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      sql = 'DROP TABLE IF EXISTS sampledata_order'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      sql = 'DELETE FROM couchwarehouse_checkpoints WHERE tablename="sampledata"'
      db.run(sql, err => {
        if (err) {
          // do nothing
        }
        done()
      })
    })
  })

  // consume changes feed
  it('should turn a changes feed into a database', async () => {
    // create mocks
    const n = nock('http://localhost:5984')
    n.get('/sampledata/_changes?since=now&limit=1')
      .reply(200, { results: [], last_seq: '230-g1AAAA', pending: 0 })
      .post('/sampledata/_changes?since=0&include_docs=true&seq_interval=100')
      .replyWithFile(200, path.join(__dirname, 'samplechanges2.txt'), { 'Content-Type': 'application/json' })
      .post('/sampledata/_changes?feed=longpoll&timeout=60000&include_docs=true&limit=100&since=230-g1AAAAfLeJy91c1NwzAYBuAIkED8qOXEEa4gpcSuE8cnugFsAPFnW1XVNoi2Z9gANoANYAPYADaADWCD4j_J9NYguRdHipT3kfz6c4ZJkrT76yI5FhzqG9kTHHX4KAVIZ5N0Us-m_RShDgzrmajG085YTof6k7Uq4a35fD7or1c7I_1ikyEBWYGbBC2ybBmWt_XKD73csjIhVFCCmmQtyvlS8pGRT728ZeW8ZByVvEmWkU_CDuGl6MzQZ57etzQAQxh3G4X9o2beM_S5pw9c04ooWkHspi-MfOnlXStjJgBBGbvpKyPXXm5buUvLnDEZvelrQ996es_SmRIFklXspscbek3u9EPr94HnGCvIZOS2nf7g9MdwzKsup1zEnm2nPzn92ejbVleUEn2lxW7d8S-Ofw1bLwnLoYo-445_c_x7mHOpBFEFWknzH07_DLc6ypGUClbS_JfTv8PfTCKAvIDVNP_j-D8zT1VOeJY1Chz8At5pfQE')
      .delay(1000)
      .reply(500)

    // go warehouse
    const opts = { database: 'sampledata', since: '0', url: 'http://localhost:5984', split: 'type' }
    await couchwarehouse.start(opts)
    couchwarehouse.stop()
    await wait(1200)
    assert(true)
  })

  it('should save and retrieve products', (done) => {
    const select = 'SELECT COUNT(*) as x FROM sampledata_product'
    db.all(select, [], (err, data) => {
      assert.strictEqual(err, null)
      assert.strictEqual(data[0].x, 20)
      done()
    })
  })

  it('should save and retrieve orders', (done) => {
    const select = 'SELECT COUNT(*) as x FROM sampledata_order'
    db.all(select, [], (err, data) => {
      assert.strictEqual(err, null)
      assert.strictEqual(data[0].x, 200)
      done()
    })
  })

  it('should save and retrieve users', (done) => {
    const select = 'SELECT COUNT(*) as x FROM sampledata_user'
    db.all(select, [], (err, data) => {
      assert.strictEqual(err, null)
      assert.strictEqual(data[0].x, 10)
      done()
    })
  })

  after((done) => {
    // delete the sampledata sqlite database
    db.serialize(() => {
      let sql = 'DROP TABLE IF EXISTS sampledata_product'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      sql = 'DROP TABLE IF EXISTS sampledata_user'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      sql = 'DROP TABLE IF EXISTS sampledata_order'
      db.run(sql, err => {
        assert.strictEqual(err, null)
      })
      sql = 'DELETE FROM couchwarehouse_checkpoints WHERE tablename="sampledata"'
      db.run(sql, err => {
        assert.strictEqual(err, null)
        done()
      })
    })
  })
})
