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
describe('couchwarehouse evolving schema', () => {
  // remove database
  before((done) => {
    // delete the sampledata sqlite database
    db = new sqlite3.Database('couchwarehouse.sqlite')
    db.serialize(() => {
      let sql = 'DROP TABLE IF EXISTS sampledata'
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
      .reply(200, { results: [], last_seq: '30-g1AAAA', pending: 0 })
      .post('/sampledata/_changes?since=0&include_docs=true&seq_interval=100')
      .replyWithFile(200, path.join(__dirname, 'samplechanges3.txt'), { 'Content-Type': 'application/json' })
      .post('/sampledata/_changes?feed=longpoll&timeout=6000&include_docs=true&limit=100&since=30-g1AAAAdreJy91M1NwzAYBmBDKyFOdAO4gpRiO05in-gGsAHY_hyVqk0Qbc6wAWwAG8AGsAFsABvABsWuI9JwairSSyLl533kvJ8zRgj1hh1Ah6B0fm0GoGhfTQKtg2IaTPNiNgwI6etxXoDMZv3MzMb2lW2J1N58Ph8NO3J7Yi_sRBq4VLpJkGOPfp-2N1dwVc8e1X5Jby1oJjGIRDcKqy-Zr0QfOPq4pDsLmgthIE2aZNVltpKMnXxSyt2FbHgCIHCTrDVqVgMnn5Yy8rJOwxAiQLtFBia9zAys1eSZiz6vDZFMMTUibL3JC0fnNdowJWPF2m7yysk3NZljwwhru8msa4_o1p4sfldNsAiF0Spue996_t7zD9XeJRLikNK2G_f8o-efqllOI240xH9nuWGrPv3Zp79U3zYhVGOgG2n21etvS39kkDQheDPNvnv-o1o8U4rg9vey5z89_1U1SyMjccL-pdlvn760Z1koScwbNTv6ATneWpY')
      .delay(1000)
      .reply(500)

    // go warehouse
    const opts = { database: 'sampledata', since: '0', url: 'http://localhost:5984' }
    await couchwarehouse.start(opts)
    couchwarehouse.stop()
    await wait(1200)
    assert(true)
  })

  it('should save and retrieve products', (done) => {
    const select = 'SELECT COUNT(*) as x FROM sampledata'
    db.all(select, [], (err, data) => {
      assert.strictEqual(err, null)
      assert.strictEqual(data[0].x, 30)
      done()
    })
  })

  it('should put nulls in missing fields', (done) => {
    const select = 'SELECT COUNT(*) as x FROM sampledata WHERE dispatchCourierRef IS NULL'
    db.all(select, [], (err, data) => {
      assert.strictEqual(err, null)
      assert.strictEqual(data[0].x, 10)
      done()
    })
  })

  after((done) => {
    // delete the sampledata sqlite database
    db.serialize(() => {
      let sql = 'DROP TABLE IF EXISTS sampledata'
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
