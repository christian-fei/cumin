/* globals describe, it */
const redisClient = require('redis').createClient()
const cuminCreator = require('../')
const cumin = cuminCreator()
const assert = require('assert')

describe('.enqueue', () => {
  it('should enqueue a job', done => {
    cumin.enqueue('test.list', {some: 'task'}, err => {
      assert.ifError(err)

      redisClient.llen('cumin.test.list', (err, len) => {
        assert.ifError(err)
        assert.equal(len, 1)

        redisClient.lpop('cumin.test.list', (err, item) => {
          assert.ifError(err)

          item = JSON.parse(item)
          assert.equal(typeof item.byPid, 'number')
          assert.equal(typeof item.byTitle, 'string')
          assert.equal(typeof item.date, 'number')
          assert.equal(item.queueName, 'test.list')
          assert.ok(Date.now() - item.date > 0 && Date.now() - item.date < 500)
          assert.deepEqual(item.data, {some: 'task'})
          done()
        })
      })
    }, 100)
  })
})

describe('.listen', () => {
  it('should listen to a list', done => {
    cumin.enqueue('test.list', {some: 'task'})

    cumin.listen('test.list', (queueData, doneTask) => {
      assert.deepEqual(queueData, {some: 'task'})
      doneTask()
      done()
    })
  })

  it('should not listen to another list while already listening to the first', () => {
    try {
      cumin.listen('test.list2', () => {})
      assert.fail('should have failed to listen')
    } catch (e) {
      assert.ok(e)
    }
  })

  it('should not even listen to the same list again', () => {
    try {
      cumin.listen('test.list', () => {})
      assert.fail('should have failed to listen')
    } catch (e) {
      assert.ok(e)
    }
  })

  it('should enqueue with promises', done => {
    cumin.enqueue('test.list3', { some: 'task' }).then((task) => {
      done()
    })
  })

  it('should listen with promises', done => {
    const cumin2 = cuminCreator()
    const delay = () => new Promise((resolve, reject) => setTimeout(resolve, 300))
    cumin2.listen('test.list3', async task => {
      await delay()
      done()
    })
  })
})
