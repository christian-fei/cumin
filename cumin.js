const redis = require('redis')
const consolePrefix = '[cumin]'
const { promisify } = require('util')

const log = (...args) => console.info(consolePrefix, ...args)

module.exports = function (port, host, options) {
  let redisArgs = arguments

  let nonBlockingClient = redis.createClient.apply(redis, redisArgs)
  let blockingClient

  let redisBlpopTimeout = 1

  let alreadyListening = false
  let killSignalReceived = false
  let pendingTasks = 0
  let killWaitTimeout = 20

  function onKillSignal () {
    if (!killSignalReceived) {
      killSignalReceived = true
      log('\n', 'Attempting clean shutdown...')
      log('To force shutdown, hit Ctrl+C again.')
      log('Waiting upto', redisBlpopTimeout, 'seconds for next chance to kill the redis connection...')
      setTimeout(function () {
        log('Forcing kill due to', killWaitTimeout, 'seconds timeout.')
        process.exit()
      }, killWaitTimeout * 1000)
    } else {
      log('\n', 'Forcing shutdown now.')
      setTimeout(process.exit, 500)
    }
  }

  function attemptCleanShutdown () {
    log('Not reconnecting to redis because of kill signal received.')
    if (pendingTasks === 0) {
      log('No pending tasks. Exiting now.')
      process.exit()
    } else {
      log('Waiting for pending tasks to be completed. Pending count:', pendingTasks)
    }
  }

  function continueListening (queueName, handler) {
    var promiseMode = (handler.length < 2)

    if (killSignalReceived) return attemptCleanShutdown()

    blockingClient.blpop(queueName, redisBlpopTimeout, function (err, data) {
      if (err) return console.log(err)

      if (data) {
        var bareQueueName = queueName.slice(('cumin.').length)
        nonBlockingClient.hset('cuminmeta.' + bareQueueName, 'lastDequeued', Date.now())
        nonBlockingClient.publish('cumin.dequeued', data[1])

        var queueItem = JSON.parse(data[1])

        var handlerOnComplete = function () {
          pendingTasks--

          nonBlockingClient.hset('cuminmeta.' + bareQueueName, 'completed', Date.now())
          nonBlockingClient.publish('cumin.processed', data[1])

          if (killSignalReceived && pendingTasks) {
            log('Waiting for pending tasks to be completed. Pending count:', pendingTasks)
          }

          if (killSignalReceived && !pendingTasks) {
            log('Pending tasks completed. Shutting down now.')
            process.exit()
          }
        }

        pendingTasks++

        if (promiseMode) {
          handler(queueItem.data).then(handlerOnComplete)
        } else {
          handler(queueItem.data, handlerOnComplete)
        }
      }

      process.nextTick(function () {
        continueListening(queueName, handler)
      })
    })
  }

  return {
    enqueue: promisify(function (queueName, queueData, done) {
      if (!queueName) {
        throw new Error("Queue name must be provided. eg. 'emailQueue'.")
      }

      var now = Date.now()
      var message = JSON.stringify({
        byPid: process.pid,
        byTitle: process.title,
        queueName: queueName,
        date: now,
        data: queueData,
        retryCount: 0
      })

      nonBlockingClient.sadd('cuminqueues', queueName)
      nonBlockingClient.hset('cuminmeta.' + queueName, 'lastEnqueued', now)
      nonBlockingClient.rpush('cumin.' + queueName, message)
      nonBlockingClient.publish('cumin.enqueued', message, done)
    }),

    listen: function (queueName, handler) {
      if (!queueName) {
        throw new Error(consolePrefix, "Queue name must be provided. eg. 'emailQueue'.")
      }

      if (!handler) {
        throw new Error(consolePrefix, 'You must provide a hander to .listen.')
      }

      if (alreadyListening) {
        throw new Error(consolePrefix, 'You can only .listen once in an app. To listen to another queue, create another app.')
      }

      if (!blockingClient) {
        blockingClient = redis.createClient.apply(redis, redisArgs)
      }

      process.on('SIGINT', onKillSignal)
      process.on('SIGTERM', onKillSignal)
      alreadyListening = true

      if (handler.length < 2) {
        console.log('Assuming that the handler returns a promise.')
      }

      continueListening('cumin.' + queueName, handler)
    }
  }
}
