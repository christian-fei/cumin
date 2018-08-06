const redis = require('redis')
const { promisify } = require('util')

const CONSOLE_PREFIX = '[cumin]'
const QUEUE_PREFIX = 'cumin.'
const log = (...args) => console.info(CONSOLE_PREFIX, ...args)

module.exports = function (port, host, options) {
  const redisArgs = arguments

  let blockingClient
  const nonBlockingClient = redis.createClient.apply(redis, redisArgs)
  const redisBlpopTimeout = 1
  const killWaitTimeout = 20
  const state = {
    alreadyListening: false,
    killSignalReceived: false,
    pendingTasks: 0
  }

  function onKillSignal () {
    if (!state.killSignalReceived) {
      state.killSignalReceived = true
      log('Attempting clean shutdown...')
      log('To force shutdown, hit Ctrl+C again.')
      log('Waiting upto', redisBlpopTimeout, 'seconds for next chance to kill the redis connection...')
      setTimeout(function () {
        log('Forcing kill due to', killWaitTimeout, 'seconds timeout.')
        process.exit()
      }, killWaitTimeout * 1000)
    } else {
      log('Forcing shutdown now.')
      setTimeout(process.exit, 500)
    }
  }

  function attemptCleanShutdown () {
    log('Not reconnecting to redis because of kill signal received.')
    if (state.pendingTasks === 0) {
      log('No pending tasks. Exiting now.')
      process.exit()
    } else {
      log('Waiting for pending tasks to be completed. Pending count:', state.pendingTasks)
    }
  }

  function continueListening (queueName, handler) {
    const promiseMode = (handler.length < 2)

    if (state.killSignalReceived) return attemptCleanShutdown()

    blockingClient && blockingClient.blpop(queueName, redisBlpopTimeout, function (err, data) {
      if (err) return log(err)

      if (data) {
        const bareQueueName = queueName.slice((QUEUE_PREFIX).length)
        nonBlockingClient.hset('cuminmeta.' + bareQueueName, 'lastDequeued', Date.now())
        nonBlockingClient.publish('cumin.dequeued', data[1])

        const queueItem = JSON.parse(data[1])

        const handlerOnComplete = function () {
          state.pendingTasks--

          nonBlockingClient.hset('cuminmeta.' + bareQueueName, 'completed', Date.now())
          nonBlockingClient.publish('cumin.processed', data[1])

          if (state.killSignalReceived && state.pendingTasks) {
            log('Waiting for pending tasks to be completed. Pending count:', state.pendingTasks)
          }

          if (state.killSignalReceived && !state.pendingTasks) {
            log('Pending tasks completed. Shutting down now.')
            process.exit()
          }
        }

        state.pendingTasks++

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

      const now = Date.now()
      const message = JSON.stringify({
        byPid: process.pid,
        byTitle: process.title,
        queueName: queueName,
        date: now,
        data: queueData,
        retryCount: 0
      })

      nonBlockingClient.sadd('cuminqueues', queueName)
      nonBlockingClient.hset('cuminmeta.' + queueName, 'lastEnqueued', now)
      nonBlockingClient.rpush(QUEUE_PREFIX + queueName, message)
      nonBlockingClient.publish('cumin.enqueued', message, done)
    }),

    listen: function (queueName, handler) {
      if (!queueName) {
        throw new Error(CONSOLE_PREFIX, "Queue name must be provided. eg. 'emailQueue'.")
      }

      if (!handler) {
        throw new Error(CONSOLE_PREFIX, 'You must provide a hander to .listen.')
      }

      if (state.alreadyListening) {
        throw new Error(CONSOLE_PREFIX, 'You can only .listen once in an app. To listen to another queue, create another app.')
      }

      if (!blockingClient) {
        blockingClient = redis.createClient.apply(redis, redisArgs)
      }

      process.on('SIGINT', onKillSignal)
      process.on('SIGTERM', onKillSignal)
      state.alreadyListening = true

      if (handler.length < 2) {
        log('Assuming that the handler returns a promise.')
      }

      continueListening(QUEUE_PREFIX + queueName, handler)
    }
  }
}
