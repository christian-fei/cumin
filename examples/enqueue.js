// Call this as 'node listen.js <queue-name>'

var cumin = require('../')()

setInterval(function () {
  console.log('-- enqueue')
  cumin.enqueue(process.argv[2] || 'populate-cache', {some: 'data'})
}, 250)
