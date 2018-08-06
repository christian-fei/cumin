// Call this as 'node listen.js <queue-name>'

var cumin = require('../')()

cumin.listen(process.argv[2] || 'populate-cache', function (data, done) {
  console.log('-- received', data)
  // Simulating a 1 second long task
  setTimeout(done, 1000)
})
