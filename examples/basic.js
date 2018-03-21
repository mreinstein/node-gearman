// basic usage example

'use strict'

const gearman = require('../')


// basic client: create a job and determine if it's been completed
const client = gearman() // assumes localhost, port 4730

// handle finished jobs
client.on('WORK_COMPLETE', function(job) {
	console.log('job completed, result:', job.payload.toString())
	client.close()
})

client.on('close', had_transmission_error => {
	console.log('client conn closed. had transmission error?', had_transmission_error)
})


// connect to the gearman server
client.connect(() =>
	// submit a job to uppercase a string with normal priority in the foreground
	client.submitJob('upper', 'Hello, World!')
)


// basic worker: create a worker, register a function, and handle work
const worker = gearman()

// handle jobs assigned by the server
worker.on('JOB_ASSIGN', function(job) {
	console.log(job.func_name + ' job assigned to this worker')
	const result = job.payload.toString().toUpperCase()
	// notify the server the job is done
	worker.sendWorkComplete(job.handle, result)

	// go back to sleep, telling the server we're ready for more work
	worker.preSleep()
})

// grab a job when the server signals one is available
worker.on('NOOP', () => worker.grabJob())

// connect to the gearman server
worker.connect(function() {
	// register the functions this worker is capable of
	worker.addFunction('upper')

	// tell the server the worker is going to sleep, waiting for work
	worker.preSleep()
})
