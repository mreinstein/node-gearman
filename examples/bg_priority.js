// shows how to submit a background job and check it's status

'use strict'

const gearman = require('../')


// create a client
const client = gearman()  // assumes localhost, port 4730

// handle finished jobs
client.on('JOB_CREATED', handle => client.getJobStatus(handle))

// listen for server to tell us about job status
client.on('STATUS_RES', job => console.log('job status retrieved:', job))

// connect to the gearman server
client.connect( () =>
	// submit a job to uppercase a string with high priority in the background
	client.submitJob('upper', 'Hello, World!', { priority: 'high', background: true })
)
