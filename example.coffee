###

some basic usage examples for the cookbook

###

'use strict'

Gearman = require('./Gearman').Gearman


# basic client: create a job and determine if it's been completed
client = new Gearman()  # assumes localhost, port 4730

# handle finished jobs
client.on 'WORK_COMPLETE', (job) ->
	console.log 'job completed, result:', job.payload.toString('utf-8')

# submit a job to uppercase a string with normal priority in the foreground
client.submitJob 'upper', 'Hello, World!'



# basic worker: create a worker, register a function, and handle work
worker = new Gearman()

# listen for the server to assign jobs
worker.on 'JOB_ASSIGN', (job) ->
	result = job.payload.toString('utf-8').toUpperCase()
	# notify the server the job is done
	worker.sendWorkComplete job.handle, result

# if no jobs are found, ping every 10 seconds for a new one
worker.on 'NO_JOB', ->
	console.log 'no work found, sleeping'
	# no work available, wait and try again
	setTimeout(
		=> worker.grabJob()
		10000
	)

# register the functions this worker is capable of
worker.addFunction 'upper'

# get a job to work on
worker.grabJob()
