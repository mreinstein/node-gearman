Gearman Client for nodejs 
========

Why Another Nodejs Gearman Client?
--------
I evaluated several existing libraries on github, but they either lack features, or stability, or recent updates. Features:

* full implementation of worker and client
* lean abstraction over raw gearman protocol
* lots of unit tests
* fast
* small


Install
--------
```
npm install https://github.com/mreinstein/node-gearman.git
```

Test
--------
```
npm test
```


Cookbook
========

Here are some usage patterns:
Basic Client: create a job and determine if it's been completed
--------
```coffeescript
client = new Gearman()  # assumes localhost, port 4730

# listen for finished jobs to be finished
client.on 'WORK_COMPLETE', (job) ->
	console.log 'job completed, result:', job.payload

# submit a job to reverse a string with normal priority in the foreground
client.submitJob 'upper', 'Hello, World!'
```

Basic Worker: create a worker, register a function, and handle work
--------
```coffeescript
worker = new Gearman()

# listen for the server to assign jobs
worker.on 'JOB_ASSIGN', (job) ->
	# is this a reverse job?
	if job.func_name is 'upper'
		result = job.payload.toString('utf-8').toUpperCase()
		# notify the server the job is done
		worker.sendWorkComplete job.handle, result

# register the functions this worker is capable of
worker.addFunction 'upper'

# get a job to work on
worker.grabJob()
```

