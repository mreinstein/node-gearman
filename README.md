# Gearman Client and Worker for nodejs 


##Why Another Nodejs Gearman Client?
I evaluated several existing libraries on github, but they either lack features, or stability, or recent updates. 

### PROS:

* full implementation of worker and client
* lean abstraction over raw gearman protocol
* lots of unit tests
* fast
* small
* fully interoperable with gearman clients and workers written in other languages

### CONS:

* lacks elegant high level abstractions for doing work. A bit more boilerplate to write
* only supports 1 server connection per client/worker
* documentation is lacking. This is a priority and it's being addressed


## Install
```
git clone https://github.com/mreinstein/node-gearman.git
npm install ./node-gearman
rm -rf node-gearman
```

## Test
```
npm test
```


## Cookbook

### create a client, create 1 job, and handle it's completion

```coffeescript
Gearman = require('gearman').Gearman

client = new Gearman("localhost", 4730 , {timeout: 3000}) # timeout in milliseconds. 

# handle timeout 
client.on 'timeout', () ->
	console.log 'Timeout occurred'
	client.close()


# handle finished jobs
client.on 'WORK_COMPLETE', (job) ->
	console.log 'job completed, result:', job.payload.toString()
	client.close()

# connect to the gearman server
client.connect ->
	# submit a job to uppercase a string with normal priority in the foreground
	client.submitJob 'upper', 'Hello, World!'
```


### create a worker, register a function, and handle jobs

```coffeescript
Gearman = require('gearman').Gearman

worker = new Gearman('127.0.0.1', 4730) 

# handle jobs assigned by the server
worker.on 'JOB_ASSIGN', (job) ->
	console.log job.func_name + ' job assigned to this worker'
	result = job.payload.toString().toUpperCase()
	# notify the server the job is done
	worker.sendWorkComplete job.handle, result

	# go back to sleep, telling the server we're ready for more work
	worker.preSleep()

# grab a job when the server signals one is available
worker.on 'NOOP', ->
	worker.grabJob()

# connect to the gearman server	
worker.connect ->
	# register the functions this worker is capable of
	worker.addFunction 'upper'

	# tell the server the worker is going to sleep, waiting for work
	worker.preSleep()
```

