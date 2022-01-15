# Gearman Client and Worker

[![Greenkeeper badge](https://badges.greenkeeper.io/mreinstein/node-gearman.svg)](https://greenkeeper.io/)

![tests](https://github.com/mreinstein/node-gearman/actions/workflows/main.yml/badge.svg)


### pros:

* full implementation of worker and client
* lean abstraction over raw gearman protocol
* lots of unit tests
* fast
* small
* fully interoperable with gearman clients and workers written in other languages

### cons:

* lacks elegant high level abstractions for doing work. A bit more boilerplate to write
* only supports 1 server connection per client/worker


## usage
[![NPM](https://nodei.co/npm/gearman.png)](https://nodei.co/npm/gearman/)



## examples

### create a client, create 1 job, and handle it's completion

```javascript
const gearman = require('gearman')

let client = gearman("localhost", 4730 , {timeout: 3000})  // timeout in milliseconds. 

// handle timeout 
client.on('timeout', function() {
	console.log('Timeout occurred')
	client.close()
})


// handle finished jobs
client.on('WORK_COMPLETE', function(job) {
	console.log('job completed, result:', job.payload.toString())
	client.close()
})

// connect to the gearman server
client.connect(function() {
	// submit a job to uppercase a string with normal priority in the foreground
	client.submitJob('upper', 'Hello, World!')
})
	
```


### create a worker, register a function, and handle jobs

```javascript
const gearman = require('gearman')

let worker = gearman('127.0.0.1', 4730)

// handle jobs assigned by the server
worker.on('JOB_ASSIGN', function(job) {
	console.log(job.func_name + ' job assigned to this worker')
	let result = job.payload.toString().toUpperCase()
	// notify the server the job is done
	worker.sendWorkComplete(job.handle, result)

	// go back to sleep, telling the server we're ready for more work
	worker.preSleep()
});

// grab a job when the server signals one is available
worker.on('NOOP', function() {  worker.grabJob() })

// connect to the gearman server	
worker.connect(function(){
	// register the functions this worker is capable of
	worker.addFunction('upper')

	// tell the server the worker is going to sleep, waiting for work
	worker.preSleep()
});
```
