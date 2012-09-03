###

based on protocol doc: http://gearman.org/index.php?id=protocol

TODO: test sending/receiving large packets
TODO: tests. lots and lots of tests.

# client functions
submitJob 'weee func', 'hehehehe my data'
getJobStatus 'H:mike:1'
setOption()

# worker functions
addFunction 'wizzleWazzleyyy'
addFunction 'fantasmagoric3', 2000
removeFunction 'wizzleWazzleyyy'

resetAbilities()
preSleep()
grabJob()
grabUniqueJob()

sendWorkComplete handle, data
sendWorkData handle, data
sendWorkException handle, exception
sendWorkWarning handle, data

sendWorkStatus handle, num, denom

sendWorkFail handle
setWorkerId id

# TODO: fix CAN_DO_TIMEOUT

###

binary = require 'binary'
put = require 'put'
net = require 'net'

req = new Buffer 'REQ', 'ascii'
req_magic = new Buffer [0x00, 0x52, 0x45, 0x51]  # \0REQ
res_magic = new Buffer [0, 0x52, 0x45, 0x53]  # \0RES

packet_types =
	CAN_DO 			   : 1  # sent by worker
	CANT_DO 		   : 2  # sent by worker
	RESET_ABILITIES    : 3  # sent by worker
	PRE_SLEEP 		   : 4  # sent by worker
	#RESERVED 		   : 5  # unused
	NOOP 			   : 6  # received by worker
	SUBMIT_JOB 		   : 7  # sent by client
	JOB_CREATED 	   : 8  # received by client
	GRAB_JOB 		   : 9  # sent by worker
	NO_JOB			   : 10 # received by worker
	JOB_ASSIGN 		   : 11 # used by client/worker
	WORK_STATUS 	   : 12 # sent by worker
	WORK_COMPLETE 	   : 13 # sent by worker
	WORK_FAIL	 	   : 14 # sent by worker
	GET_STATUS 		   : 15 # sent by client
	ECHO_REQ 		   : 16
	ECHO_RES 		   : 17 # received by client/worker
	SUBMIT_JOB_BG      : 18 # sent by client
	ERROR 	 		   : 19 # received by client/worker
	STATUS_RES		   : 20 # received by client
	SUBMIT_JOB_HIGH    : 21 # sent by client
	SET_CLIENT_ID 	   : 22 # sent by worker
	CAN_DO_TIMEOUT 	   : 23 # sent by worker
	ALL_YOURS 		   : 24 # not yet implemented by server
	WORK_EXCEPTION 	   : 25 # sent by worker
	OPTION_REQ 		   : 26 # used by client/worker
	OPTION_RES 		   : 27 # used by client/worker
	WORK_DATA 		   : 28 # sent by worker
	WORK_WARNING 	   : 29 # sent by worker
	GRAB_JOB_UNIQ 	   : 30 # sent by worker
	SUBMIT_JOB_HIGH_BG : 32 # sent by client
	SUBMIT_JOB_LOW 	   : 33 # sent by client
	SUBMIT_JOB_LOW_BG  : 34 # sent by client
	SUBMIT_JOB_SCHED   : 35 # unused, may be removed in the future
	SUBMIT_JOB_EPOCH   : 36 # unused, may be removed in the future

# decode and encode augmented from https://github.com/cramerdev/gearman-node/blob/master/lib/packet.js
# converts binary buffer packet to object
decodePacket = (buf) ->
	size = 0
	o = binary.parse(buf).
		word32bu('reqType').
		word32bu('type').
		word32bu('size').
		tap(
			(vars) -> size = vars.size
		).
		buffer('inputData', size).
		vars

	# verify the magic code is either request or response
	if (o.reqType isnt binary.parse(res_magic).word32bu('reqType').vars.reqType) and (o.reqType isnt binary.parse(req_magic).word32bu('reqType').vars.reqType)
		throw Error 'invalid request header'
	# check type
	if o.type < 1 or o.type > 36
		throw Error 'invalid packet type'
	# check size
	if o.size != o.inputData.length
		throw Error 'invalid packet size, mismatches data length'
	o

# construct a gearman binary packet
encodePacket = (type, data, encoding=null) ->
	len = data.length
	if !Buffer.isBuffer data
		data = new Buffer data, encoding
	
	# check type
	if type < 1 or type > 36
		throw Error 'invalid packet type'
	# decode the packet
	put().
	word8(0).
	put(req).
	word32be(type).
	word32be(len).
	put(data).
	buffer()

# connect to socket
conn = net.createConnection 4730, '127.0.0.1'

#conn.on 'connect', _
#console.log 'sweet, connected'
conn.setKeepAlive true

conn.on 'data', (chunk) ->
	# decode the data and execute the proper response handler
	data = decodePacket chunk
	console.log 'received a packet', data , 'hmm', data.inputData.toString('utf-8')
	# TODO: handle these packet types: JOB_CREATED, ERROR, STATUS_RES, OPTION_RES, NOOP, NO_JOB, JOB_ASSIGN, JOB_ASSIGN_UNIQ

conn.on 'error', (error) ->
	console.log 'error', error
conn.on 'close', ->
	console.log 'socket closed'

# let's try sending an ECHO packet
#encoding = 'utf-8'
#echo = encodePacket packet_types.ECHO_REQ, 'Hello World!', encoding
#conn.write echo, encoding

#conn.end()

# TODO: figure out what this is in the submit job packet. I didnt see it specified in the gearman spec
unique_id = 'temp-unique-id'


# public gearman client functions

# submit a job to the gearman server
# @param options supported options are: id, bg, payload
submitJob = (func_name, data=null, options={}) ->
	if typeof(func_name) isnt 'string'
		throw Error 'function name must be a string'

	#determine which packet type to build
	lookup_table = 
		'lowtrue' : packet_types.SUBMIT_JOB_LOW_BG
		'lowfalse' : packet_types.SUBMIT_JOB_LOW
		'normaltrue' : packet_types.SUBMIT_JOB_BG
		'normalfalse' : packet_types.SUBMIT_JOB
		'hightrue' : packet_types.SUBMIT_JOB_HIGH_BG
		'highfalse' : packet_types.SUBMIT_JOB_HIGH

	if !options.background
		options.background = false
	if !options.priority
		options.priority = 'normal'
	if !options.encoding
		options.encoding = null
	if !data
		data = new Buffer()
	if !Buffer.isBuffer data
		data = new Buffer data, options.encoding

	options.background += '' # convert boolean to string
	sig = options.priority.trim().toLowerCase() + options.background.trim().toLowerCase()
	packet_type = lookup_table[sig]
	if !packet_type
		throw Error 'invalid background or priority setting'

	payload = put().
	put(new Buffer(func_name, 'ascii')).
	word8(0).
	put(new Buffer(unique_id, 'ascii')).
	word8(0).
	put(data).
	buffer()

	job = encodePacket packet_type, payload, options.encoding
	conn.write job, options.encoding

getJobStatus = (handle) ->
	_sendPacketS packet_types.GET_STATUS, handle

# set options on the job server. For now gearman only supports one option 'exceptions'
# setting this will forward WORK_EXCEPTION packets to the client
setOption = (optionName='exceptions') ->
	if optionName isnt 'exceptions'
		throw Error 'unsupported option'
	_sendPacketS packet_types.OPTION_REQ, optionName



# public gearman worker functions

# tell the server that this worker is capable of doing work
# @param string func_name name of the function that is supported
# @param int timeout (optional) specify a max time this function may run on a 
# 		 worker. if not done within timeout, server notifies client of timeout
# 		 0 means no timeout
addFunction = (func_name, timeout=0) ->
	if typeof(func_name) isnt 'string'
		throw Error 'function name must be a string'
	if typeof(timeout) isnt 'number'
		throw Error 'timout must be an int'
	if timeout < 0
		throw Error 'timeout must be greater than zero'

	if timeout == 0
		encoding = 'ascii'
		payload = put().
		put(new Buffer(func_name, encoding)).
		buffer()
		job = encodePacket packet_types.CAN_DO, payload, encoding
		
	else
		encoding = null
		payload = put().
		put(new Buffer(func_name, 'ascii')).
		word8(0).
		word32be(timeout).
		buffer()
		job = encodePacket packet_types.CAN_DO_TIMEOUT, payload
	conn.write job, encoding	

# tell a server that the worker is no longer capable of handling a function
removeFunction = (func_name) ->
	_sendPacketS packet_types.CANT_DO, func_name
	conn.write job, 'ascii'

# notify the server it can't handle any of the functions previously declared
# with addFunction
resetAbilities = ->
	job = encodePacket packet_types.RESET_ABILITIES, '', 'ascii'
	conn.write job, 'ascii'

# TODO: maybe this is private, only called internally?
# notify the server that this worker is going to sleep. wake up with a NOOP 
# when new work is ready
preSleep = ->
	job = encodePacket packet_types.PRE_SLEEP, '', 'ascii'
	conn.write job, 'ascii'

# tell the server we want a new job
grabJob = ->
	job = encodePacket packet_types.GRAB_JOB, '', 'ascii'
	conn.write job, 'ascii'

# same as grabJob, but grabs jobs with unique ids assigned to them
grabUniqueJob = ->
	job = encodePacket packet_types.GRAB_JOB_UNIQ, '', 'ascii'
	conn.write job, 'ascii'

sendWorkData = (job_handle, data) ->
	_sendPacketSB packet_types.WORK_DATA, job_handle, data

sendWorkWarning = (job_handle, data) ->
	_sendPacketSB packet_types.WORK_WARNING, job_handle, data

sendWorkStatus = (job_handle, percent_numerator, percent_denominator) ->
	payload = put().
		put(new Buffer(job_handle, 'ascii')).
		word8(0).
		put(new Buffer(percent_numerator, 'ascii')).
		word8(0).
		put(new Buffer(percent_denominator, 'ascii')).
		buffer()
	job = encodePacket packet_types.WORK_STATUS, payload
	conn.write job

sendWorkComplete = (job_handle, data) ->
	_sendPacketSB packet_types.WORK_COMPLETE, job_handle, data

sendWorkFail = (job_handle) ->
	_sendPacketS packet_types.WORK_FAIL, job_handle

sendWorkException = (job_handle, exception) ->
	_sendPacketSB packet_types.WORK_EXCEPTION, job_handle, exception

# sets the worker ID in a job server so monitoring and reporting commands can 
# uniquely identify the various workers, and different connections to job 
# servers from the same worker.
setWorkerId = (id) ->
	_sendPacketS packet_types.SET_CLENT_ID, id


# common send function. send a packet with 1 string
_sendPacketS = (packet_type, str) ->
	if typeof(str) isnt 'string'
		throw Error 'parameter 1 must be a string'

	payload = put().
		put(new Buffer(str, 'ascii')).
		buffer()
	job = encodePacket packet_type, payload
	conn.write job

# common send function. send a packet with 1 string and 1 Buffer
_sendPacketSB = (packet_type, str, buf) ->
	if !Buffer.isBuffer(buf)
		buf = new Buffer(buf)

	payload = put().
		put(new Buffer(str, 'ascii')).
		word8(0).
		put(buf).
		buffer()
	job = encodePacket packet_type, payload
	conn.write job
