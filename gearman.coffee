###

based on protocol doc: http://gearman.org/index.php?id=protocol

###

'use strict'

binary = require 'binary'
put = require 'put'
net = require 'net'
EventEmitter = require('events').EventEmitter
utillib = require 'util'

nb = new Buffer [0] # null buffer

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

# client events emitted:
#	JOB_CREATED - server successfully rcv'd the job and queued it to be run by a worker
#	STATUS_RES - sent in response to a getJobStatus to determine status of background jobs 
#	WORK_STATUS, WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION, WORK_DATA, WORK_WARNING
#	OPTION_RES

# worker events emitted:
#	NO_JOB
#	JOB_ASSIGN
#	JOB_ASSIGN_UNIQ


class Gearman
	constructor: (@host='127.0.0.1', @port=4730) ->
		@_worker_id = null
		
		# TODO: figure out what this is in the submit job packet. I didnt see it specified in the gearman spec
		#unique_id = 'temp-unique-id'

	utillib.inherits @, EventEmitter

	# close the socket connection and cleanup
	close: ->
		if @_conn
			@_conn.end()

	# public gearman client/worker functions
	# send an echo packet. Server will respond with ECHO_RES packet. mostly debug
	echo: ->
		# let's try sending an ECHO packet
		encoding = 'utf-8'
		echo = @_encodePacket packet_types.ECHO_REQ, 'Hello World!', encoding
		@_send echo, encoding
		

	# public gearman client functions

	getJobStatus: (handle) ->
		@_sendPacketS packet_types.GET_STATUS, handle

	# set options on the job server. For now gearman only supports one option 'exceptions'
	# setting this will forward WORK_EXCEPTION packets to the client
	setOption: (optionName='exceptions') ->
		if optionName isnt 'exceptions'
			throw new Error 'unsupported option'
		@_sendPacketS packet_types.OPTION_REQ, optionName

	# submit a job to the gearman server
	# @param options supported options are: id, bg, payload
	submitJob: (func_name, data=null, options={}) ->
		if typeof(func_name) isnt 'string'
			throw new Error 'function name must be a string'

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
			throw new Error 'invalid background or priority setting'

		unique_id = '' # TODO: what is this used for
		payload = put().
		put(new Buffer(func_name, 'ascii')).
		word8(0).
		put(new Buffer(unique_id, 'ascii')).
		word8(0).
		put(data).
		buffer()

		job = @_encodePacket packet_type, payload, options.encoding
		@_send job, options.encoding

	# public gearman worker functions

	# tell the server that this worker is capable of doing work
	# @param string func_name name of the function that is supported
	# @param int timeout (optional) specify a max time this function may run on a 
	# 		 worker. if not done within timeout, server notifies client of timeout
	# 		 0 means no timeout
	addFunction: (func_name, timeout=0) ->
		if typeof(func_name) isnt 'string'
			throw new Error 'function name must be a string'
		if typeof(timeout) isnt 'number'
			throw new Error 'timout must be an int'
		if timeout < 0
			throw new Error 'timeout must be greater than zero'

		if timeout == 0
			job = @_encodePacket packet_types.CAN_DO, func_name, encoding
		else
			encoding = null
			payload = put().
			put(new Buffer(func_name, 'ascii')).
			word8(0).
			word32be(timeout).
			buffer()
			job = @_encodePacket packet_types.CAN_DO_TIMEOUT, payload
		@_send job, encoding

	# tell a server that the worker is no longer capable of handling a function
	removeFunction: (func_name) ->
		@_sendPacketS packet_types.CANT_DO, func_name

	# notify the server it can't handle any of the functions previously declared
	# with addFunction
	resetAbilities: ->
		job = @_encodePacket packet_types.RESET_ABILITIES, '', 'ascii'
		@_send job, 'ascii'

	# TODO: maybe this is private, only called internally?
	# notify the server that this worker is going to sleep. wake up with a NOOP 
	# when new work is ready
	preSleep: ->
		job = @_encodePacket packet_types.PRE_SLEEP, '', 'ascii'
		@_send job, 'ascii'

	# tell the server we want a new job
	grabJob: ->
		job = @_encodePacket packet_types.GRAB_JOB
		@_send job, 'ascii'

	# same as grabJob, but grabs jobs with unique ids assigned to them
	grabUniqueJob: ->
		job = @_encodePacket packet_types.GRAB_JOB_UNIQ, '', 'ascii'
		@_send job, 'ascii'

	sendWorkStatus: (job_handle, percent_numerator, percent_denominator) ->
		payload = put().
			put(new Buffer(job_handle, 'ascii')).
			word8(0).
			put(new Buffer(percent_numerator, 'ascii')).
			word8(0).
			put(new Buffer(percent_denominator, 'ascii')).
			buffer()
		job = @_encodePacket packet_types.WORK_STATUS, payload
		@_send job

	sendWorkFail: (job_handle) ->
		@_sendPacketS packet_types.WORK_FAIL, job_handle

	sendWorkComplete: (job_handle, data) ->
		@_sendPacketSB packet_types.WORK_COMPLETE, job_handle, data

	sendWorkData: (job_handle, data) ->
		@_sendPacketSB packet_types.WORK_DATA, job_handle, data

	sendWorkException: (job_handle, exception) ->
		@_sendPacketSB packet_types.WORK_EXCEPTION, job_handle, exception

	sendWorkWarning: (job_handle, warning) ->
		@_sendPacketSB packet_types.WORK_WARNING, job_handle, warning

	# sets the worker ID in a job server so monitoring and reporting commands can 
	# uniquely identify the various workers, and different connections to job 
	# servers from the same worker.
	setWorkerId: (id) ->
		@_sendPacketS packet_types.SET_CLENT_ID, id
		@_worker_id = id


	# private methods
	_connect: ->
		# connect socket to server
		@_conn = net.createConnection @port, @host
		@_conn.setKeepAlive true

		@_conn.on 'data', (chunk) =>
			# decode the data and execute the proper response handler
			data = @_decodePacket chunk
			@_handlePacket data
			
		@_conn.on 'error', (error) ->
			console.log 'error', error

		@_conn.on 'close', ->
			console.log 'socket closed'


	# decode and encode augmented from https://github.com/cramerdev/gearman-node/blob/master/lib/packet.js
	# converts binary buffer packet to object
	_decodePacket: (buf) ->
		if !Buffer.isBuffer buf
			throw new Error 'argument must be a buffer'

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

		# verify magic code in header matches request or response expected value
		if (o.reqType isnt binary.parse(res_magic).word32bu('reqType').vars.reqType) and 
		(o.reqType isnt binary.parse(req_magic).word32bu('reqType').vars.reqType)
			throw new Error 'invalid request header'
		# check type
		if o.type < 1 or o.type > 36
			throw new Error 'invalid packet type'
		# check size
		if o.size != o.inputData.length
			throw new Error 'invalid packet size, mismatches data length'
		o

	# construct a gearman binary packet
	_encodePacket: (type, data=null, encoding=null) ->
		if !data?
			data = new Buffer 0

		len = data.length
		if !Buffer.isBuffer data
			data = new Buffer data, encoding
		
		# check type
		if type < 1 or type > 36
			throw new Error 'invalid packet type'

		# encode the packet
		put().
		word8(0).
		put(req).
		word32be(type).
		word32be(len).
		put(data).
		buffer()

	# handle all packets received over the socket
	_handlePacket: (packet) ->
		size = 0
		console.log 'got a packet', packet

		# client packets
		if packet.type is packet_types.JOB_CREATED
			job_handle = packet.inputData.toString() #parse the job handle
			@emit 'JOB_CREATED', job_handle
			return
		if packet.type is packet_types.ERROR
			error = binary.parse(packet.inputData).
			scan('code', nb).
			scan('text').
			vars
			@emit 'ERROR', error
			return
		if packet.type is packet_types.STATUS_RES
			o = binary.parse(packet.inputData).
			scan('handle', nb).
			scan('known', nb).
			scan('running', nb).
			scan('percent_done_num', nb).
			word8be('percent_done_den').
			vars
			o.handle = o.handle.toString()
			@emit 'STATUS_RES', o
			return
		if packet.type is packet_types.WORK_COMPLETE
			result = binary.parse(packet.inputData).
			scan('handle', nb).
			tap( (vars) -> size = packet.inputData.length - (vars.handle.length + 1) ).
			buffer('payload', size).
			vars
			@emit 'WORK_COMPLETE', result
			return
		if packet.type is packet_types.WORK_DATA
			result = binary.parse(packet.inputData).
			scan('handle', nb).
			scan('payload').
			vars
			@emit 'WORK_DATA', result
			return
		if packet.type is packet_types.WORK_EXCEPTION
			result = binary.parse(packet.inputData).
			scan('handle', nb).
			scan('exception').
			vars
			@emit 'WORK_EXCEPTION', result
			return
		if packet.type is packet_types.WORK_WARNING
			result = binary.parse(packet.inputData).
			scan('handle', nb).
			scan('warning').
			vars
			@emit 'WORK_WARNING', result
			return
		if packet.type is packet_types.WORK_STATUS
			result = binary.parse(packet.inputData).
			scan('handle', nb).
			scan('percent_numerator', nb).
			scan('percent_denominator').
			vars
			@emit 'WORK_STATUS', result
			return
		if packet.type is packet_types.WORK_FAIL
			result = binary.parse(packet.inputData).
			scan('handle').vars
			@emit 'WORK_FAIL', result
			return
		if packet.type is packet_types.OPTION_RES
			result = binary.parse(packet.inputData).
			scan('option_name').vars
			@emit 'OPTION_RES', result
			return

		# worker packets
		if packet.type is packet_types.NO_JOB
			@emit 'NO_JOB'
			return
		if packet.type is packet_types.JOB_ASSIGN
			result = binary.parse(packet.inputData).
			scan('handle', nb).
			scan('func_name', nb).
			tap( (vars) -> size = packet.inputData.length - (vars.handle.length + vars.func_name.length + 2) ).
			buffer('payload', size).
			vars
			result.func_name = result.func_name.toString 'utf-8'
			@emit 'JOB_ASSIGN', result
			return
		if packet.type is packet_types.JOB_ASSIGN_UNIQ
			result = binary.parse(packet.inputData).
			scan('handle', nb).
			scan('func_name', nb).
			scan('unique_id', nb).
			scan('payload').
			vars
			@emit 'JOB_ASSIGN_UNIQ', result
			return
		# TODO: handle these packet types: NOOP, ECHO_RES
		#console.log 'rcvd packet', data , data.inputData.toString('utf-8')
		
	# common socket I/O
	_send: (data, encoding=null) ->
		if !@_conn
			@_connect()
		@_conn.write data, encoding

	# common send function. send a packet with 1 string
	_sendPacketS: (packet_type, str) ->
		if typeof(str) isnt 'string'
			throw new Error 'parameter 1 must be a string'
		payload = put().
			put(new Buffer(str, 'ascii')).
			buffer()
		job = @_encodePacket packet_type, payload
		@_send job

	# common send function. send a packet with 1 string followed by 1 Buffer
	_sendPacketSB: (packet_type, str, buf) ->
		if !Buffer.isBuffer(buf)
			buf = new Buffer(buf)
		payload = put().
			put(new Buffer(str, 'ascii')).
			word8(0).
			put(buf).
			buffer()
		job = @_encodePacket packet_type, payload
		@_send job

module.exports.Gearman = Gearman
module.exports.packet_types = packet_types
