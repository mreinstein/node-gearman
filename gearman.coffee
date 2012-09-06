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

# TODO: consider using this dense format to specify packet formats. trades 
# 		clarity for brevity 
#packet_formats =
#	CAN_DO 			   : [ 'a', ['buh'] ]

# common client and worker events emitted:
#	ECHO_RES - sent in response to ECHO_REQ. mostly used for debug

# client events emitted:
#	JOB_CREATED - server rcv'd the job and queued it to be run by a worker
#	STATUS_RES - sent in response to a getJobStatus to determine status of background jobs 
#	WORK_STATUS, WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION, WORK_DATA, WORK_WARNING
#	OPTION_RES

# worker events emitted:
#	NO_JOB - server has no available jobs 
#	JOB_ASSIGN - server assigned a job to the worker
#	JOB_ASSIGN_UNIQ - same as JOB_ASSIGN_UNIQ
#	NOOP - the server has available jobs


class Gearman
	constructor: (@host='127.0.0.1', @port=4730) ->
		@_worker_id = null

		@_connected = false

		@_conn = new net.Socket()
		
		@_conn.on 'data', (chunk) =>
			console.log 'got data packet', chunk
			
			# decode the data and execute the proper response handler
			@_handlePacket @_decodePacket(chunk)
			
		@_conn.on 'error', (error) ->
			console.log 'error', error

		@_conn.on 'close', (had_transmission_error) ->
			console.log 'socket closed'

		@_conn.on 'timeout', () ->
			console.log 'socket timed out'

		# TODO: figure out what this is in the submit job packet. I didnt see it specified in the gearman spec
		#unique_id = 'temp-unique-id'

	utillib.inherits @, EventEmitter

	# close the socket connection and cleanup
	close: ->
		if @_connected
			@_conn.end()

	connect: (callback) ->
		@_connected = true
		# connect socket to server
		@_conn.connect @port, @host, () =>
			# connection established
			#@_conn.setKeepAlive true
			callback()

	# public gearman client/worker functions
	# send an echo packet. Server will respond with ECHO_RES packet. mostly debug
	echo: (payload) ->
		# let's try sending an ECHO packet
		echo = @_encodePacket packet_types.ECHO_REQ, payload
		@_send echo
		

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
	# @param int timeout (optional) specify a max time this function may run on
	#		 a worker. if not done within timeout, server notifies client of 
	#		 timeout 0 means no timeout
	addFunction: (func_name, timeout=0) ->
		if typeof(func_name) isnt 'string'
			throw new Error 'function name must be a string'
		if typeof(timeout) isnt 'number'
			throw new Error 'timout must be an int'
		if timeout < 0
			throw new Error 'timeout must be greater than zero'

		if timeout == 0
			job = @_encodePacket packet_types.CAN_DO, func_name
		else
			payload = put().
			put(new Buffer(func_name, 'utf-8')).
			word8(0).
			word32be(timeout).
			buffer()
			job = @_encodePacket packet_types.CAN_DO_TIMEOUT, payload
		@_send job

	# tell a server that the worker is no longer capable of handling a function
	removeFunction: (func_name) ->
		@_sendPacketS packet_types.CANT_DO, func_name

	# notify the server it can't handle any of the functions previously declared
	# with addFunction
	resetAbilities: ->
		job = @_encodePacket packet_types.RESET_ABILITIES, '', 'ascii'
		@_send job, 'ascii'

	# notify the server that this worker is going to sleep. Server will send a  
	# NOOP packet when new work is ready
	preSleep: ->
		job = @_encodePacket packet_types.PRE_SLEEP
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

		delete o.reqType    # remove magic header, unused
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
		#console.log 'packet type:', packet.type
		# client and worker packets
		if packet.type is packet_types.ECHO_RES
			result = @_parsePacket packet.inputData, 'B'
			@emit 'ECHO_RES', result[0]
			return

		# client packets
		if packet.type is packet_types.JOB_CREATED
			job_handle = packet.inputData.toString() #parse the job handle
			@emit 'JOB_CREATED', job_handle
			return
		if packet.type is packet_types.ERROR
			result = @_parsePacket packet.inputData, 'ss' # code, text
			@emit 'ERROR', result[0], result[1]
			return
		if packet.type is packet_types.STATUS_RES
			result = @_parsePacket packet.inputData, 'ssss8'
			result = { handle : result[0], known: result[1], running: result[2], percent_done_num: result[3], percent_done_den: result[4] }
			@emit 'STATUS_RES', o
			return
		if packet.type is packet_types.WORK_COMPLETE
			result = @_parsePacket packet.inputData, 'sb'
			@emit 'WORK_COMPLETE', { handle : result[0], payload: result[1] }
			return
		if packet.type is packet_types.WORK_DATA
			result = @_parsePacket packet.inputData, 'sb'
			@emit 'WORK_DATA', { handle : result[0], payload: result[1] }
			return
		if packet.type is packet_types.WORK_EXCEPTION
			result = @_parsePacket packet.inputData, 'ss'
			@emit 'WORK_EXCEPTION', { handle : result[0], exception: result[1] }
			return
		if packet.type is packet_types.WORK_WARNING
			result = @_parsePacket packet.inputData, 'ss'
			@emit 'WORK_WARNING', { handle : result[0], warning: result[1] }
			return
		if packet.type is packet_types.WORK_STATUS
			result = @_parsePacket packet.inputData, 'sss'
			@emit 'WORK_STATUS', { handle : result[0], percent_num: result[1], percent_den: result[2] }
			return
		if packet.type is packet_types.WORK_FAIL
			result = @_parsePacket packet.inputData, 's'
			@emit 'WORK_FAIL', { handle: result[0] }
			return
		if packet.type is packet_types.OPTION_RES
			result = @_parsePacket packet.inputData, 's'
			@emit 'OPTION_RES', { option_name: result[0] }
			return

		# worker packets
		if packet.type is packet_types.NO_JOB
			@emit 'NO_JOB'
			return
		if packet.type is packet_types.JOB_ASSIGN
			result = @_parsePacket packet.inputData, 'ssB'
			@emit 'JOB_ASSIGN', { handle : result[0], func_name: result[1], payload: result[2] }
			return
		if packet.type is packet_types.JOB_ASSIGN_UNIQ
			p = @_parsePacket packet.inputData, 'sssB'
			@emit 'JOB_ASSIGN_UNIQ', { handle : p[0], func_name: p[1], unique_id: p[2], payload: p[3] }
			return
		if packet.type is packet_types.NOOP
			@emit 'NOOP'
		
	# parse a buffer based on a format string
	_parsePacket: (packet, format_string) ->
		format_string = format_string.toUpperCase()
		b = binary.parse packet
		len = 0
		i = 0
		while i < (format_string.length-1)
			key = '' + i
			c = format_string.charAt(i)
			if c is 'S'
				b.scan key, nb
				len += (b.vars[key].length + 1)
				b.vars[key] = b.vars[key].toString()
			if c is 'B'
				b.scan key, nb
				len += (b.vars[key].length + 1)
			if c is '8'
				b.word8be key
				len++
			i++
		if format_string.length > 0
			i = format_string.length - 1
			c = format_string.charAt i
			if c is '8'
				b.word8be(''+ i)
			else
				b.buffer ''+ i, packet.length - len
				if c is 'S'
					b.vars[''+ i]= b.vars[''+ i].toString()
		b.vars

	# common socket I/O
	_send: (data, encoding=null) ->
		if !@_connected
			throw new Error 'Cannot send packets before connecting. Please connect first.'
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
