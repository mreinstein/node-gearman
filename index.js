'use strict'

// based on protocol doc: http://gearman.org/index.php?id=protocol
const binary        = require('binary')
const debug         = require('debug')('gearman')
const events        = require('events')
const packetFactory = require('./lib/packet-factory')
const packetTypes   = require('./lib/packet-types')
const put           = require('put')
const net           = require('net')


let nb = Buffer.from([0]) // null buffer
let req = Buffer.from('REQ', 'ascii')
const req_magic = Buffer.from([0x00, 0x52, 0x45, 0x51]) // \0REQ
const res_magic = Buffer.from([0, 0x52, 0x45, 0x53])    // \0RES

// common client and worker events emitted:
// ECHO_RES - sent in response to ECHO_REQ. mostly used for debug

// client events emitted:
//  JOB_CREATED - server rcv'd the job and queued it to be run by a worker
//  STATUS_RES - sent in response to a getJobStatus to determine status of background jobs
//  WORK_STATUS, WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION, WORK_DATA, WORK_WARNING
//  OPTION_RES

// worker events emitted:
//  NO_JOB - server has no available jobs
//  JOB_ASSIGN - server assigned a job to the worker
//  JOB_ASSIGN_UNIQ - same as JOB_ASSIGN_UNIQ
//  NOOP - the server has available jobs
module.exports = function gearman(host='127.0.0.1', port=4730, options={}) {
  let _worker_id = null
  let _connected = false
  let _emitter = new events.EventEmitter()
  let _conn = new net.Socket()
  let _packetFactory = packetFactory()

  if (options && options.timeout != null) {
    _conn.setTimeout(options.timeout)
  }

  _conn.on('data', function (chunk) {
    // add the stream of incoming bytes to the packet factory
    let packets = _packetFactory.addBytes(chunk)

    // factory produces fully formed Gearman packets
    packets.forEach(function(packet){
      //console.log 'decoding packet ', packet
      // decode the data and execute the proper response handler
      _handlePacket(_decodePacket(packet))
    })
  })

  _conn.on('error', function (error) {
    debug("error", error)
    _emitter.emit("error", error)
  })

  _conn.on('close', function (had_transmission_error) {
    debug("close", had_transmission_error)
    _emitter.emit("close", had_transmission_error)
  })

  _conn.on('timeout', function() {
    debug("timeout")
    _emitter.emit("timeout")
  })

  // close the socket connection and cleanup
  const close = function() {
    if (_connected) _conn.end()
  }

  const connect = function (callback) {
    _connected = true
    // connect socket to server
    _conn.connect(port, host, function() {
      // connection established
      //_conn.setKeepAlive(true)
      callback()
    })
  }

  const on = function(event, listener) {
    _emitter.on(event, listener)
  }

  const removeEventListener = function(event, listener) {
    _emitter.removeListener(event, listener)
  }

  // public gearman client/worker functions

  // send an echo packet. Server will respond with ECHO_RES packet. mostly debug
  const echo = function (payload) {
    _send(_encodePacket(packetTypes.ECHO_REQ, payload))
  }


  // public gearman client functions

  const getJobStatus = function (handle) {
    _sendPacketS(packetTypes.GET_STATUS, handle)
  }

  // public gearman client functions

  const getJobStatusUnique = function (uniqueId) {
    _sendPacketS(packetTypes.GET_STATUS_UNIQUE, uniqueId)
  }

  // set options on the job server. For now gearman only supports one option 'exceptions'
  // setting this will forward WORK_EXCEPTION packets to the client
  const setOption = function (optionName='exceptions') {
    if (optionName !== 'exceptions')
      throw new Error('unsupported option')
    _sendPacketS(packetTypes.OPTION_REQ, optionName)
  }

  // submit a job to the gearman server
  // @param options supported options are: id, bg, payload
  // @param callback Function to call after the job has been submitted
  const submitJob = function(func_name, data=null, options={}, callback=null) {
    if (typeof(func_name) !== 'string')
      throw new Error('function name must be a string')

    // determine which packet type to build
    const lookup_table = {
      lowtrue: packetTypes.SUBMIT_JOB_LOW_BG,
      lowfalse: packetTypes.SUBMIT_JOB_LOW,
      normaltrue: packetTypes.SUBMIT_JOB_BG,
      normalfalse: packetTypes.SUBMIT_JOB,
      hightrue: packetTypes.SUBMIT_JOB_HIGH_BG,
      highfalse: packetTypes.SUBMIT_JOB_HIGH
    }

    // gearmand maintains a jobs hash, with the key being unique_id. setting
    // this allows clients to ensure that only 1 job is present for a given
    // id at any moment. This can be used to reduce the stampeding herd
    // problem, where several clients would try to submit an identical job
    // into gearman. By setting a unique id for the job, only 1 job instance
    // will be present in gearmand at any time.
    if (!options.unique_id)
      // no unique_id is set, assume this job is unique and generate a key
      options.unique_id = _uniqueId()
    if (!options.background)
      options.background = false
    if (!options.priority)
      options.priority = 'normal'
    if (!options.encoding)
      options.encoding = null
    if (!data)
      data = Buffer.from([])
    if (!Buffer.isBuffer(data))
      data = Buffer.from(data, options.encoding)

    options.background += '' // convert boolean to string
    let sig = options.priority.trim().toLowerCase() + options.background.trim().toLowerCase()
    let packet_type = lookup_table[sig]
    if (!packet_type)
      throw new Error('invalid background or priority setting')

    let payload = put().
      put(Buffer.from(func_name, 'ascii')).
      word8(0).
      put(Buffer.from(options.unique_id, 'ascii')).
      word8(0).
      put(data).
      buffer()

    let job = _encodePacket(packet_type, payload, options.encoding)
    _send(job, options.encoding, callback)
  }

  // public gearman worker functions

  // tell the server that this worker is capable of doing work
  // @param string func_name name of the function that is supported
  // @param int timeout (optional) specify a max time this function may run on
  //    a worker. if not done within timeout, server notifies client of
  //    timeout 0 means no timeout
  const addFunction = function (func_name, timeout=0) {
    if (typeof(func_name) !== 'string')
      throw new Error('function name must be a string')
    if (typeof(timeout) !== 'number')
      throw new Error('timout must be an int')
    if (timeout < 0) {
      throw new Error('timeout must be greater than zero')
    }

    if (timeout === 0) {
      _send(_encodePacket(packetTypes.CAN_DO, func_name))
    } else {
      let payload = put().put(Buffer.from(func_name, 'utf-8')).word8(0).word32be(timeout).buffer()
      _send(_encodePacket(packetTypes.CAN_DO_TIMEOUT, payload))
    }
  }

  // tell a server that the worker is no longer capable of handling a function
  const removeFunction = function (func_name) {
    _sendPacketS(packetTypes.CANT_DO, func_name)
  }

  // notify the server it can't handle any of the functions previously declared
  // with addFunction
  const resetAbilities = function() {
    _send(_encodePacket(packetTypes.RESET_ABILITIES, '', 'ascii'), 'ascii')
  }

  // notify the server that this worker is going to sleep. Server will send a
  // NOOP packet when new work is ready
  const preSleep = function() {
    _send(_encodePacket(packetTypes.PRE_SLEEP), 'ascii')
  }

  // tell the server we want a new job
  const grabJob = function() {
    _send(_encodePacket(packetTypes.GRAB_JOB), 'ascii')
  }

  // same as grabJob, but grabs jobs with unique ids assigned to them
  const grabUniqueJob = function() {
    _send(_encodePacket(packetTypes.GRAB_JOB_UNIQ), 'ascii')
  }

  const sendWorkStatus = function (job_handle, percent_numerator, percent_denominator) {
    let payload = put().
      put(Buffer.from(job_handle, 'ascii')).
      word8(0).
      put(Buffer.from(percent_numerator, 'ascii')).
      word8(0).
      put(Buffer.from(percent_denominator, 'ascii')).
      buffer()
    _send(_encodePacket(packetTypes.WORK_STATUS, payload))
  }

  const sendWorkFail = function (job_handle) {
    _sendPacketS(packetTypes.WORK_FAIL, job_handle)
  }

  const sendWorkComplete = function (job_handle, data) {
    _sendPacketSB(packetTypes.WORK_COMPLETE, job_handle, data)
  }

  const sendWorkData = function(job_handle, data) {
    _sendPacketSB(packetTypes.WORK_DATA, job_handle, data)
  }

  const sendWorkException = function(job_handle, exception) {
    _sendPacketSB(packetTypes.WORK_EXCEPTION, job_handle, exception)
  }

  const sendWorkWarning = function (job_handle, warning) {
    _sendPacketSB(packetTypes.WORK_WARNING, job_handle, warning)
  }

  // sets the worker ID in a job server so monitoring and reporting commands can
  // uniquely identify the various workers, and different connections to job
  // servers from the same worker.
  const setWorkerId = function (id) {
    _sendPacketS(packetTypes.SET_CLIENT_ID, id)
    _worker_id = id
  }

  // public methods for Administrative protocol
  const adminStatus = function (callback) {
    let conn = new net.Socket()

    conn.on('data', function(chunk) {
      // parse response as  FUNCTION\tTOTAL\tRUNNING\tAVAILABLE_WORKERS
      let result = {}
      let lines = chunk.toString('ascii').split('\n')
      lines.forEach(function(line){
        if (line.length) {
          line = line.split('\t')
          result[line[0]] = { total: parseInt(line[1]), running: parseInt(line[2]), available_workers: parseInt(line[3]) }
        }
      })
      conn.destroy()
      callback(result)
    })

    conn.connect(port, host, function() {
      // connection established
      let b = Buffer.from('status\n', 'ascii')
      conn.write(b, 'ascii')
    })
  }


  const adminWorkers = function (callback) {
    let conn = new net.Socket()
    conn.on('data', function(chunk) {
      // parse response as  FD IP-ADDRESS CLIENT-ID : FUNCTION ...
      let result = []
      let lines = chunk.toString('ascii').split('\n')
      lines.forEach(function(line){
        if (line.length > 1) {
          line = line.split(' ')
          let o = {
            fd: parseInt(line.shift()),
            ip: line.shift(),
            id: line.shift()
          }
          line.shift() // skip the : character
          o.functions = line
          result.push(o)
        }
      })

      conn.destroy()
      callback(result)
    })

    conn.connect(port, host, function() {
      // connection established
      let b = Buffer.from('workers\n', 'ascii')
      conn.write(b, 'ascii')
    })
  }


  const adminDropFunction = function (name, callback) {
    let conn = new net.Socket()
    conn.on('data', function(chunk) {
      let result = new Error('unknown')
      let line = chunk.toString('ascii').split('\n')[0]
      if (/^OK/.test(line)) {
        result = null
      } else if (/^ERR/.test(line)) {
        result = new Error(line.substring(4))
      } else {
        result = new Error(line)
      }

      conn.destroy()
      callback(result)
    })

    conn.connect(port, host, function() {
      // connection established
      let b = Buffer.from('drop function ' + name + '\n', 'ascii')
      conn.write(b, 'ascii')
    })
  }


  // decode and encode augmented from https://github.com/cramerdev/gearman-node/blob/master/lib/packet.js
  // converts binary buffer packet to object
  const _decodePacket = function (buf) {
    if (!Buffer.isBuffer(buf))
      throw new Error('argument must be a buffer')
    let size = 0
    let o = binary.parse(buf).
      word32be('reqType').
      word32be('type').
      word32be('size').
      tap(
        function(vars) { size = vars.size }
      ).
      buffer('inputData', size).
      vars

    // verify magic code in header matches request or response expected value
    if ((o.reqType !== binary.parse(res_magic).word32bu('reqType').vars.reqType) &&
    (o.reqType !== binary.parse(req_magic).word32bu('reqType').vars.reqType))
      throw new Error('invalid request header')
    // check type
    if (o.type < 1 || o.type > 42)
      throw new Error('invalid packet type')
    // check size
    if (o.size !== o.inputData.length)
      throw new Error('invalid packet size, mismatches data length')

    delete o.reqType    // remove magic header, unused
    return o
  }

  // construct a gearman binary packet
  const _encodePacket = function(type, data=null, encoding='utf-8') {
    if (typeof data === "undefined" || data === null) {
      data = Buffer.alloc(0)
    }

    if (!Buffer.isBuffer(data)) {
      data = Buffer.from(data, encoding)
    }

    if (type < 1 || type > 42) {
      throw new Error('invalid packet type')
    }

    return put().
      word8(0).
      put(req).
      word32be(type).
      word32be(data.length).
      put(data).
      buffer()
  }

  // private methods

  // handle a packet received over the socket
  const _handlePacket = function (packet) {
    //console.log 'packet type:', packet.type
    // client and worker packets
    if (packet.type === packetTypes.ECHO_RES) {
      let result = _parsePacket(packet.inputData, 'B')
      _emitter.emit('ECHO_RES', result[0])
      return
    }

    // client packets
    if (packet.type === packetTypes.JOB_CREATED) {
      _emitter.emit('JOB_CREATED', packet.inputData.toString())
      return
    }

    if (packet.type === packetTypes.ERROR) {
      let result = _parsePacket(packet.inputData, 'ss')
      _emitter.emit('ERROR', result[0], result[1])
      return
    }

    if (packet.type === packetTypes.STATUS_RES) {
      let result = _parsePacket(packet.inputData, 'ssss8')
      result = {
        handle: result[0],
        known: result[1],
        running: result[2],
        percent_done_num: result[3],
        percent_done_den: result[4]
      }
      _emitter.emit('STATUS_RES', result)
      return
    }

    if (packet.type === packetTypes.STATUS_RES_UNIQUE) {
      let result = _parsePacket(packet.inputData, 'ssss8')
      result = {
        unique_id: result[0],
        known: result[1],
        running: result[2],
        percent_done_num: result[3],
        percent_done_den: result[4]
      }
      _emitter.emit('STATUS_RES_UNIQUE', result)
      return
    }

    if (packet.type === packetTypes.WORK_COMPLETE) {
      let result = _parsePacket(packet.inputData, 'sb')
      _emitter.emit('WORK_COMPLETE', {
        handle: result[0],
        payload: result[1]
      })
      return
    }

    if (packet.type === packetTypes.WORK_DATA) {
      let result = _parsePacket(packet.inputData, 'sb')
      _emitter.emit('WORK_DATA', {
        handle: result[0],
        payload: result[1]
      })
      return
    }

    if (packet.type === packetTypes.WORK_EXCEPTION) {
      let result = _parsePacket(packet.inputData, 'ss')
      _emitter.emit('WORK_EXCEPTION', {
        handle: result[0],
        exception: result[1]
      })
      return
    }

    if (packet.type === packetTypes.WORK_WARNING) {
      let result = _parsePacket(packet.inputData, 'ss')
      _emitter.emit('WORK_WARNING', {
        handle: result[0],
        warning: result[1]
      })
      return
    }

    if (packet.type === packetTypes.WORK_STATUS) {
      let result = _parsePacket(packet.inputData, 'sss')
      _emitter.emit('WORK_STATUS', {
        handle: result[0],
        percent_num: result[1],
        percent_den: result[2]
      })
      return
    }

    if (packet.type === packetTypes.WORK_FAIL) {
      let result = _parsePacket(packet.inputData, 's')
      _emitter.emit('WORK_FAIL', { handle: result[0] })
      return
    }

    if (packet.type === packetTypes.OPTION_RES) {
      let result = _parsePacket(packet.inputData, 's')
      _emitter.emit('OPTION_RES', { option_name: result[0] })
      return
    }

    // worker packets
    if (packet.type === packetTypes.NO_JOB) {
      _emitter.emit('NO_JOB')
      return
    }

    if (packet.type === packetTypes.JOB_ASSIGN) {
      let result = _parsePacket(packet.inputData, 'ssB')
      _emitter.emit('JOB_ASSIGN', {
        handle: result[0],
        func_name: result[1],
        payload: result[2]
      })
      return
    }

    if (packet.type === packetTypes.JOB_ASSIGN_UNIQ) {
      let p = _parsePacket(packet.inputData, 'sssB')
      _emitter.emit('JOB_ASSIGN_UNIQ', {
        handle: p[0],
        func_name: p[1],
        unique_id: p[2],
        payload: p[3]
      })
      return
    }

    if (packet.type === packetTypes.NOOP) {
      _emitter.emit('NOOP')
    }
  }

  // parse a buffer based on a format string
  const _parsePacket = function (packet, format_string) {
    format_string = format_string.toUpperCase()
    let b = binary.parse(packet)
    let len = 0
    let i = 0
    while (i < (format_string.length-1)) {
      let key = '' + i
      let c = format_string.charAt(i)
      if (c === 'S') {
        b.scan(key, nb)
        len += (b.vars[key].length + 1)
        b.vars[key] = b.vars[key].toString()
      }
      else if (c === 'B') {
        b.scan(key, nb)
        len += (b.vars[key].length + 1)
      }
      else if (c === '8') {
        b.word8be(key)
        len++
      }
      i++
    }
    if (format_string.length > 0) {
      i = format_string.length - 1
      let c = format_string.charAt(i)
      if (c === '8')
        b.word8be(''+ i)
      else {
        b.buffer(''+ i, packet.length - len)
        if (c === 'S')
          b.vars[''+ i]= b.vars[''+ i].toString()
      }
    }
    return b.vars
  }

  // common socket I/O
  const _send = function (data, encoding=null, callback=null) {
    if (!_connected)
      throw new Error('Cannot send packets before connecting. Please connect first.')
    _conn.write(data, encoding, callback)
  }

  // common send function. send a packet with 1 string
  const _sendPacketS = function (packet_type, str) {
    if (typeof(str) !== 'string')
      throw new Error('parameter 1 must be a string')
    const payload = put().
      put(Buffer.from(str, 'ascii')).
      buffer()
    _send(_encodePacket(packet_type, payload))
  }

  // common send function. send a packet with 1 string followed by 1 Buffer
  const _sendPacketSB = function (packet_type, str, buf) {
    if (!Buffer.isBuffer(buf))
      buf = Buffer.from('' + buf, 'utf8')
    const payload = put().
      put(Buffer.from(str, 'ascii')).
      word8(0).
      put(buf).
      buffer()

    _send(_encodePacket(packet_type, payload))
  }

  // utility function to generate a random alphanumeric format_string
  const _uniqueId = function (length = 12) {
    let id = ''
    while (id.length < length)
      id += Math.random().toString(36).substr(2)
    return id.substr(0, length)
  }

  const result = { close, connect, echo, getJobStatus, getJobStatusUnique,
           setOption, submitJob, addFunction, preSleep, grabJob, grabUniqueJob,
           sendWorkStatus, sendWorkFail, sendWorkComplete, sendWorkData, on,
           sendWorkException, sendWorkWarning, setWorkerId, adminStatus,
           adminWorkers, removeFunction, resetAbilities, removeEventListener,
           adminDropFunction }

  // expose some internal functions (useful for debugging)
  if (options.exposeInternals) {
    result._handlePacket = _handlePacket
    result._parsePacket = _parsePacket
    result._decodePacket = _decodePacket
    result._encodePacket = _encodePacket
  }

  return Object.freeze(result)
}
