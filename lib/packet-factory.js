'use strict'

const binary = require('binary')
const put    = require('put')


// takes in a stream of bytes as raw material, and produces Gearman packets
module.exports = function gearmanPacketFactory() {
  let _buffer = put()

  // add a series of bytes as a buffer. Returns all completed packets
  // @param chunk Buffer object containing raw bytes
  const addBytes = function (chunk) {
    if (chunk.length === 0) return []

    let packets = []
    // append the new data to the existing buffer
    _buffer.put(chunk)

    let new_packet

    // pull all finished packets out of the buffer
    while (new_packet = _packetHunt())
      if (new_packet)
        packets.push(new_packet)

    return packets
  }

  const getBuffer = function() {
    return _buffer.buffer()
  }

  // pull 1 complete packet out of the buffer
  // @returns object containing packet on success, null if 0 complete packets
  const _packetHunt = function() {
    let new_packet = null
    let buffer_length = _buffer.buffer().length
    // all packets are at least 12 bytes
    if (buffer_length >= 12) {
      // get the expected packet size
      let o = binary.parse(_buffer.buffer()).word32be('reqType').word32be('type').word32be('size').vars

      // determine if a full packet is in the buffer
      if (buffer_length >= (12 + o.size)) {
        // yes we have enough for a packet! shift bytes off the buffer
        new_packet = Buffer.alloc(12 + o.size)
        _buffer.buffer().copy(new_packet, 0, 0, new_packet.length)
        // remove the bytes from the existing buffer
        let new_buffer = Buffer.alloc(buffer_length - new_packet.length)
        _buffer.buffer().copy(new_buffer, 0, new_packet.length, buffer_length)
        _buffer = put().put(new_buffer)
      }
    }
    return new_packet
  }

  return Object.freeze({ addBytes, getBuffer })
}
