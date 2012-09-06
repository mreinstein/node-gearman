var Gearman = require('../gearman.js').Gearman;
var packet_types = require('../gearman.js').packet_types;
require('buffertools');
var put = require('put');

exports.setUp = function (callback) {
        this.g = new Gearman();
        callback();
};

exports.tearDown = function (callback) {
    // clean up
    this.g.close();
    this.g = null;
  
    callback();
};


// passing something other than a buffer should throw an error
exports.testInvalidInputBuffer = function(test){
	test.throws(function(){
		this.g._decodePacket();
	});
	test.throws(function(){
		this.g._decodePacket('');
	});
	test.throws(function(){
		this.g._decodePacket(null);
	});
	test.throws(function(){
		this.g._decodePacket(undefined_var);
	});
	test.throws(function(){
		this.g._decodePacket({});
	});
	test.throws(function(){
		this.g._decodePacket(function(){});
	});

	test.throws(function(){
		this.g._decodePacket(023);
	});
	test.done();
};

exports.testMagicHeader = function(test){
	// test a bad header
	test.throws(function(){
		var bad_buffer = new Buffer([0x00, 0x00, 0x45, 0x51, 0, 0, 0, 3, 0, 0, 0, 0]);
		this.g._decodePacket(bad_buffer);
	});

	// test 2 good headers (REQ and RES)
	var good_buffer1 = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, packet_types.RESET_ABILITIES, 0, 0, 0, 0]);
	var result = this.g._decodePacket(good_buffer1);
	test.equal(result.type, packet_types.RESET_ABILITIES, 'REQ magic header fails');
	var good_buffer2 = new Buffer([0x00, 0x52, 0x45, 0x53, 0, 0, 0, packet_types.RESET_ABILITIES, 0, 0, 0, 0]);
	result = this.g._decodePacket(good_buffer1);
	test.equal(result.type, packet_types.RESET_ABILITIES, 'RES magic header fails');

	test.done();
};


exports.testInvalidPacketType = function(test){
	// passing an invalid packet type should throw an exception
	var bad_buffer = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, 0, 0, 0, 0, 0 ]);
	test.throws(function(){
		this.g._decodePacket(bad_buffer);
	});

	// passing an invalid packet type should throw an exception
	bad_buffer = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, 37, 0, 0, 0, 0 ]);
	test.throws(function(){
		this.g._decodePacket(bad_buffer);
	});

	test.done();
};

exports.testValidPacketType = function(test){
	var good_buffer = null, result = null;

	// validate all the valid packet types result in a buffer
	for(var p_type=1; p_type< 37; p_type++)
	{
		good_buffer = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, p_type, 0, 0, 0, 0]);
		result = this.g._decodePacket(good_buffer);
		test.equal(result.type, p_type, 'packet type ' + p_type + ' failed to decode');
	}

	test.done();
};

exports.testInvalidPacketSize = function(test){
	// passing an invalid packet size should throw an exception
	var bad_buffer = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, 3, 0, 0, 0, 1 ]);
	test.throws(function(){
		this.g._decodePacket(bad_buffer);
	});

	// passing an invalid packet type should throw an exception
	bad_buffer = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, 3, 0, 0, 0, 0x04 ]);
	test.throws(function(){
		this.g._decodePacket(bad_buffer);
	});

	test.done();
};

exports.testValidPacketSize = function(test){
	// passing an invalid packet size should throw an exception
	var bad_buffer = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, 3, 0, 0, 0, 1, 0x45 ]);
	test.throws(function(){
		this.g._decodePacket(bad_buffer);
	});

	// passing an invalid packet type should throw an exception
	bad_buffer = new Buffer([0x00, 0x52, 0x45, 0x51, 0, 0, 0, 3, 0, 0, 0, 4, 0x45, 0x45, 0x45, 0x45 ]);
	test.throws(function(){
		this.g._decodePacket(bad_buffer);
	});

	test.done();
};


exports.testParsePacket = function(test){
	var good_buffer = new Buffer([ 0x74, 0x65, 0x73, 0x74, 0x20, 0x66, 0x75, 
							   0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00, 0x00, 
							   0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 
							   0x6c, 0x6f, 0x61, 0x64 ]);
	result = this.g._parsePacket(good_buffer, 's8s');
	test.equal(result['0'], 'test function');
	test.equal(result['1'], 0);
	test.equal(result['2'], 'test payload');

	good_buffer = new Buffer([ 0x74 ]);
	result = this.g._parsePacket(good_buffer, '8');
	test.equal(result['0'], 116);

	test.done();
};

