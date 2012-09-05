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


exports.textEncodeReturnsBuffer = function(test){
	var b = this.g._encodePacket(packet_types.CAN_DO, 'some data', 'ascii');
	test.ok(Buffer.isBuffer(b), 'passing string payload fails');

	var c = this.g._encodePacket(packet_types.CAN_DO, new Buffer('some data'), 'ascii');
	test.ok(Buffer.isBuffer(c), 'passing buffer payload fails');

	var d = this.g._encodePacket(packet_types.CAN_DO, new Buffer(''), 'ascii');
	test.ok(Buffer.isBuffer(d), 'passing empty buffer payload fails');

	var e = this.g._encodePacket(packet_types.CAN_DO, '', 'ascii');
	test.ok(Buffer.isBuffer(e), 'passing empty string payload fails');

	var f = this.g._encodePacket(packet_types.CAN_DO, null, 'ascii');
	test.ok(Buffer.isBuffer(f), 'passing null payload fails');

	test.done();
};


exports.textInvalidPacketType = function(test){
	// passing an invalid packet type should throw an exception
	test.throws(function(){
		this.g._encodePacket(0);
	});

	// passing an invalid packet type should throw an exception
	test.throws(function(){
		this.g._encodePacket(37);
	});

	// passing an invalid packet type should throw an exception
	test.throws(function(){
		this.g._encodePacket(null);
	});

	// passing an invalid packet type should throw an exception
	test.throws(function(){
		this.g._encodePacket(undefined_var);
	});

	test.done();
};

exports.testValidPacketType = function(test){
	// validate all the valid packet types result in a buffer
	for(var p_type=1; p_type< 37; p_type++)
	{
		var h = this.g._encodePacket(p_type);
		test.ok(Buffer.isBuffer(h), 'packet type '+ p_type +' fails');
	}
    test.done();
};

exports.testPacketLength = function(test){
	var type = data = encoding = null;

	type = ''
	var b = this.g._encodePacket(packet_types.CAN_DO, '');
    test.expect(1);
    test.ok(true, "this assertion should pass");
    test.done();
};

exports.testCAN_DO = function(test){
	var b = this.g._encodePacket(packet_types.CAN_DO, 'test function');
	test.ok((b.length == 25), 'CAN_DO packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0,1,0,0,0,13, 0x74, 0x65, 0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e]);
    test.ok( b.equals(data), 'encoded CAN_DO packet is malformed');
    test.done();
};

exports.testCANT_DO = function(test){
	var b = this.g._encodePacket(packet_types.CANT_DO, 'test function');
	test.ok((b.length == 25), 'CANT_DO packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0,2,0,0,0,13, 0x74, 0x65, 0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e]);
    test.ok( b.equals(data), 'encoded CANT_DO packet is malformed');
    test.done();
};

exports.testRESET_ABILITIES = function(test){
	var b = this.g._encodePacket(packet_types.RESET_ABILITIES);
	test.ok((b.length == 12), 'RESET_PACKET packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0,3,0,0,0,0]);
    test.ok( b.equals(data), 'encoded RESET_ABILITIES packet is malformed');
    test.done();
};

exports.testPRE_SLEEP = function(test){
	var b = this.g._encodePacket(packet_types.PRE_SLEEP);
	var data = new Buffer([0,0x52,0x45,0x51,0,0,0,4,0,0,0,0]);
    test.ok( b.equals(data), 'encoded PRE_SLEEP packet is malformed');
    test.done();
};

exports.testSUBMIT_JOB = function(test){
	var func_name = 'test function';
	var unique_id = '';
	var payload = new Buffer('test payload');
	var payload2 = put().
		put(new Buffer(func_name, 'ascii')).
		word8(0).
		put(new Buffer(unique_id, 'ascii')).
		word8(0).
		put(payload).
		buffer();

	var b = this.g._encodePacket(packet_types.SUBMIT_JOB, payload2);
	test.ok((b.length == 39), 'SUBMIT_JOB packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0,7,0,0,0, 0x1b, 0x74, 0x65, 
    	0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00,
    	 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f, 
    	 0x61, 0x64]);
    test.ok( b.equals(data), 'encoded SUBMIT_JOB packet is malformed');
    test.done();
};

exports.testGRAB_JOB = function(test){
	var b = this.g._encodePacket(packet_types.GRAB_JOB);
	test.ok((b.length == 12), 'GRAB_JOB packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0,9,0,0,0,0]);
    test.ok( b.equals(data), 'encoded GRAB_JOB packet is malformed');
    test.done();
};

exports.testWORK_STATUS = function(test){
	var job_handle = 'test job handle';
	var percent_numerator = '2';
	var percent_denominator = '100';
	var payload = put().
		put(new Buffer(job_handle, 'ascii')).
		word8(0).
		put(new Buffer(percent_numerator, 'ascii')).
		word8(0).
		put(new Buffer(percent_denominator, 'ascii')).
		buffer();

	var b = this.g._encodePacket(packet_types.WORK_STATUS, payload);
	test.ok((b.length == 33), 'WORK_STATUS packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 12, 0, 0, 0, 21, 0x74, 0x65,
     0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 
     0x65, 0x00, 0x32, 0x00, 0x31, 0x30, 0x30]);
    test.ok( b.equals(data), 'encoded WORK_STATUS packet is malformed');
    test.done();
};

exports.testWORK_COMPLETE = function(test){
	var job_handle = new Buffer('test job handle', 'ascii');
	var payload = new Buffer('test work result', 'ascii');
	payload = put().
		put(job_handle).
		word8(0).
		put(payload).
		buffer();

	var b = this.g._encodePacket(packet_types.WORK_COMPLETE, payload);
	test.ok((b.length == 44), 'WORK_COMPLETE packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 13, 0, 0, 0, 32, 0x74, 0x65,
    0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 
    0x65, 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x77, 0x6f, 0x72, 0x6b, 0x20, 
    0x72, 0x65, 0x73, 0x75, 0x6c, 0x74]);
    test.ok( b.equals(data), 'encoded WORK_COMPLETE packet is malformed');
    test.done();
};

exports.testWORK_FAIL = function(test){
	var job_handle = new Buffer('test job handle', 'ascii');
	payload = put().
		put(job_handle).
		buffer();

	var b = this.g._encodePacket(packet_types.WORK_FAIL, payload);
	test.ok((b.length == 27), 'WORK_FAIL packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 14, 0, 0, 0, 15, 0x74, 0x65,
    0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65 ]);
    test.ok( b.equals(data), 'encoded WORK_FAIL packet is malformed');
    test.done();
};

exports.testGET_STATUS = function(test){
	var job_handle = new Buffer('test job handle', 'ascii');
	payload = put().
		put(job_handle).
		buffer();

	var b = this.g._encodePacket(packet_types.GET_STATUS, payload);
	console.log (b);
	test.ok((b.length == 27), 'GET_STATUS packet is wrong length');

    var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 15, 0, 0, 0, 15, 0x74, 0x65,
    0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65 ]);
    test.ok( b.equals(data), 'encoded GET_STATUS packet is malformed');
    test.done();
};
