var Gearman      = require('../').Gearman;
var packet_types = require('../').packet_types;
var put          = require('put');
require('buffertools').extend();


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


exports.testEncodeReturnsBuffer = function(test){
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


exports.testInvalidPacketType = function(test){
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
  test.equal(b.length, 12, 'packet length is wrong for encoded CAN_DO packet');
  test.done();
};


exports.testCAN_DO = function(test){
	var b = this.g._encodePacket(packet_types.CAN_DO, 'test function');
	test.equal(b.length, 25, 'CAN_DO packet is wrong length');

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
	test.equal(b.length, 12, 'RESET_PACKET packet is wrong length');

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
	test.equal(b.length, 39, 'SUBMIT_JOB packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0,7,0,0,0, 0x1b, 0x74, 0x65, 
  	0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00,
  	 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f, 
  	 0x61, 0x64]);
  test.ok( b.equals(data), 'encoded SUBMIT_JOB packet is malformed');
  test.done();
};

exports.testGRAB_JOB = function(test){
	var b = this.g._encodePacket(packet_types.GRAB_JOB);
	test.equal(b.length, 12, 'GRAB_JOB packet is wrong length');

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
	test.equal(b.length, 33, 'WORK_STATUS packet is wrong length');

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
	test.equal(b.length, 44, 'WORK_COMPLETE packet is wrong length');

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
	test.equal(b.length , 27, 'WORK_FAIL packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 14, 0, 0, 0, 15, 0x74, 0x65,
  0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65 ]);
  test.ok( b.equals(data), 'encoded WORK_FAIL packet is malformed');
  test.done();
};

exports.testGET_STATUS = function(test){
	var payload = new Buffer('test job handle', 'ascii');

	var b = this.g._encodePacket(packet_types.GET_STATUS, payload);
	test.ok((b.length == 27), 'GET_STATUS packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 15, 0, 0, 0, 15, 0x74, 0x65,
  0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65 ]);
  test.ok( b.equals(data), 'encoded GET_STATUS packet is malformed');
  test.done();
};

exports.testECHO_REQ = function(test){
	var payload = new Buffer('Hello World!', 'ascii');

	var b = this.g._encodePacket(packet_types.ECHO_REQ, payload);
	test.ok((b.length == 24), 'ECHO_REQ packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 16, 0, 0, 0, 12, 0x48,
  0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21 ]);
  test.ok( b.equals(data), 'encoded ECHO_REQ packet is malformed');
  test.done();
};

exports.testSUBMIT_JOB_BG = function(test){
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

	var b = this.g._encodePacket(packet_types.SUBMIT_JOB_BG, payload2);
	test.equal(b.length, 39, 'SUBMIT_JOB_BG packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0,18,0,0,0, 0x1b, 0x74, 0x65, 
  	0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00,
  	 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f, 
  	 0x61, 0x64]);
  test.ok( b.equals(data), 'encoded SUBMIT_JOB_BG packet is malformed');
  test.done();
};

exports.testSUBMIT_JOB_HIGH = function(test){
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

	var b = this.g._encodePacket(packet_types.SUBMIT_JOB_HIGH, payload2);
	test.equal(b.length, 39, 'SUBMIT_JOB_HIGH packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0,21,0,0,0, 0x1b, 0x74, 0x65, 
  	0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00,
  	 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f, 
  	 0x61, 0x64]);
  test.ok( b.equals(data), 'encoded SUBMIT_JOB_HIGH packet is malformed');
  test.done();
};

exports.testSET_CLIENT_ID = function(test){
	var payload = new Buffer('test client id', 'ascii');

	var b = this.g._encodePacket(packet_types.SET_CLIENT_ID, payload);
	test.equal(b.length, 26, 'SET_CLIENT_ID packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 22, 0, 0, 0, 14, 0x74, 0x65,
  0x73, 0x74, 0x20, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x20, 0x69, 0x64 ]);
  test.ok( b.equals(data), 'encoded SET_CLIENT_ID packet is malformed');
  test.done();
};

exports.testCAN_DO_TIMEOUT = function(test){
	var timeout = 2000;
	var payload = put().
		put(new Buffer('test function', 'ascii')).
		word8(0).
		word32be(timeout).
		buffer();

	var b = this.g._encodePacket(packet_types.CAN_DO_TIMEOUT, payload);
	test.equal(b.length, 30, 'CAN_DO_TIMEOUT packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 23, 0, 0, 0, 18, 0x74, 0x65,
   0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00, 
   0x00, 0x00, 0x07, 0xd0 ]);
  test.ok( b.equals(data), 'encoded CAN_DO_TIMEOUT packet is malformed');
  test.done();
};

exports.testALL_YOURS = function(test){
	var b = this.g._encodePacket(packet_types.ALL_YOURS);
	test.equal(b.length, 12, 'ALL_YOURS packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0,24,0,0,0,0]);
  test.ok( b.equals(data), 'encoded ALL_YOURS packet is malformed');
  test.done();
};

exports.testWORK_EXCEPTION = function(test){
	var job_handle = new Buffer('test job handle', 'ascii');
	var exception = new Buffer('test job handle', 'ascii');
	var payload = put().
		put(job_handle).
		word8(0).
		put(exception).
		buffer();

	var b = this.g._encodePacket(packet_types.WORK_EXCEPTION, payload);
	test.equal(b.length, 43, 'WORK_EXCEPTION packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 25, 0, 0, 0, 31, 0x74, 0x65,
   0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 
   0x65, 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 
   0x61, 0x6e, 0x64, 0x6c, 0x65 ]);
  test.ok( b.equals(data), 'encoded WORK_EXCEPTION packet is malformed');
  test.done();
};

exports.testOPTION_REQ = function(test){
	var payload = new Buffer('exceptions');

	var b = this.g._encodePacket(packet_types.OPTION_REQ, payload);
	test.equal(b.length, 22, 'OPTION_REQ packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 26, 0, 0, 0, 10, 0x65, 0x78,
  	0x63, 0x65, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73 ]);
  test.ok( b.equals(data), 'encoded OPTION_REQ packet is malformed');
  test.done();
};

exports.testWORK_DATA = function(test){
	var job_handle = new Buffer('test job handle', 'ascii');
	var payload = new Buffer('partial work', 'ascii');
	payload = put().
		put(job_handle).
		word8(0).
		put(payload).
		buffer();

	var b = this.g._encodePacket(packet_types.WORK_DATA, payload);
	test.equal(b.length, 40, 'WORK_DATA packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 28, 0, 0, 0, 28, 0x74, 0x65,
   0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 
   0x65, 0x00, 0x70, 0x61, 0x72, 0x74, 0x69, 0x61, 0x6c, 0x20, 0x77, 0x6f, 
   0x72, 0x6b ]);
  test.ok( b.equals(data), 'encoded WORK_DATA packet is malformed');
  test.done();
};

exports.testWORK_WARNING = function(test){
	var job_handle = new Buffer('test job handle', 'ascii');
	var warning = new Buffer('test warning', 'ascii');
	payload = put().
		put(job_handle).
		word8(0).
		put(warning).
		buffer();

	var b = this.g._encodePacket(packet_types.WORK_WARNING, payload);
	test.equal(b.length, 40, 'WORK_WARNING packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 29, 0, 0, 0, 28, 0x74, 0x65,
   0x73, 0x74, 0x20, 0x6a, 0x6f, 0x62, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x6c, 
   0x65, 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x77, 0x61, 0x72, 0x6e, 0x69, 0x6e, 0x67 ]);
  test.ok( b.equals(data), 'encoded WORK_WARNING packet is malformed');
  test.done();
};

exports.testGRAB_JOB_UNIQ = function(test){
	var b = this.g._encodePacket(packet_types.GRAB_JOB_UNIQ);
	test.equal(b.length, 12, 'GRAB_JOB_UNIQ packet is wrong length');
	
  var data = new Buffer([0,0x52,0x45,0x51,0,0,0, 30, 0, 0, 0, 0 ]);
  test.ok( b.equals(data), 'encoded GRAB_JOB_UNIQ packet is malformed');
  test.done();
};

exports.testSUBMIT_JOB_HIGH_BG = function(test){
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

	var b = this.g._encodePacket(packet_types.SUBMIT_JOB_HIGH_BG, payload2);
	test.equal(b.length, 39, 'SUBMIT_JOB_HIGH_BG packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0,32,0,0,0, 0x1b, 0x74, 0x65, 
  	0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00,
  	 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f, 
  	 0x61, 0x64]);
  test.ok( b.equals(data), 'encoded SUBMIT_JOB_HIGH_BG packet is malformed');
  test.done();
};

exports.testSUBMIT_JOB_LOW = function(test){
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

	var b = this.g._encodePacket(packet_types.SUBMIT_JOB_LOW, payload2);
	test.equal(b.length, 39, 'SUBMIT_JOB_LOW packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0,33,0,0,0, 0x1b, 0x74, 0x65, 
  	0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00,
  	 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f, 
  	 0x61, 0x64]);
  test.ok( b.equals(data), 'encoded SUBMIT_JOB_LOW packet is malformed');
  test.done();
};

exports.testSUBMIT_JOB_LOW_BG = function(test){
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

	var b = this.g._encodePacket(packet_types.SUBMIT_JOB_LOW_BG, payload2);
	test.equal(b.length, 39, 'SUBMIT_JOB_LOW_BG packet is wrong length');

  var data = new Buffer([0,0x52,0x45,0x51,0,0,0,34,0,0,0, 0x1b, 0x74, 0x65, 
  	0x73, 0x74, 0x20, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00,
  	 0x00, 0x74, 0x65, 0x73, 0x74, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f, 
  	 0x61, 0x64]);
  test.ok( b.equals(data), 'encoded SUBMIT_JOB_LOW_BG packet is malformed');
  test.done();
};
