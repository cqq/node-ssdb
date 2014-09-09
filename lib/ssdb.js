// Nodejs client for https://github.com/ideawu/ssdb
// Copyright (c) 2014 Eleme, Inc.

var events = require('events');
var net = require('net');
var util = require('util');
var Q = require('q');
var pipline_mode = false;
var pipline_mode_buff;
var conversions = {  // cast responsed strings to js types
  int: function (val) {
    return parseInt(val, 10);
  },
  float: parseFloat,
  str: function (val) {
    return val;
  },
  bool: function (val) {
    return !!parseInt(val, 10);
  },
};


var commands = {
  set: 'int',
  setx: 'int',
  expire: 'int',
  ttl: 'int',
  setnx: 'int',
  get: 'str',
  getset: 'str',
  del: 'int',
  incr: 'int',
  exists: 'bool',
  getbit: 'int',
  setbit: 'int',
  countbit: 'int',
  substr: 'str',
  strlen: 'int',
  keys: 'list',
  scan: 'list',
  rscan: 'list',
  multi_set: 'int',
  multi_get: 'list',
  multi_del: 'int',
  hset: 'int',
  hget: 'str',
  hdel: 'int',
  hincr: 'int',
  hexists: 'bool',
  hsize: 'int',
  hlist: 'list',
  hrlist: 'list',
  hkeys: 'list',
  hgetall: 'list',
  hscan: 'list',
  hrscan: 'list',
  hclear: 'int',
  multi_hset: 'int',
  multi_hget: 'list',
  multi_hdel: 'int',
  zset: 'int',
  zget: 'int',
  zdel: 'int',
  zincr: 'int',
  zexists: 'bool',
  zsize: 'int',
  zlist: 'list',
  zrlist: 'list',
  zkeys: 'list',
  zscan: 'list',
  zrscan: 'list',
  zrank: 'int',
  zrrank: 'int',
  zrange: 'list',
  zrrange: 'list',
  zclear: 'int',
  zcount: 'int',
  zsum: 'int',
  zavg: 'float',
  zremrangebyrank: 'int',
  zremrangebyscore: 'int',
  multi_zset: 'int',
  multi_zget: 'list',
  multi_zdel: 'int',
  qsize: 'int',
  qclear: 'int',
  qfront: 'str',
  qback: 'str',
  qget: 'str',
  qslice: 'list',
  qpush: 'str',
  qpush_front: 'int',
  qpush_back: 'int',
  qpop: 'str',
  qpop_front: 'str',
  qpop_back: 'str',
  qlist: 'list',
  qrlist: 'list',
  info: 'list'
};


function ResponseParser() {
}


ResponseParser.prototype.parse = function(buf) {
  buf = new Buffer(buf);

  if (this.unfinished) {
    // pick up unfinished buffer last time
    buf = Buffer.concat([this.unfinished, buf]);
  }

  var resps = [], resp = [];
  var len = buf.length;
  var p = 0;  // pointer to loop over buffer
  var q = 0; // the position of the first '\n' in an unit
  var r = 0;  // always the start of the next response
  var d;

  while (p < len) {
    // find the first '\n'
    q = [].indexOf.apply(buf, [10, p]);

    // no '\n' was found
    if (q < 0) break;

    q += 1;  // an '\n' was found, skip it

    d = q - p;

    // the first char is '\n' or the first two chars are '\r\n',
    // ends of a response
    if (d == 1 || (d == 2 && buf[p] == 13)) {
      r = p += d; // skip the distance
      resps.push(resp); resp = [];
      continue;  // continue to next response
    }

    // size of data
    var size = + buf.slice(p, q);

    // moving to the '\n' after data
     var t = p = q + size;

     if (p < len && buf[p] == 10)
       p += 1;  // skip a '\n'
     else if (p + 1 < len && buf[p] == 10 && buf[p + 1] == 13)
       p += 2;  // skip a '\r\n'
     else
       break; // exceeds len

     var data = buf.slice(q, t);
     resp.push(data.toString());
  }

  if (r < len) {
    this.unfinished = buf.slice(r);
  } else {
    this.unfinished = undefined;
  }

  return resps;
};

function Connection(port, host, timeout) {
  this.host = host || '0.0.0.0';
  this.port = port || 8888;
  this.timeout = timeout || 0;

  this.sock = null;
  this.callbacks = [];
  this.commands = [];
  this.parser = new ResponseParser();

  events.EventEmitter.call(this);
}
util.inherits(Connection, events.EventEmitter);


Connection.prototype.connect = function () {
  this.sock = net.Socket();
  this.sock.setTimeout(this.timeout);
  this.sock.setEncoding('utf8');
  this.sock.setNoDelay(true);
  this.sock.setKeepAlive(true);
  var self = this;
  this.sock.connect(this.port, this.host, this.connectListener);
  this.sock.on('data', function(buf){return self.onrecv(buf);});

  // register connect events
  this.sock.on('connect', function(){self.emit('connect');});
  this.sock.on('error', function(err){self.emit('error', err);});
  this.sock.on('timeout', function(){self.emit('timeout');});
  this.sock.on('drain', function(){self.emit('drain');});
  this.sock.on('end', function(){self.emit('end');});
  this.sock.on('close', function(){self.emit('close');});
};


Connection.prototype.close = function () {
  if (this.sock) {
    this.sock.end();
    this.sock.destroy();
    this.sock = null;
  }
};


Connection.prototype.compile = function (cmd, params) {  // build command buffer
  var args = [];
  var list = [];
  var pattern = '%d\n%s\n';

  args.push(cmd);
  Array.prototype.push.apply(args, params);

  for (var i = 0; i < args.length; i++) {
    var arg = args[i];
    var bytes = Buffer.byteLength(util.format('%s', arg));
    list.push(util.format(pattern, bytes, arg));
  }

  list.push('\n');
  return new Buffer(list.join(''));
};

Connection.prototype.send = function (cmd, params) {
  if (!this.sock) this.connect();
  var buffer = this.compile(cmd, params);
  if (pipline_mode == false) {

     // console.log("send to socket" +  buffer);
      return this.sock.write(buffer);
  } else {
      if (!pipline_mode_buff) {
          pipline_mode_buff = new Buffer(buffer);
      } else {
          pipline_mode_buff = Buffer.concat([pipline_mode_buff, buffer]);
      }

      //console.log("batch mode = " +  buffer);

  }

};


Connection.prototype.buildValue = function (type, data) {
  switch (type) {
    case 'int':
    case 'str':
    case 'bool':
    case 'float':
      return conversions[type](data[0]);
    case 'list':
      return data;
  }
};


Connection.prototype.onrecv = function (buf) {
  var responses = this.parser.parse(buf);
  var callbacks = this.callbacks;
  var self = this;

  responses.forEach(function (response) {
    var error;
    var data;

    var status = response[0];
    var body = response.slice(1);
    var command = self.commands.shift();

    switch (status) {
      case 'ok':
        var type = commands[command] || 'str';
        data = self.buildValue(type, body);
        self.emit('status_ok', command, data);
        break;
      case 'not_found':
        self.emit('status_not_found', command);
        self.emit('status_not_ok', status, command);
        break;
      case 'client_error':
        self.emit('status_client_error', command);
      default:
        self.emit('status_not_ok', status, command);
        var etpl = "ssdb: '%s' on command '%s'";
        error = util.format(etpl, status, command);
    }

    var callback = callbacks.shift();
    if (callback) callback(error, data);
  });
};


Connection.prototype.request = function (cmd, params, callback) {
  this.commands.push(cmd);
  this.callbacks.push(callback);
  return this.send(cmd, params);
};


function Client(options) {
  this.conn = new Connection(options.port, options.host, options.timeout);
  this._registerCommands();
  this._registerEvents();
  events.EventEmitter.call(this);
}
util.inherits(Client, events.EventEmitter);


Client.prototype._registerEvents = function () {
  var self = this;

  // status code events
  this.conn.on('status_ok', function(cmd, data){self.emit('status_ok', cmd, data);});
  this.conn.on('status_not_found', function(cmd){self.emit('status_not_found', cmd);});
  this.conn.on('status_client_error', function(cmd){self.emit('status_client_error', cmd);});
  this.conn.on('status_not_ok', function(status, cmd){self.emit('status_not_ok', status, cmd);});

  // connection events, the same as `net.socket`
  this.conn.on('connect', function(){self.emit('connect');});
  this.conn.on('error', function(err){self.emit('error', err);});
  this.conn.on('timeout', function(){self.emit('timeout');});
  this.conn.on('drain', function(){self.emit('drain');});
  this.conn.on('end', function(){self.emit('end');});
  this.conn.on('close', function(){self.emit('close');});
};


Client.prototype._registerCommands = function () {
  var self = this;

  for (var key in commands) {
    (function (key) {
      self[key] = function () {
        var callback;
        var cmd = key;
        var params = Array.prototype.slice.call(arguments, 0, -1);
        var lastItem = Array.prototype.slice.call(arguments, -1)[0];

        if (typeof lastItem === 'function') callback = lastItem;
        else params.push(lastItem);

        return self.conn.request(cmd, params, callback);
      };
    })(key);
  }
};


Client.prototype.quit = function () {
  return this.conn.close();
};

Client.prototype.batch = function () {
    pipline_mode = true;

};

Client.prototype.exec = function () {
    if (!this.conn.sock)   this.conn.connect();
    pipline_mode = false;
    var ret = this.conn.sock.write(pipline_mode_buff);

    pipline_mode_buff = undefined;
    return ret;

};


Client.prototype.unref = function () {
  return this.conn.sock.unref();
};


Client.prototype.thunkify = function () {
  var self = this;
  for (var cmd in commands) {
    (function (cmd) {
      var nfunc = self[cmd];
      self[cmd] = function () {
        var args = arguments;
        return function (callback) {
          Array.prototype.push.call(args, callback);
          nfunc.apply(this, args);
        };
      };
    })(cmd);
  }
  return self;
};


Client.prototype.promisify = function () {
  for (var cmd in commands) {
    this[cmd] = Q.nbind(this[cmd]);
  }
  return this;
};

exports.commands = commands;
exports.createClient = function (options) {
  return new Client(options || {});
};
