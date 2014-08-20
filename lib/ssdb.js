// Nodejs client for https://github.com/ideawu/ssdb
// Copyright (c) 2014 Eleme, Inc.

var events = require('events');
var net = require('net');
var util = require('util');
var Q = require('q');


conversions = {  // cast responsed strings to js types
  int: function (val) {
    return parseInt(val, 10)
  },
  float: parseFloat,
  str: function (val) {
    return val
  },
  bool: function (val) {
    return !!parseInt(val, 10)
  },
}


commands = {
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
}


function ResponseParser() {
}


ResponseParser.prototype.parse = function (buf) {
  this.unfinished = this.unfinished || '';

  var data = this.unfinished + buf.toString();
  data = new Buffer(data);
  this.unfinished = '';

  var resps = [], resp = [];
  var cursor = 0;
  var size, pos, body, last;

  while (cursor < data.length) {
    pos = [].indexOf.apply(data, [10, cursor]) //查找回车 \n

    if (pos > 0) {
      var sstr = data.slice(cursor, pos);
      cursor = ++pos;  // go to the next of `pos`

      if (!sstr.length) {
        resps.push(resp);
        resp = [];
        last = cursor;
      } else {
        size = parseInt(sstr, 10);
        body = data.slice(cursor, cursor + size);
        cursor += size + 1;  // go to the end of this resp
        if (cursor < data.length) {
          resp.push(body.toString());  // push if this a finished resp
        }
      }
    }
  }

  if (cursor > data.length) {
    this.unfinished = data.slice(last);
  }

  return resps;
}


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
  this.sock.on('data', function(buf){return self.onrecv(buf)});

  // register connect events
  this.sock.on('connect', function(){self.emit('connect')});
  this.sock.on('error', function(err){self.emit('error', err)});
  this.sock.on('timeout', function(){self.emit('timeout')});
  this.sock.on('drain', function(){self.emit('drain')});
  this.sock.on('end', function(){self.emit('end')});
  this.sock.on('close', function(){self.emit('close')});
}


Connection.prototype.close = function () {
  if (this.sock) {
    this.sock.end();
    this.sock.destroy();
    this.sock = null;
  }
}


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
}


Connection.prototype.send = function (cmd, params) {
  if (!this.sock) this.connect();
  var buffer = this.compile(cmd, params);
  return this.sock.write(buffer);
}


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
}


Connection.prototype.onrecv = function (buf) {
  var responses = this.parser.parse(buf);
  var callbacks = this.callbacks;
  var self = this;

  responses.forEach(function (response) {
    var error;
    var data;

    var status = response[0];
    var body = response.slice(1);
    var command = self.commands.shift()

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
}


Connection.prototype.request = function (cmd, params, callback) {
  this.commands.push(cmd);
  this.callbacks.push(callback);
  return this.send(cmd, params);
}


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
  this.conn.on('status_ok', function(cmd, data){self.emit('status_ok', cmd, data)});
  this.conn.on('status_not_found', function(cmd){self.emit('status_not_found', cmd)});
  this.conn.on('status_client_error', function(cmd){self.emit('status_client_error', cmd)});
  this.conn.on('status_not_ok', function(status, cmd){self.emit('status_not_ok', status, cmd)});

  // connection events, the same as `net.socket`
  this.conn.on('connect', function(){self.emit('connect')});
  this.conn.on('error', function(err){self.emit('error', err)});
  this.conn.on('timeout', function(){self.emit('timeout')});
  this.conn.on('drain', function(){self.emit('drain')});
  this.conn.on('end', function(){self.emit('end')});
  this.conn.on('close', function(){self.emit('close')});
}


Client.prototype._registerCommands = function () {
  var self = this;

  for (var key in commands) {
    (function (key) {
      self[key] = function () {
        var callback;
        var cmd = key;
        var params = Array.prototype.slice.call(arguments, 0, -1);
        var lastItem = Array.prototype.slice.call(arguments, -1)[0];

        if (typeof lastItem === 'function') var callback = lastItem;
        else params.push(lastItem);

        return self.conn.request(cmd, params, callback);
      };
    })(key);
  }
}


Client.prototype.quit = function () {
  return this.conn.close();
}


Client.prototype.unref = function () {
  return this.conn.sock.unref();
}


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
}


Client.prototype.promisify = function () {
  for (var cmd in commands) {
    this[cmd] = Q.nbind(this[cmd]);
  }
  return this;
}

exports.commands = commands;
exports.createClient = function (options) {
  return new Client(options || {})
};
