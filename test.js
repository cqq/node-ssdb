var util = require('util');
var ssdb = require('./lib/ssdb');
var should = require('should');
var co = require('co');
var sleep = require('co-sleep');

var client = ssdb.createClient();
client.thunkify();

// helpers
var uk = (function(base){
  var cursor = base;
  return function(prefix){
    prefix = prefix || 'key';
    return util.format('%s-%d', prefix, cursor++);
  };
})(new Date().getTime());


function randomString(length) {
  var chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz'
              .split('');

  if (! length) {
    length = Math.floor(Math.random() * chars.length);
  }

  var str = '';
  for (var i = 0; i < length; i++) {
    str += chars[Math.floor(Math.random() * chars.length)];
  }
  return str;
}


// mocha ssdb module
describe('ssdb', function(){

  it('set', function(done){
    co(function*(){
      var key = uk();
      var d = yield client.set(key, 'v');
      should(d).eql(1);
      done();
    })();
  });

  it('setx', function(done){
    co(function*(){
      var key = uk();
      var d = yield client.setx(key, 'v', 1.2);
      var ttl = yield client.ttl(key);
      yield sleep(1201);
      var b = yield client.exists(key);
      should(d).eql(1)
      should(ttl).below(1.2);
      should(b).be.false;
      done();
    })();
  });

  it('expire', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'v');
      var b = yield client.expire(key, 1.2);
      var c = yield client.ttl(key);
      var d = yield client.expire(uk(), 1.1);
      should(a).eql(1);
      should(b).eql(1);
      should(c).below(1.2);
      should(d).eql(0);
      done();
    })();
  });

  it('ttl', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.setx(key, 'v', 1.2);
      var b = yield client.ttl(key);
      should(a).eql(1);
      should(b).not.below(0);
      should(b).below(1.2);
      done();
    })();
  });

  it('setnx', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.setnx(key, 'v');
      var b = yield client.setnx(key, 'v');
      should(a).eql(1);
      should(b).eql(0);
      done();
    })();
  });

  it('get', function(done){
    co(function*(){
      var key = uk();
      yield client.set(key, 'v');
      var d = yield client.get(key);
      should(d).eql('v');
      done();
    })();
  });

  it('getset', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'v');
      var b = yield client.getset(key, 'val');
      should(a).eql(1);
      should(b).eql('v');
      done();
    })();
  });

  it('del', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'v');
      var b = yield client.del(key);
      var c = yield client.exists(key);
      should([a, b, c]).eql([1, 1, false]);
      done();
    })();
  });

  it('incr', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 1);
      var b = yield client.incr(key, 2);
      var c = yield client.get(key);
      should([a, b, c]).eql([1, 3, 3]);
      done();
    })();
  });

  it('exists', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 1);
      var b = yield client.exists(key);
      var c = yield client.exists(uk());
      should([a, b, c]).eql([1, true, false]);
      done();
    })();
  });

  it('getbit', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'val');
      var b = yield client.getbit(key, 2);
      should([a, b]).eql([1, 1]);
      done();
    })();
  });

  it('setbit', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'val');
      var b = yield client.setbit(key, 2, 0);
      var c = yield client.get(key);
      should([a, b, c]).eql([1, 1, 'ral']);
      done();
    })();
  });

  it('countbit', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'val');
      var b = yield client.countbit(key);
      should([a, b]).eql([1, 8]);
      done();
    })();
  });

  it('substr', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'hello world');
      var b = yield client.substr(key);
      var c = yield client.substr(key, 6, 10);
      should([a, b, c]).eql([1, 'hello world', 'world']);
      done();
    })();
  });

  it('strlen', function(done){
    co(function*(){
      var key = uk();
      var a = yield client.set(key, 'hello world');
      var b = yield client.strlen(key);
      should([a, b]).eql([1, 11]);
      done();
    })();
  });

  it('keys', function(done){
    co(function*(){
      var start = uk();
      var a = uk(); var b = uk();
      yield client.set(a, 1);
      yield client.set(b, 1);
      d = yield client.keys(start, uk(), 2);  // (start, end]
      should(d).eql([a, b]);
      done();
    })();
  });

  it('scan', function(done){
    co(function*(){
      var start = uk();
      var a = uk(); var b = uk();
      yield client.set(a, 1);
      yield client.set(b, 1);
      d = yield client.scan(start, uk(), -1);  // (start, end]
      should(d).eql([a, 1, b, 1]);
      done();
    })();
  });

  it('rscan', function(done){
    co(function*(){
      var stop = uk();
      var a = uk(); var b = uk();
      yield client.set(a, 1);
      yield client.set(b, 1);
      var start = uk();
      var d = yield client.rscan(start, stop, -1);  // (start, end]
      should(d).eql([b, 1, a, 1]);
      done();
    })();
  });

  it('multi_set/multi_get/multi_del', function(done){
    co(function*(){
      var k1 = uk();
      var k2 = uk();
      var k3 = uk();
      var a = yield client.multi_set(k1, 'v1', k2, 'v2', k3, 'v3');
      var b = yield client.multi_get(k1, k2, k3);
      var c = yield client.multi_del(k1, k2, k3);
      should([a, c]).eql([3, 3]);
      should(b).eql([k1, 'v1', k2, 'v2', k3, 'v3']);
      done();
    })();
  });

  it('hset/hget/hdel/hincr/hexists', function(done){
    co(function*(){
      var hash = uk('hash');
      var field = uk('field');
      var a = yield client.hset(hash, field, 'v');
      var b = yield client.hget(hash, field);
      var c = yield client.hdel(hash, field);
      var d = yield client.hincr(hash, field, 3);
      var e = yield client.hexists(hash, field)
      should([a, b, c, d, e]).eql([1, 'v', 1, 3, true]);
      done();
    })();
  });

  it('hexists', function(done){
    co(function*(){
      var hash = uk('hash');
      var field = uk('field');
      var d = client.hexists(hash, field);
      should(yield d).eql(false);
      done();
    })();
  });

  it('hsize', function(done){
    co(function*(){
      var hash = uk('hash');
      var d = [];
      for (var i = 0; i < 10; i++) {
        d.push(client.hset(hash, uk('field'), 'v'));
      }
      yield d;
      should(yield client.hsize(hash)).eql(10);
      done();
    })();
  });

  it('hlist/hrlist', function(done){
    co(function*(){
      var start = uk('hash');
      var a = uk('hash');
      var b = uk('hash');
      yield client.hset(a, 'field', 'v');
      yield client.hset(b, 'field', 'v');
      var lst = yield client.hlist(start, uk('hash'), -1);
      var rlst = yield client.hrlist(uk('hash'), start, -1);
      should(lst).eql([a, b]);
      should(rlst).eql([b, a]);
      done();
    })();
  });

  it('hkeys/hscan/hrscan/hgetall/hclear', function(done){
    co(function*(){
      var h = uk('hash');
      var a = uk('field');
      var b = uk('field');
      yield client.hset(h, a, 'va');
      yield client.hset(h, b, 'vb');
      var keys = yield client.hkeys(h, '', '', -1);
      var scan = yield client.hscan(h, '', '', -1);
      var rscan = yield client.hrscan(h, '', '', -1);
      var all = yield client.hgetall(h, '', '', -1);
      var nums = yield client.hclear(h);
      should(keys).eql([a, b]);
      should(scan).eql([a, 'va', b, 'vb']);
      should(rscan).eql([b, 'vb', a, 'va']);
      should(all).eql([a, 'va', b, 'vb']);
      should(nums).eql(2);
      should(yield client.hsize(h)).eql(0);
      done();
    })();
  });

  it('multi_hset/multi_hget/multi_hdel', function(done){
    co(function*(){
      var h = uk('hash');
      var k1 = uk();
      var k2 = uk();
      var k3 = uk();
      var a = yield client.multi_hset(h, k1, 'v1', k2, 'v2', k3, 'v3');
      var b = yield client.multi_hget(h, k1, k2, k3);
      var c = yield client.multi_hdel(h, k1, k2, k3);
      should([a, c]).eql([3, 3]);
      should(b).eql([k1, 'v1', k2, 'v2', k3, 'v3']);
      should(yield client.hsize(h)).eql(0);
      done();
    })();
  });

  it('zset/zget/zdel/zincr/zexists', function(done){
    co(function*(){
      var z = uk('zset');
      var k = uk();
      var a = client.zset(z, k, 13);
      var b = client.zget(z, k);
      var c = client.zincr(z, k, 3);
      var d = client.zexists(z, k);
      var e = client.zdel(z, k);
      var f = client.zexists(z, k);
      should(yield [a, b, c, d, e, f]).eql([1, 13, 16, true, 1, false]);
      done();
    })();
  });

  it('zsize', function(done){
    co(function*(){
      var z = uk('zset');
      for (var i = 0; i < 10; i++) {
        yield client.zset(z, uk(), i + 10);
      }
      should(yield client.zsize(z)).eql(10);
      done();
    })();
  });

  it('zlist/zrlist', function(done){
    co(function*(){
      var start = uk('zset');
      var a = uk('zset');
      var b = uk('zset');
      yield client.zset(a, 'key', 12581);
      yield client.zset(b, 'key', 12581);
      var lst = yield client.zlist(start, uk('zset'), -1);
      var rlst = yield client.zrlist(uk('zset'), start, -1);
      should(lst).eql([a, b]);
      should(rlst).eql([b, a]);
      done();
    })();
  });

  it('zkeys/zscan/zrscan/zclear', function(done){
    co(function*(){
      var z = uk('zset');
      var a = uk('key');
      var b = uk('key');
      yield client.zset(z, a, 12581);
      yield client.zset(z, b, 12582);
      var keys = yield client.zkeys(z, '', '', '', -1);
      var scan = yield client.zscan(z, '', '', '', -1);
      var rscan = yield client.zrscan(z, '', '', '', -1);
      var nums = yield client.zclear(z);
      should(keys).eql([a, b]);
      should(scan).eql([a, 12581, b, 12582]);
      should(rscan).eql([b, 12582, a, 12581]);
      should(nums).eql(2);
      should(yield client.zsize(z)).eql(0);
      done();
    })();
  });

  it('multi_zset/multi_zget/multi_zdel', function(done){
    co(function*(){
      var z = uk('zset');
      var k1 = uk();
      var k2 = uk();
      var k3 = uk();
      var a = yield client.multi_zset(z, k1, 1267, k2, 1268, k3, 1269);
      var b = yield client.multi_zget(z, k1, k2, k3);
      var c = yield client.multi_zdel(z, k1, k2, k3);
      should([a, c]).eql([3, 3]);
      should(b).eql([k1, 1267, k2, 1268, k3, 1269]);
      should(yield client.zsize(z)).eql(0);
      done();
    })();
  });

  it('zrange/zrrange/zrank/zrrank/zcount/zsum/zavg/zremrangeby[score|rank]', function(done){
    co(function*(){
      var z = uk('zset');
      var keys = [];
      var results = [];

      for (var i = 0; i < 10; i++) {
        var key = uk()
        keys.push(key);
        results.push(client.zset(z, key, i + 100));
      }
      yield results;
      var rank = client.zrank(z, keys[0]);  // 0
      var rrank = client.zrrank(z, keys[9]);  // 0
      should(yield [rank, rrank]).eql([0, 0]);

      var lst = client.zrange(z, 0, 2);
      var rlst = client.zrrange(z, 0, 2);
      should(yield lst).eql([keys[0], 100, keys[1], 101]);
      should(yield rlst).eql([keys[9], 109, keys[8], 108]);

      var sum = client.zsum(z, 100, 102);
      var avg = client.zavg(z, 100, 102);
      var count = client.zcount(z, 100, 101); // 2

      should(yield [sum, avg, count]).eql([303, 101, 2]);

      var numsr = client.zremrangebyrank(z, 0, 7);
      var numss = client.zremrangebyscore(z, 108, 109);  // 2

      should(yield [numsr, numss]).eql([8, 2]);

      should(yield client.zsize(z)).eql(0);
      done();
    })();
  });

  it('qpush[_back]/qpush_front/qfront/qback/qsize/qget/qpop[_front]/qpop_back/qclear', function(done){
    co(function*(){
      var q = uk('q');
      should(yield client.qpush(q, 1)).eql(1);  // qpush/qpush_back
      should(yield client.qpush_front(q, 2)).eql(2); // qpush_front
      should(yield client.qfront(q)).eql(2); // qfront
      should(yield client.qback(q)).eql(1);  // qback
      should(yield client.qsize(q)).eql(2);  // qsize
      should(yield client.qget(q, 1)).eql(1); // qget
      should(yield client.qget(q, 0)).eql(2); // qget
      should(yield client.qslice(q, 0, 3)).eql([2, 1]);
      should(yield client.qpop(q)).eql(2); // qpop_front/qpop
      should(yield client.qpop_back(q)).eql(1); // qpop_back
      should(yield client.qpush(q, 1)).eql(1);  // qpush/qpush_back
      should(yield client.qclear(q)).eql(1); // qclear
      done();
    })();
  });

  it('qlist/qrlist', function(done){
    co(function*(){
      var start = uk('q');
      var a = uk('q');
      var b = uk('q');
      yield client.qpush(a, 1)
      yield client.qpush(b, 1)
      var lst = yield client.qlist(start, uk('q'), -1);
      var rlst = yield client.qrlist(uk('q'), start, -1);
      should(lst).eql([a, b]);
      should(rlst).eql([b, a]);
      done();
    })();
  });

  it('parse large size response (issue#4)', function(done){
    co(function*(){
      var key = uk();
      var value = randomString(65535 * 3);
      yield client.set(key, value);
      var d = yield client.get(key);
      should(d).eql(value);
      done();
    })();
  });
    it('get a chinese value', function(done){
      co(function* (){
        var key = uk();
        var a = yield client.set(key, '中文测试');
        var b = yield client.get(key);
        should(b).eql('中文测试');
        done();
      })();
    });
});
