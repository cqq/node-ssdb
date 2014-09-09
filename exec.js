
var ssdb = require('./lib/ssdb');
var client = ssdb.createClient();

ret1 = client.batch();
//console.log(ret1);
client.zset("exec7", 'test1', 13);
client.zset("exec8", 'test12', 13);
client.zset("exec9", 'test12', 13);
//client.zset("test11111", 'test12', 13);
var ret = client.exec();
console.log(ret);
