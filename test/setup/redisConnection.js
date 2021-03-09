var Redis = require('ioredis');

var redis = module.exports = new Redis();

before(function(done){
  this.redis = redis;
  if(redis.connected) return done();
  else redis.on('ready', done);
});
