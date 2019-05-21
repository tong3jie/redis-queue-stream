const redisQueue = require('./index');
const queue = new redisQueue(
  {
    port: 6379,
    host: '127.0.0.1',
    family: 4,
    password: 'auth',
    db: 0,
  },
  {
    streamName: 'send',
    groupName: 'sendGroup',
    pendingTime: 900000,
  }
);

queue.pub({ a: 1, b: 2 });
queue.sub(1).then(msg => {
  console.log(msg);
});
