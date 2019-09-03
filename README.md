a message queue by redis streams.
Supports Redis >= 5.0.0 and (Node.js >= 8).

# Quick Start

## Install

```shell
$ npm install redis-queue-stream
```

## Basic Usage

```javascript
const redisQueue = require('redis-queue-stream');
const RedisQ = new redisQueue(config, option);
RedisQ.Pub('send', { msg1: 'msg1', msg2: 'msg2' });
const message = RedisQ.Sub('send');
RedisQ.Xack('send', '1567505432681-0');
```

## Andvance

```javascript
const EventEmitter = require('event').EventEmitter;
const redisEvent = new EventEmitter();
const redisQueue = require('redis-queue-string').RedisQueue;
const RedisQ = new redisQueue(config, option);
RedisQ.Pub('send', { a: 1, b: 2 });
//  enqueue

redisEvent.on('pull', async () => {
  const message = RedisQ.Redis.get('foo').Sub('send');
  //  dequeue
  RedisQ.Redis.get('foo').Xack('send', message.messageInfo.streamId);
  //   confirm
  await sleep(5);
  redisEvent.emit('pull');
});

redisEvent.emit('pull');
```

## option

````javascript
{
  queueNames: ['send', 'notify'], //  all queueName
  maxlen:number  // queue length , default 1000000
  groupName:string  // default Queue
  expiryTime:number // outtime,default 15 munites
  pendingCount: number, // retry times,default 2 times
},

```
## Configuration

**Single Client**

```javascript

config = {
port: 6379, // port
host: '127.0.0.1', // host
password: 'auth',
db: 0,
}

````

**Multi Clients**

```javascript
config = {
  foo: {
    // instanceName. See below
    port: 6379, // Redis port
    host: '127.0.0.1', // Redis host
    password: 'auth',
    db: 0,
  },
  bar: {
    port: 6379,
    host: '127.0.0.1',
    password: 'auth',
    db: 1,
  },
};
```

**Sentinel**

```javascript
config = {
  sentinels: [
    {
      // Sentinel instances
      port: 26379, // Sentinel port
      host: '127.0.0.1', // Sentinel host
    },
  ],
  name: 'mymaster', // Master name
  password: 'auth',
  db: 0,
};
```

**Cluster**

```javascript
config = {
  cluster: true,
  nodes: [
    {
      host: '127.0.0.1',
      port: '6379',
      family: 'user',
      password: 'password',
      db: 'db',
    },
    {
      host: '127.0.0.1',
      port: '6380',
      family: 'user',
      password: 'password',
      db: 'db',
    },
  ],
};
```

---

## Questions & Suggestions

Please open an issue [here](https://github.com/tong3jie/redis-queue-stream/issues).

```

```
