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
RedisQ.pub({ msg1: 'msg1', msg2: 'msg2' });
const message = RedisQ.sub((count = 1), streamName);
RedisQ.xack();
```

## Configuration

**Single Client**

```
config = {
    port: 6379,          //  port
    host: '127.0.0.1',   //  host
    password: 'auth',
    db: 0,
}
```

**Multi Clients**

```
config = {
    foo: {                 // instanceName. See below
      port: 6379,          // Redis port
      host: '127.0.0.1',   // Redis host
      password: 'auth',
      db: 0,
    },
    bar: {
      port: 6379,
      host: '127.0.0.1',
      password: 'auth',
      db: 1,
    },
}
```

**Sentinel**

```
config = {
    sentinels: [{          // Sentinel instances
      port: 26379,         // Sentinel port
      host: '127.0.0.1',   // Sentinel host
    }],
    name: 'mymaster',      // Master name
    password: 'auth',
    db: 0
}
```

**Cluster**

```
config = {
     cluster: true,
     nodes: [{
       host: '127.0.0.1',
       port: '6379',
       family: 'user',
       password: 'password',
       db: 'db',
     }, {
       host: '127.0.0.1',
       port: '6380',
       family: 'user',
       password: 'password',
       db: 'db',
     }]
};
```

---

## Questions & Suggestions

Please open an issue [here](https://github.com/tong3jie/redis-queue-stream/issues).
