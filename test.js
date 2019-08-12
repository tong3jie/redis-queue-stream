const redisQueue = require('./index');
const cluster = require('cluster');

(async function() {
  if (cluster.isMaster) {
    // 循环 fork 任务 CPU i5-7300HQ 四核四进程
    for (let i = 0; i < 6; i++) {
      cluster.fork();
    }
  } else {
    const queue = new redisQueue(
      {
        cluster: true,
        nodes: [
          {
            port: 8000,
            host: '127.0.0.1',
            password: '198312..3',
            db: 0,
          },
          {
            port: 8001,
            host: '127.0.0.1',
            password: '198312..3',
            db: 0,
          },
          {
            port: 8002,
            host: '127.0.0.1',
            password: '198312..3',
            db: 0,
          },
          {
            port: 8003,
            host: '127.0.0.1',
            password: '198312..3',
            db: 0,
          },
          {
            port: 8004,
            host: '127.0.0.1',
            password: '198312..3',
            db: 0,
          },
          {
            port: 8005,
            host: '127.0.0.1',
            password: '198312..3',
            db: 0,
          },
        ],
        natMap: {
          '172.18.0.2:8000': { host: '127.0.0.1', port: 8000 },
          '172.18.0.3:8001': { host: '127.0.0.1', port: 8001 },
          '172.18.0.4:8002': { host: '127.0.0.1', port: 8002 },
          '172.18.0.5:8003': { host: '127.0.0.1', port: 8003 },
          '172.18.0.6:8004': { host: '127.0.0.1', port: 8004 },
          '172.18.0.7:8005': { host: '127.0.0.1', port: 8005 },
        },
        scaleReads: 'slave',
        enableReadyCheck: false,
      },
      {
        streamName: 'send',
        groupName: 'sendGroup',
        pendingTime: 900000,
      }
    );

    for (let i = 0; i < 10000; i++) {
      console.time('ok');
      const messageId = await queue.pub('send', { a: 1, b: 2 });
      console.timeEnd('ok');
      console.log('messageId', messageId);
      console.time('ok1');
      const messageInfo = await queue.sub({ streamName: 'send' });
      console.timeEnd('ok1');
      // console.log('messageInfo', messageInfo);
      queue.xack({ streamName: 'send', messageId: messageInfo.streamInfo.streamId });
      // queue.sub({ streamName: 'send' }).then(msg => {
      //   console.log(msg);
      // });
    }
  }
})();
