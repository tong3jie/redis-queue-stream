const cluster = require('cluster');
const os = require('os');
const { RedisQueue } = require('./built/index');

(async function () {
  if (cluster.isMaster) {
    // 循环 fork 任务 CPU i5-7300HQ 四核四进程
    for (let i = 0; i < os.cpus().length; i++) {
      cluster.fork();
    }
  } else {
    const queue = new RedisQueue(
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
          '172.18.0.6:8000': { host: '127.0.0.1', port: 8000 },
          '172.18.0.7:8001': { host: '127.0.0.1', port: 8001 },
          '172.18.0.2:8002': { host: '127.0.0.1', port: 8002 },
          '172.18.0.4:8003': { host: '127.0.0.1', port: 8003 },
          '172.18.0.3:8004': { host: '127.0.0.1', port: 8004 },
          '172.18.0.5:8005': { host: '127.0.0.1', port: 8005 },
        },
        scaleReads: 'slave',
        enableReadyCheck: false,
      },
      {
        queueNames: ['send'],
        groupName: 'sendGroup',
      },
    );

    for (let i = 0; i < 100; i++) {
      // console.time('ok');
      const messageId = await queue.Pub('send', { a: 1, b: 2 });
      console.timeEnd('ok');
      // console.log('messageId', messageId);
      // console.time('ok1');
      const messageInfo = await queue.Sub('send');
      console.log(messageInfo);
      // console.timeEnd('ok1');
      // queue.Xack('send', messageInfo.messageInfo.streamId);
      // const l = await queue.ReadById('send', messageInfo.messageInfo.streamId);
      // console.log(l);
      // queue.sub({ streamName: 'send' }).then(msg => {
      //   console.log(msg);
      // });
    }
  }
}());
