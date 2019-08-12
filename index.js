const RedisQueues = require('./lib/redis');
const assert = require('assert');
const Arraychunk = require('lodash.chunk');
const events = require('events');
const sleep = require('mz-modules/sleep');
const os = require('os');
class RedisQueue extends RedisQueues {
  constructor(config, options) {
    super(config);
    this.streams = {};
    this.maxlen = options.maxlen || 10000000;
    this.groupName = options && options.groupName ? options.groupName : 'Queue'; // 消费组名
    this.consumerName = `${os.hostname()}:${process.pid}`; // 消费者名字
    this.idleTime = options && options.idleTime ? options.idleTime : 0; // 设置队列转移时间，超过该时间则进行重新入队
    this.pendingCount = options && options.pendingCount ? options.pendingCount : 0; // 设置队列转移次数上限,超过该次数将进行丢弃
    this.pendingTime = options && options.pendingTime ? options.pendingTime : 0; // 每15分钟扫描一次，进行消费者转移
    const processEvent = new events.EventEmitter();
    processEvent.on('start', async () => {
      await this.Client.setex(`Consumer:${this.consumerName}`, 60, new Date().getTime());
      await sleep(56 * 1000);
      processEvent.emit('start');
    });
    processEvent.emit('start');
  }

  /**
   * 初始化并创建消费组
   * @param {string} streamName 消费组名
   */
  async streamInit(streamName) {
    const { Client } = this;
    try {
      const key = await Client.exists(streamName);
      // 判断redis中是否已经创建该stream
      if (key === 0) {
        await Client.xgroup('create', streamName, this.groupName, 0, 'mkstream');
      }
      this.streams[streamName] = true;
      this.checkPending();
    } catch (error) {
      this.checkPending();
      console.log('init:', error);
      return;
    }
  }

  /**
   * 定期进行pending消息的转移
   */
  async checkPending() {
    const checkPendingWork = new events.EventEmitter();
    checkPendingWork.on('pending', async () => {
      try {
        const streams = Object.keys(this.streams);
        const consumers = [];
        let scanIndex = 0;
        do {
          const consumer = await this.Client.scan(scanIndex, 'match', 'Consumer:*', 'count', os.cpus().length * 2);
          scanIndex = parseInt(consumer[0]);
          consumers.push(...consumer[1]);
        } while (scanIndex !== 0);

        for (const streamName of streams) {
          const pendingInfos = await this.pending(streamName);
          if (pendingInfos[3]) {
            for (const [ consumer, count ] of pendingInfos[3]) {
              let time = 0;
              while (time < count) {
                const conPendingInfo = await this.Client.xpending(streamName, this.groupName, '-', '+', 10, consumer);
                for (const [ pendindId, , idleTime, pendingCount ] of conPendingInfo) {
                  if (!consumers.includes(`Consumer:${consumer}`)) {
                    await this.Client.xclaim(streamName, this.groupName, this.consumerName, idleTime - 1000, pendindId);
                  } else if (this.idleTime && idleTime > this.idleTime) {
                    await this.Client.xclaim(streamName, this.groupName, this.consumerName, this.idleTime, pendindId);
                    const messageInfo = await this.readById(streamName, pendindId);
                    this.pub(streamName, messageInfo);
                  } else if (this.pendingCount && this.pendingCount < pendingCount) {
                    this.xack({ streamName, messageId: pendindId });
                  }
                }
                time = time + 10;
              }
            }
          }
        }
        await sleep(this.pendingTime - 1000);
        checkPendingWork.emit('pending');
      } catch (error) {
        console.error(` checkPending error : ${error.stack}`);
        await sleep(this.pendingTime - 1000);
        checkPendingWork.emit('pending');
      }
    });
    checkPendingWork.emit('pending');
  }

  /**
   * 获取队列中处于pending状态的消息
   * @param {string} streamName 流名称
   */
  async pending(streamName) {
    const { Client } = this;
    try {
      const pendingInfo = await Client.xpending(streamName, this.groupName);
      return pendingInfo;
    } catch (error) {
      console.error(`${streamName} pending error : ${error.stack}`);
    }
  }

  /**
   * 发布消息内容
   * @param {string} streamName  流名称
   * @param {object} message  消息内容
   */
  async pub(streamName, message) {
    const { Client } = this;
    assert(Object.prototype.toString.call(message) === '[object Object]', 'the type of message  must be object');
    assert(Object.keys(message).length > 0, 'message is require');
    try {
      if (!this.streams[streamName]) {
        await this.streamInit(streamName);
      }
      const streamId = await Client.xadd(streamName, 'maxlen', this.maxlen, '*', message);
      return streamId;
    } catch (error) {
      console.error(`${streamName} pub error : ${error.stack}`);
    }
  }

  /**
   * 获取消息内容
   * @param {streamName,count} streamName为操作的stranm名称，  count为获取的数量
   */
  async sub({ streamName, count = 1 }) {
    const { Client } = this;
    try {
      if (!this.streams[streamName]) {
        this.streamInit(streamName);
      }
      const subInfo = await Client.xreadgroup('group', this.groupName, this.consumerName, 'count', count, 'streams', streamName, '>');
      if (!subInfo) {
        return null;
      }
      let streamInfo = [];
      for (const [ key, value ] of subInfo[0][1]) {
        streamInfo.push({
          streamId: key,
          streamV: new Map(Arraychunk(value, 2)),
        });
      }
      streamInfo = streamInfo.length === 1 ? streamInfo[0] : streamInfo;
      return { streamName, streamInfo };
    } catch (error) {
      console.error(`${streamName} sub error : ${error.stack}`);
    }
  }

  /**
   * 消息确认
   * @param {streamName，messageId} streamName  流名称，messageId  消息ID
   */
  async xack({ streamName, messageId }) {
    const { Client } = this;
    try {
      await Client.multi()
        .xack(streamName, this.groupName, messageId)
        .xdel(streamName, messageId);
    } catch (error) {
      console.error(`${streamName} xack error : ${error.stack}`);
    }
  }

  /**
   * 根据ID获取消息内容
   * @param {string} streamName  流名称
   * @param {string} messageId   消息ID
   */
  async readById(streamName, messageId) {
    const { Client } = this;
    try {
      const streamInfo = await Client.xrange(streamName, messageId, messageId);
      const messageInfo = {};
      if (streamInfo.length > 0) {
        messageInfo[streamInfo[0][1][0]] = streamInfo[0][1][1];
      }
      return messageInfo;
    } catch (error) {
      console.error(`${streamName} readById error : ${error.stack}`);
    }
  }
}

module.exports = RedisQueue;
