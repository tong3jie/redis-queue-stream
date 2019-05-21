const redis = require('./lib/redis');
const assert = require('assert');
const Arraychunk = require('lodash.chunk');
const events = require('events');
const sleep = require('mz-modules/sleep');
const os = require('os');
class redisQueue extends redis {
  constructor(config, options) {
    super(config);
    this.streams = {};
    this.groupName = options && options.groupName ? options.groupName : 'Queue'; // 消费组名
    this.consumerName = `${os.hostname()}:${process.pid}`; // 消费者名字
    this.idleTime = options && options.idleTime ? options.idleTime : undefined; // 设置队列转移时间，超过该时间则进行重新入队
    this.pendingCount = options && options.pendingCount ? options.pendingCount : undefined; // 设置队列转移次数上限,超过该次数将进行丢弃
    this.pendingTime = options && options.pendingTime ? options.pendingTime : undefined; // 每15分钟扫描一次，进行消费者转移
    const processEvent = new events.EventEmitter();
    processEvent.on('start', async () => {
      await this.Client.multi()
        .setex(`Consumer:${this.consumerName}`, 60, new Date().getTime())
        .exec();
      await sleep(56 * 1000);
      processEvent.emit('start');
    });
    processEvent.emit('start');
  }

  /**
   * 初始化并创建消费组
   * @param {string} streamName 消费组名
   */
  async init({ streamName }) {
    const { Client } = this;
    try {
      const key = await Client.exists(streamName);
      if (!key) {
        await Client.xgroup('create', streamName, this.groupName, 0, 'mkstream');
      }
      this.streams[streamName] = true;

      this.pendingTime && this.checkPending();
    } catch (error) {
      this.pendingTime && this.checkPending();
      return;
    }
  }

  /**
   * 定期进行pending消息的转移
   * @param {string} streamName  流名称
   */
  async checkPending() {
    const { Client } = this;
    const checkPendingWork = new events.EventEmitter();
    checkPendingWork.on('pending', async () => {
      try {
        const streams = Object.keys(this.streams);
        const consumers = [];
        let scanIndex = 0;
        do {
          // eslint-disable-next-line no-bitwise
          const consumer = await Client.scan(~~scanIndex, 'match', 'Consumer:*', 'count', os.cpus().length * 2);
          scanIndex = consumer[0];
          consumers.push(...consumer[1]);
        } while (scanIndex !== '0');

        for (const streamName of streams) {
          const pendingInfos = await this.pending(streamName);
          if (pendingInfos[3]) {
            for (const [ consumer, count ] of pendingInfos[3]) {
              let time = 0;
              while (time < count) {
                const conPendingInfo = await Client.xpending(streamName, this.groupName, '-', '+', 10, consumer);
                for (const [ pendindId, , idleTime, pendingCount ] of conPendingInfo) {
                  if (!consumers.includes(`Consumer:${consumer}`)) {
                    await Client.xclaim(streamName, this.groupName, this.consumerName, idleTime - 1000, pendindId);
                  } else if (this.idleTime && idleTime > this.idleTime) {
                    await Client.xclaim(streamName, this.groupName, this.consumerName, this.idleTime, pendindId);
                    const messageInfo = await this.readById(streamName, pendindId);
                    this.pub(streamName, messageInfo);
                  } else if (this.pendingCount && this.pendingCount > pendingCount) {
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
        console.log(error);
        console.error(` checkPending error : ${error.stack}`);
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
      const streamId = await Client.xadd(streamName, '*', message);
      if (!this.streams[streamName]) {
        this.init({ streamName });
      }
      return streamId;
    } catch (error) {
      console.error(`${streamName} pub error : ${error.stack}`);
    }
  }

  /**
   * 获取消息内容
   * @param {number} count 获取数量，默认为1
   * @param {string} streamName 流名称
   */
  async sub({ count = 1, streamName }) {
    const { Client } = this;
    try {
      if (!this.streams[streamName]) {
        this.init({ streamName });
      }

      const subInfo = await Client.xreadgroup('group', this.groupName, this.consumerName, 'count', count || 1, 'streams', streamName, '>');
      if (!subInfo) return null;
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
   * @param {string} streamName  流名称
   * @param {string} messageId  消息ID
   */
  async xack({ streamName, messageId }) {
    const { Client } = this;
    try {
      await Client.xack(streamName, this.groupName, messageId);
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

module.exports = redisQueue;
