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
    // this.streamName = options && options.streamName ? options.streamName : 'Stream'; // 队列名字
    this.groupName = options && options.groupName ? options.groupName : 'Queue'; // 消费组名
    this.consumerName = `${os.hostname()}:${process.pid}`; // 消费者名字
    this.pendingTime = options && options.pendingTime ? options.pendingTime : undefined; // 每15分钟扫描一次，进行消费者转移
  }

  /**
   * 初始化并创建消费组
   * @param {string} streamName 消费组名
   */
  async init({ streamName }) {
    const { Client } = this;
    try {
      this.consumerName = `${os.hostname()}:${process.pid}`;
      const key = await Client.exists(streamName);
      if (!key) {
        await Client.xgroup('create', streamName, this.groupName, 0, 'mkstream');
      }
      this.streams[streamName] = true;
      if (this.pendingTime) {
        console.log('object');
      }
      this.pendingTime && this.checkPending(streamName);
    } catch (error) {
      this.pendingTime && this.checkPending(streamName);
      return;
    }
  }

  /**
   * 定期进行pending消息的转移
   * @param {string} streamName  流名称
   */
  async checkPending(streamName) {
    const { Client } = this;
    const checkPendingWork = new events.EventEmitter();
    checkPendingWork.on('pending', async () => {
      try {
        await Client.pipeline()
          .hset('Consumer', this.consumerName, new Date().getTime())
          .pexpire('Consumer', this.pendingTime)
          .exec();
        const pendingInfos = await this.pending(streamName);
        if (pendingInfos[3]) {
          for (const [ consumer, count ] of pendingInfos[3]) {
            const conPendingInfo = await Client.xpending(streamName, this.groupName, '-', '+', count, consumer);
            console.log('conPendingInfo', process.pid, conPendingInfo);
            for (const [ peindId ] of conPendingInfo) {
              await Client.xclaim(streamName, this.groupName, this.consumerName, this.pendingTime, peindId);
            }
          }
        }
        await sleep(this.pendingTime - 1000);
        console.log('xpending');
        checkPendingWork.emit('pending');
      } catch (error) {
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
      console.log('pendingInfo', streamName, pendingInfo);
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

      const subRes = {};
      subRes.streamName = subInfo[0][0];

      const streamInfo = [];

      for (const [ key, value ] of subInfo[0][1]) {
        streamInfo.push({
          streamId: key,
          streamV: new Map(Arraychunk(value, 2)),
        });
      }

      subRes.streamInfo = streamInfo.length === 0 ? streamInfo[0] : streamInfo;
      return subRes;
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
      return streamInfo.length > 0 ? streamInfo[0][1][1] : null;
    } catch (error) {
      console.error(`${streamName} readById error : ${error.stack}`);
    }
  }
}

module.exports = redisQueue;
