const redis = require('./lib/redis');
const assert = require('assert');
const util = require('./lib/util');
const flattenDeep = require('lodash.flattendeep');
const chunk = require('lodash.chunk');
const events = require('events');
const sleep = require('mz-modules/sleep');
const os = require('os');
class redisQueue extends redis {
  constructor(config, options) {
    super(config);
    this.streamName = options && options.streamName ? options.streamName : 'Stream'; // 队列名字
    this.groupName = options && options.groupName ? options.groupName : 'Queue'; // 消费组名
    this.consumerName = `${os.hostname()}:${process.pid}`; // 消费者名字
    this.pendingTime = options && options.pendingTime ? options.pendingTime : 15 * 60 * 1000; // 每15分钟扫描一次，进行消费者转移
    this.isCreatedGroup = false;
    this.init(); // 初始化
    this.checkPending();
  }
  /**
   * 创建消费组
   * @param {string} groupName 消费组名
   * @param {number} lastId 最后消费的ID
   */
  async init() {
    try {
      if (!this.isCreatedGroup) {
        await this.Client.xgroup('create', this.streamName, this.groupName, 0);
        this.isCreatedGroup = true;
      }
    } catch (error) {
      return;
    }
  }

  async checkPending() {
    const checkPendingWork = new events.EventEmitter();
    checkPendingWork.on('pending', async () => {
      await this.Client.pipeline()
        .hset('Consumer', this.consumerName, new Date().getTime())
        .expire('Consumer', this.pendingTime)
        .exec();
      const pendingInfos = await this.pending();
      for (const [ consumer, count ] of pendingInfos[3]) {
        const conPendingInfo = await this.Client.xpending(
          this.streamName,
          this.groupName,
          '-',
          '+',
          count,
          consumer
        );
        for (const [ peindId ] of conPendingInfo) {
          await this.Client.xclaim(
            this.streamName,
            this.groupName,
            this.consumerName,
            this.pendingTime,
            peindId
          );
        }
      }
      await sleep(this.pendingTime - 1000);
      checkPendingWork.emit('pending');
    });
    checkPendingWork.emit('pending');
  }

  async pub(message) {
    assert(util.isObject(message), 'the type of message  must be object');
    assert(Object.keys(message).length > 0, 'message is require');
    this.Client.xadd(this.streamName, '*', message);
  }

  async pending() {
    const pendingInfo = await this.Client.xpending(this.streamName, this.groupName);
    return pendingInfo;
  }

  async sub(count = 1, streamName = this.streamName) {
    const subInfo = await this.Client.xreadgroup(
      'group',
      this.groupName,
      this.consumerName,
      'count',
      count,
      'streams',
      streamName,
      '>'
    );
    return new Map(chunk(flattenDeep(subInfo), 2));
  }

  async xack(lastId) {
    try {
      console.log(lastId);
      await this.Client.xack(this.streamName, this.groupName, lastId);
      return true;
    } catch (error) {
      console.log(error);
      return false;
    }
  }
}

module.exports = redisQueue;
