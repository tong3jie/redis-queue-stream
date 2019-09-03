"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const assert = require("assert");
const os = require("os");
const events = require("events");
const Arraychunk = require("lodash.chunk");
const sleep = require("mz-modules/sleep");
const redis_1 = require("./redis");
class RedisQueue extends redis_1.RedisQueues {
    constructor(config, options) {
        super(config);
        this.streams = {};
        this.consumers = [];
        assert(options && options.queueNames && options.queueNames.length > 0, 'queueNames must be an array');
        this.queueNames = new Set(options.queueNames);
        this.maxlen = options.maxlen || 10000000;
        this.groupName = options.groupName || 'Queue'; // 消费组名
        this.expiryTime = options.expiryTime || 1000 * 60 * 1; // 设置队列转移时间，超过该时间则进行重新入队
        this.consumerName = `${os.hostname()}:${process.pid}`; // 消费者名字
        this.pendingCount = options.pendingCount || 2; // 设置队列转移次数上限,超过该次数将进行丢弃
        const processEvent = new events.EventEmitter();
        processEvent.on('start', () => __awaiter(this, void 0, void 0, function* () {
            yield this.Client.setex(`Consumer:${this.consumerName}`, 60, new Date().getTime());
            yield sleep(55 * 1000);
            processEvent.emit('start');
        }));
        processEvent.on('getConsumer', () => __awaiter(this, void 0, void 0, function* () {
            yield this.GetConsumers(config.cluster);
            yield sleep(55 * 1000);
            processEvent.emit('getConsumer');
        }));
        processEvent.emit('start');
        processEvent.emit('getConsumer');
        this.CheckPending();
    }
    /**
     * 初始化并创建消费组
     * @param streamName 消费组名
     */
    Init(streamName) {
        return __awaiter(this, void 0, void 0, function* () {
            const { Client } = this;
            try {
                const key = yield Client.exists(streamName);
                // 判断redis中是否已经创建该stream
                if (key === 0) {
                    yield Client.xgroup('create', streamName, this.groupName, 0, 'mkstream');
                }
                this.streams[streamName] = true;
            }
            catch (error) {
                console.log('streamInit error:', error);
            }
        });
    }
    /**
     * 获取所有的consumers
     */
    GetConsumers(Cluster) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                let scanIndex = 0;
                const consumers = [];
                if (Cluster) {
                    const clusterNodes = this.Client.nodes('master');
                    for (const node of clusterNodes) {
                        do {
                            const consumer = yield node.scan(scanIndex, 'match', 'Consumer:*', 'count', os.cpus().length * 2);
                            // TODO  判断consumer是否为有
                            scanIndex = parseInt(consumer[0], 10);
                            consumers.push(...consumer[1]);
                        } while (scanIndex !== 0);
                    }
                }
                else {
                    do {
                        const consumer = yield this.Client.scan(scanIndex, 'match', 'Consumer:*', 'count', os.cpus().length * 2);
                        // TODO  判断consumer是否为有
                        scanIndex = parseInt(consumer[0], 10);
                        consumers.push(...consumer[1]);
                    } while (scanIndex !== 0);
                }
                this.consumers = consumers;
            }
            catch (error) {
                console.log('GetConsumers error:', error);
            }
        });
    }
    /**
     * 定期进行pending消息的转移
     */
    CheckPending() {
        return __awaiter(this, void 0, void 0, function* () {
            const checkPendingWork = new events.EventEmitter();
            checkPendingWork.on('pending', () => __awaiter(this, void 0, void 0, function* () {
                try {
                    for (const queueName of this.queueNames) {
                        const pendingInfos = yield this.Pending(queueName);
                        if (pendingInfos[3]) {
                            for (const [consumer, count] of pendingInfos[3]) {
                                let time = 0;
                                while (time < count) {
                                    const conPendingInfo = yield this.Client.xpending(queueName, this.groupName, '-', '+', 10, consumer);
                                    for (const [pendindId, , idleTime, pendingCount] of conPendingInfo) {
                                        if (!this.consumers.includes(`Consumer:${consumer}`)) {
                                            // 转移已死进性未确认的消息
                                            yield this.Client.xclaim(queueName, this.groupName, this.consumerName, idleTime - 1000, pendindId);
                                        }
                                        else if (this.pendingCount && this.pendingCount < pendingCount) {
                                            this.Xack(queueName, pendindId);
                                        }
                                        else if (this.expiryTime && idleTime > this.expiryTime && consumer === this.consumerName) {
                                            yield this.Client.xclaim(queueName, this.groupName, this.consumerName, this.expiryTime, pendindId);
                                            const messageInfo = yield this.ReadById(queueName, pendindId);
                                            this.Pub(queueName, messageInfo);
                                        }
                                    }
                                    time += 10;
                                }
                            }
                        }
                    }
                    yield sleep(this.expiryTime - 1000);
                    checkPendingWork.emit('pending');
                }
                catch (error) {
                    console.error(` checkPending error : ${error.stack}`);
                    yield sleep(this.expiryTime - 1000);
                    checkPendingWork.emit('pending');
                }
            }));
            checkPendingWork.emit('pending');
        });
    }
    /**
     * 获取队列中处于pending状态的消息
     * @param queueName 队列名称
     */
    Pending(queueName) {
        return __awaiter(this, void 0, void 0, function* () {
            assert(this.queueNames.has(queueName), `not defined queueName:${queueName}`);
            const { Client } = this;
            try {
                const pendingInfo = yield Client.xpending(queueName, this.groupName);
                return pendingInfo;
            }
            catch (error) {
                console.error(`${queueName} pending error : ${error.stack}`);
            }
        });
    }
    /**
     *  入队
     * @param queueName 队列名称
     * @param message 消息内容荣
     */
    Pub(queueName, message) {
        return __awaiter(this, void 0, void 0, function* () {
            assert(this.queueNames.has(queueName), `not defined queueName:${queueName}`);
            const { Client } = this;
            assert(Object.prototype.toString.call(message) === '[object Object]', 'the type of message  must be object');
            assert(Object.keys(message).length > 0, 'message is require');
            try {
                if (!this.streams[queueName]) {
                    yield this.Init(queueName);
                }
                const streamId = yield Client.xadd(queueName, 'maxlen', this.maxlen, '*', message);
                return streamId;
            }
            catch (error) {
                console.error(`${queueName} pub error : ${error.stack}`);
            }
        });
    }
    /**
     *  出队
     * @param queueName 队列名称
     * @param count 获取的数量
     */
    Sub(queueName, count = 1) {
        return __awaiter(this, void 0, void 0, function* () {
            assert(this.queueNames.has(queueName), `not defined queueName:${queueName}`);
            const { Client } = this;
            try {
                if (!this.streams[queueName]) {
                    this.Init(queueName);
                }
                const subInfo = yield Client.xreadgroup('group', this.groupName, this.consumerName, 'count', count, 'streams', queueName, '>');
                if (!subInfo) {
                    return null;
                }
                const streamInfo = [];
                for (const [key, value] of subInfo[0][1]) {
                    streamInfo.push({
                        streamId: key,
                        streamV: new Map(Arraychunk(value, 2)),
                    });
                }
                const stream = streamInfo.length === 1 ? streamInfo[0] : streamInfo;
                return { queueName, messageInfo: stream };
            }
            catch (error) {
                console.error(`${queueName} sub error : ${error.stack}`);
            }
        });
    }
    /**
     *  消息确认
     * @param queueName 流名称
     * @param messageId 消息ID
     */
    Xack(queueName, messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            assert(this.queueNames.has(queueName), `not defined queueName:${queueName}`);
            const { Client } = this;
            try {
                yield Client.xack(queueName, this.groupName, messageId);
            }
            catch (error) {
                console.error(`${queueName} xack error : ${error.stack}`);
            }
        });
    }
    /**
     * 根据ID获取消息内容
     * @param queueName 流名称
     * @param messageId 消息ID
     */
    ReadById(queueName, messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            assert(this.queueNames.has(queueName), `not defined queueName:${queueName}`);
            const { Client } = this;
            try {
                const streamInfo = yield Client.xrange(queueName, messageId, messageId);
                const messageInfo = {};
                if (streamInfo.length > 0) {
                    const message = streamInfo[0][1];
                    for (let i = 0; i < message.length; i += 2) {
                        messageInfo[message[i]] = message[i + 1];
                    }
                }
                return messageInfo;
            }
            catch (error) {
                console.error(`${queueName} readById error : ${error.stack}`);
            }
        });
    }
    /**
     * 根据设定的超时时间，获取失败的消息内容
     * @param queueName 队列名称
     * @param count 一次获取的数量
     */
    ExpiryMsg(queueName, count = 1) {
        return __awaiter(this, void 0, void 0, function* () {
            const pendindIds = [];
            const pendingInfos = yield this.Pending(queueName);
            if (pendingInfos[3]) {
                for (const [consumer] of pendingInfos[3]) {
                    if (consumer === this.consumerName) {
                        const PendingInfo = yield this.Client.xpending(queueName, this.groupName, '-', '+', count, consumer);
                        for (const [pendindId, , idleTime] of PendingInfo) {
                            if (this.expiryTime && idleTime > this.expiryTime) {
                                pendindIds.push({ queueName, pendindId });
                            }
                        }
                    }
                }
            }
            return pendindIds;
        });
    }
    /**
     * 将过期的消息重新发布，超过失败次数的则直接丢弃
     * @param ids 过期的消息ID
     */
    RePub(ids, callBack) {
        return __awaiter(this, void 0, void 0, function* () {
            for (const id of ids) {
                const [queueName, pendindId] = Object.values(id);
                const messageInfo = yield this.ReadById(queueName, pendindId);
                this.Xack(queueName, pendindId);
                if (messageInfo.xcount < this.pendingCount) {
                    this.Pub(queueName, Object.assign(messageInfo, { xcount: messageInfo.xcount + 1 || 1 }));
                }
                else {
                    callBack();
                }
            }
        });
    }
}
exports.RedisQueue = RedisQueue;
