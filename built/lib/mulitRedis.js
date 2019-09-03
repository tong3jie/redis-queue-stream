"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const redisQueue_1 = require("./redisQueue");
class MulitRedis {
    constructor(config, options) {
        this.Redis = new Map();
        if (config.clients) {
            for (const [key, node] of Object.entries(config.clients)) {
                this.Redis.set(key, new redisQueue_1.RedisQueue(node, options));
            }
        }
    }
}
exports.MulitRedis = MulitRedis;
