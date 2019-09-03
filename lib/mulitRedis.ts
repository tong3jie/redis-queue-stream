import { RedisQueue, Config, Options } from './redisQueue';

export class MulitRedis {
  public Redis;

  constructor(config: Config, options: Options) {
    this.Redis = new Map();
    if (config.clients) {
      for (const [key, node] of Object.entries(config.clients)) {
        this.Redis.set(key, new RedisQueue(node, options));
      }
    }
  }
}
