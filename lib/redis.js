const ioredis = require('ioredis');
const assert = require('assert');
const convertObjectToArray = obj => {
  const result = [];
  for (const [ key, value ] of Object.entries(obj)) {
    result.push(key, value);
  }
  return result;
};
class redisQueue {
  constructor(config) {
    if (config.cluster === true) {
      assert(config.nodes && config.nodes.length !== 0, 'redis集群模式配置错误');
      config.nodes.forEach(item => {
        assert(
          item.host && item.port && item.password !== undefined && item.db !== undefined,
          `reids 'host: ${item.host}', 'port: ${item.port}', 'password: ${item.password}', 'db: ${
            item.db
          }' are required on config`
        );
      });

      this.Client = new ioredis.Cluster(config.nodes, config);
    } else if (config.sentinels) {
      assert(
        config.sentinels && config.sentinels.length !== 0,
        'redis sentinels configuration is required when use redis sentinel'
      );

      config.sentinels.forEach(sentinel => {
        assert(
          sentinel.host && sentinel.port,
          `[egg-redis] 'host: ${sentinel.host}', 'port: ${sentinel.port}' are required on config`
        );
      });

      assert(
        config.name && config.password !== undefined && config.db !== undefined,
        `redis 'name of master: ${config.name}', 'password: ${config.password}', 'db: ${
          config.db
        }' are required on config`
      );

      this.Client = new ioredis(config);
    } else {
      assert(
        config.host && config.port && config.password !== undefined && config.db !== undefined,
        `redis 'host: ${config.host}', 'port: ${config.port}', 'password: ${
          config.password
        }', 'db: ${config.db}' are required on config`
      );

      this.Client = new ioredis(config);
    }
    ioredis.Command.setArgumentTransformer('xadd', args => {
      if (args.length === 3) {
        if (typeof args[2] === 'object' && args[1] !== null) {
          return [ args[0] ].concat(args[1]).concat(convertObjectToArray(args[2]));
        }
      }
      return args;
    });
    this.Client.on('connect', () => {});
    this.Client.on('error', err => {
      console.log(err);
    });
  }
}
module.exports = redisQueue;
