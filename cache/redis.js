/* eslint-disable no-new-func */
const redis = require('redis');
const BaseCache = require('./base');

const DEQUEUE_SCRIPT = `
local queue = redis.call('ZREVRANGE', KEYS[1], 0, 0)[1]\n
if (queue) then\n
  redis.call('ZREM', KEYS[1], queue)\n
end\n
return queue\n
`;

// eslint-disable-next-line arrow-parens
const serialize = (obj) => {
  let o = obj;

  if (Array.isArray(obj)) {
    // eslint-disable-next-line arrow-parens
    o = obj.map((v) => serialize(v));
    // eslint-disable-next-line no-else-return
  } else if (obj && typeof obj === 'object') {
    const otemp = {};
    // eslint-disable-next-line arrow-parens
    Object.keys(obj).forEach((k) => {
      if (typeof obj[k] === 'function') {
        otemp.functionOptions = Array.isArray(otemp.functionOptions)
          ? otemp.functionOptions.push(k)
          : [k];

        obj[k] = obj[k].toString();
      } else if (Array.isArray(obj[k])) {
        otemp.functionOptions = Array.isArray(otemp.functionOptions)
          ? otemp.functionOptions.push(k)
          : [k];

        obj[k] = obj[k].map(v => {
          let out = v;

          if (Array.isArray(v)) {
            out = v.map(x => serialize(x));
          } else if (v && typeof v === 'object') {
            out = serialize(v);
          }

          return out;
        });
      }
    });
    o = { ...obj, ...otemp };
  }

  return o;
};

// eslint-disable-next-line arrow-parens
const deserialize = (obj) => {
  const convert = (element, args) => {
    const argsNames = args && Array.isArray(args) ? args : '';
    // eslint-disable-next-line no-extra-boolean-cast
    return !!argsNames
      ? new Function(
        ...argsNames,
        `return (${element})(${argsNames.join(',')})`,
      )
      : new Function(`return (${element})()`);
  };

  let o = obj;

  if (Array.isArray(obj)) {
    // eslint-disable-next-line arrow-parens
    o = obj.map((v) => deserialize(v));
    // eslint-disable-next-line no-else-return
  } else if (obj && typeof obj === 'object') {
    // eslint-disable-next-line arrow-parens
    (obj.functionOptions || []).forEach((k) => {
      if (Array.isArray(obj[k])) {
        obj[k] = obj[k].map((v, i) => convert(v, (obj[`${k}ArgsNames`] || [])[i]));
      } else if (typeof obj[k] === 'string') {
        obj[k] = convert(obj[k], obj[`${k}ArgsNames`]);
      }
    });

    o = { ...obj };
  }

  return o;
};

/**
 * @implements {BaseCache}
 */
class RedisCache extends BaseCache {
  /**
   * @override
   * @return {!Promise}
   */
  init() {
    this._client = redis.createClient(this._settings);
    return Promise.resolve();
  }

  /**
   * @return {!Promise}
   * @override
   */
  clear() {
    return new Promise((resolve, reject) => {
      // eslint-disable-next-line arrow-parens
      this._client.flushdb((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }

  /**
   * @return {!Promise}
   * @override
   */
  close() {
    this._client.quit();
    return Promise.resolve();
  }

  /**
   * @param {!string} key
   * @return {!Promise}
   * @override
   */
  get(key) {
    return new Promise((resolve, reject) => {
      this._client.get(key, (error, json) => {
        if (error) {
          reject(error);
          return;
        }
        try {
          let value = JSON.parse(json || null);
          value = deserialize(value);
          resolve(value);
        } catch (_error) {
          reject(_error);
        }
      });
    });
  }

  /**
   * @param {!string} key
   * @param {!string} value
   * @return {!Promise}
   * @override
   */
  set(key, value) {
    return new Promise((resolve, reject) => {
      let json;
      try {
        json = serialize(value);
        json = JSON.stringify(value);
      } catch (error) {
        reject(error);
        return;
      }
      // eslint-disable-next-line arrow-parens
      this._client.set(key, json, (error) => {
        if (error) {
          reject(error);
          return;
        }
        if (!this._settings.expire) {
          resolve();
          return;
        }
        // eslint-disable-next-line arrow-parens
        this._client.expire(key, this._settings.expire, (_error) => {
          if (_error) {
            reject(_error);
            return;
          }
          resolve();
        });
      });
    });
  }

  /**
   * @param {!string} key
   * @param {!string} value
   * @param {!number=} priority
   * @return {!Promise}
   * @override
   */
  enqueue(key, value, priority = 1) {
    return new Promise((resolve, reject) => {
      let json;
      try {
        json = serialize(value);
        json = JSON.stringify(value);
      } catch (error) {
        reject(error);
        return;
      }
      // eslint-disable-next-line arrow-parens
      this._client.zadd(key, priority, json, (error) => {
        if (error) {
          reject(error);
          return;
        }
        if (!this._settings.expire) {
          resolve();
          return;
        }
        // eslint-disable-next-line arrow-parens
        this._client.expire(key, this._settings.expire, (_error) => {
          if (_error) {
            reject(_error);
            return;
          }
          resolve();
        });
      });
    });
  }

  /**
   * @param {!string} key
   * @return {!Promise}
   * @override
   */
  dequeue(key) {
    return new Promise((resolve, reject) => {
      this._client.eval(DEQUEUE_SCRIPT, 1, key, (error, json) => {
        if (error) {
          reject(error);
          return;
        }
        try {
          let value = JSON.parse(json || null);
          value = deserialize(value);
          resolve(value);
        } catch (_error) {
          reject(_error);
        }
      });
    });
  }

  /**
   * @param {!string} key
   * @return {!Promise<!number>}
   * @override
   */
  size(key) {
    return new Promise((resolve, reject) => {
      this._client.zcount(key, '-inf', 'inf', (error, size) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(size || 0);
      });
    });
  }

  /**
   * @param {!string} key
   * @return {!Promise}
   * @override
   */
  remove(key) {
    return new Promise((resolve, reject) => {
      // eslint-disable-next-line arrow-parens
      this._client.del(key, (error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }
}

module.exports = RedisCache;
