/* eslint-disable no-new-func */
const redis = require("redis");
const serialize = require("serialize-javascript");
const BaseCache = require("./base");

const deserialize = (serializedJavascript) => {
  return eval("(" + serializedJavascript + ")");
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
    this._maxPriority = 0;
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
          const value = deserialize(json);
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
  enqueue(key, value, priority = 0) {
    if (this._maxPriority < priority) this._maxPriority = priority;

    return new Promise((resolve, reject) => {
      const newKey = `${key}:${priority}`;
      let json;
      try {
        json = serialize(value);
      } catch (error) {
        reject(error);
        return;
      }
      // eslint-disable-next-line arrow-parens
      if (this._settings.expire) {
        const m = this._client.multi();

        // eslint-disable-next-line arrow-parens
        m.rpush(newKey, json, (err) => {
          if (err) {
            reject(err);
          }
        });

        // eslint-disable-next-line arrow-parens
        m.expire(newKey, this._settings.expire, (err) => {
          if (err) {
            reject(err);
          }
        });

        // eslint-disable-next-line arrow-parens
        m.exec((err) => {
          if (err) {
            reject(err);
            return;
          }

          resolve();
        });
      } else {
        // eslint-disable-next-line arrow-parens
        this._client.rpush(newKey, json, (err) => {
          if (err) {
            reject(err);
            return;
          }

          resolve();
        });
      }
    });
  }

  /**
   * @param {!string} key
   * @return {!Promise}
   * @override
   */
  dequeue(key) {
    return new Promise((resolve, reject) => {
      const keys = [];
      // eslint-disable-next-line no-plusplus
      for (let i = this._maxPriority; i >= 0; i--) {
        keys.push(`${key}:${i}`);
      }

      this._client.blpop(...keys, 0.005, (err, json) => {
        if (err) {
          reject(err);
          return;
        }

        try {
          json = Array.isArray(json) && json.length === 2 ? json[1] : null;
          const value = deserialize(json);
          resolve(value);
        } catch (_err) {
          reject(_err);
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
      const m = this._client.multi();

      // eslint-disable-next-line no-plusplus
      for (let i = this._maxPriority; i >= 0; i--) {
        // eslint-disable-next-line arrow-parens
        m.llen(`${key}:${i}`, (err) => {
          if (err) {
            reject(err);
          }
        });
      }

      m.exec((err, results) => {
        if (err) {
          reject(err);
          return;
        }

        resolve(results.reduce((acc, curr) => acc + curr, 0));
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
