import { logger } from "../index.js";

/**
 * Retry utility
 */
async function withRetry(
  fn,
  { retries = 3, baseDelay = 500, maxDelay = 10_000, shouldRetry, onRetry }
) {
  let attempt = 0;

  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt++;

      if (attempt > retries || !shouldRetry(err)) {
        throw err;
      }

      const delay = Math.min(baseDelay * 2 ** (attempt - 1), maxDelay);

      onRetry?.(err, attempt, delay);

      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

/**
 * Simple throttle (token bucket)
 */
class Throttle {
  constructor({ limit, intervalMs }) {
    this.limit = limit;
    this.intervalMs = intervalMs;
    this.queue = [];
    this.active = 0;

    setInterval(() => {
      this.active = 0;
      this._process();
    }, intervalMs);
  }

  run(fn) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, resolve, reject });
      this._process();
    });
  }

  _process() {
    while (this.active < this.limit && this.queue.length > 0) {
      const { fn, resolve, reject } = this.queue.shift();
      this.active++;

      fn().then(resolve).catch(reject);
    }
  }
}

/**
 * Retry predicate (centralized)
 */
function isRetryableError(err) {
  const status = err?.response?.status;

  if (!status) return true; // network / timeout

  return [429, 500, 502, 503, 504].includes(status);
}

/**
 * Factory to create executor per integration
 */
function createRequestExecutor({ name, rateLimit, intervalMs, retries = 3 }) {
  const throttle = new Throttle({
    limit: rateLimit,
    intervalMs,
  });

  return async function execute(fn, meta = {}) {
    return throttle.run(() =>
      withRetry(fn, {
        retries,
        shouldRetry: isRetryableError,
        onRetry: (err, attempt, delay) => {
          logger.warn(`${name} retry`, {
            attempt,
            delay,
            status: err?.response?.status,
            ...meta,
          });
        },
      })
    );
  };
}

const throttle = (limit, intervalMs) => new Throttle({ limit, intervalMs });

export {
  Throttle,
  throttle,
  withRetry,
  isRetryableError,
  createRequestExecutor,
};
