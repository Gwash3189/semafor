import { describe, expect, it } from 'vitest'
import {
  semafor,
  semaphore,
  resetSemaphores,
  SemaforWaitTimeoutError,
  SemaforLimitExceededError,
} from '../src'

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

describe('semafor', () => {
  describe('.semafor', () => {
    describe('when invoked with a callback directly', () => {
      it('executes the callback and returns the result', async () => {
        const key = 'direct-exec'
        const { result, release } = await semafor(
          key,
          { wait: 1000, workers: 1, limit: 1000 },
          async (innerRelease) => {
            try {
              return 'completed'
            } finally {
              await innerRelease()
            }
          }
        )
        await release()

        expect(result).toBe('completed')
      })

      it('allows releasing the semaphore from inside the callback', async () => {
        const key = 'inner-release'
        const events: string[] = []

        await semafor(key, { wait: 1000, workers: 1, limit: 1000 }, async (release) => {
          try {
            events.push('first')
          } finally {
            await release()
          }
        })

        await semafor(key, { wait: 1000, workers: 1, limit: 1000 }, async (release) => {
          try {
            events.push('second')
          } finally {
            await release()
          }
        })

        expect(events).toEqual(['first', 'second'])
      })

      it('allows forcing the semaphore release from outside the callback', async () => {
        const key = 'outer-release'
        let secondRan = false

        const { release } = await semafor(key, { wait: 1000, workers: 1 }, async () => {
          return 'held'
        })

        await release()

        await semafor(key, { wait: 100, workers: 1 }, async (innerRelease) => {
          try {
            secondRan = true
          } finally {
            await innerRelease()
          }
        })

        expect(secondRan).toBe(true)
      })

      it('handles multiple calls to release idempotently', async () => {
        const key = 'idempotent-release'
        let activeDuringSecond = 0

        const { release } = await semafor(key, { workers: 1 }, async (innerRelease) => {
          await innerRelease()
          await innerRelease()
        })
        await release()

        const p1 = semafor(key, { workers: 1 }, async (r1) => {
          activeDuringSecond++
          await delay(30)
          activeDuringSecond--
          await r1()
        })

        const p2 = semafor(key, { workers: 1 }, async (r2) => {
          const observed = activeDuringSecond
          await r2()
          return observed
        })

        const [, second] = await Promise.all([p1, p2])

        expect(second.result).toBe(0)
      })
    })

    describe('when bounding concurrency with workers', () => {
      it('limits concurrent executions to the configured worker count', async () => {
        const key = 'bounded-workers'
        let currentActive = 0
        let maxActive = 0

        const tasks = Array.from({ length: 6 }, () =>
          semafor(key, { wait: 2000, workers: 2, limit: 2000 }, async (release) => {
            try {
              currentActive++
              maxActive = Math.max(maxActive, currentActive)
              await delay(25)
            } finally {
              currentActive--
              await release()
            }
          })
        )

        await Promise.all(tasks)

        expect(maxActive).toBe(2)
      })

      it('isolates concurrency limits between different keys', async () => {
        let activeTotal = 0
        let maxActiveTotal = 0

        const taskA = semafor('key-a', { workers: 1 }, async (release) => {
          try {
            activeTotal++
            maxActiveTotal = Math.max(maxActiveTotal, activeTotal)
            await delay(30)
          } finally {
            activeTotal--
            await release()
          }
        })

        const taskB = semafor('key-b', { workers: 1 }, async (release) => {
          try {
            activeTotal++
            maxActiveTotal = Math.max(maxActiveTotal, activeTotal)
            await delay(30)
          } finally {
            activeTotal--
            await release()
          }
        })

        await Promise.all([taskA, taskB])

        expect(maxActiveTotal).toBe(2)
      })
    })

    describe('when waiting to acquire a lock exceeds wait timeout', () => {
      it('rejects with SemaforWaitTimeoutError', async () => {
        const key = 'wait-timeout'

        const { release } = await semafor(key, { workers: 1 }, async () => {
          // Intentionally do not call release inside callback so lock stays held
        })

        const timedOutAttempt = semafor(key, { wait: 25, workers: 1 }, async (innerRelease) => {
          await innerRelease()
        })

        await expect(timedOutAttempt).rejects.toBeInstanceOf(SemaforWaitTimeoutError)
        await release()
      })
    })

    describe('when work execution exceeds the limit timeout', () => {
      it('cancels the work and rejects with SemaforLimitExceededError', async () => {
        const key = 'limit-timeout'

        const slowWork = semafor(key, { wait: 500, workers: 1, limit: 25 }, async (release) => {
          try {
            await delay(150)
          } finally {
            await release()
          }
        })

        await expect(slowWork).rejects.toBeInstanceOf(SemaforLimitExceededError)
      })

      it('aborts the AbortSignal passed to the callback when limit is exceeded', async () => {
        const key = 'limit-abort-signal'
        let signalAborted = false

        try {
          await semafor(key, { wait: 500, workers: 1, limit: 20 }, async (_release, signal) => {
            signal.addEventListener('abort', () => {
              signalAborted = true
            })
            await delay(100)
          })
        } catch {
          // Expected timeout error
        }

        expect(signalAborted).toBe(true)
      })

      it('releases the semaphore slot when work is canceled by limit', async () => {
        const key = 'limit-releases-slot'
        let subsequentWorkRan = false

        try {
          await semafor(key, { wait: 500, workers: 1, limit: 20 }, async () => {
            await delay(200)
          })
        } catch {
          // Expected limit exceeded error
        }

        await semafor(key, { wait: 50, workers: 1, limit: 500 }, async (release) => {
          try {
            subsequentWorkRan = true
          } finally {
            await release()
          }
        })

        expect(subsequentWorkRan).toBe(true)
      })
    })

    describe('when the callback throws an uncaught error', () => {
      it('releases the semaphore slot and rethrows the error', async () => {
        const key = 'uncaught-error'
        let recovered = false

        try {
          await semafor(key, { workers: 1 }, async () => {
            throw new Error('boom')
          })
        } catch {
          // Ignore expected error
        }

        await semafor(key, { wait: 50, workers: 1 }, async (release) => {
          try {
            recovered = true
          } finally {
            await release()
          }
        })

        expect(recovered).toBe(true)
      })
    })

    describe('when invalid configuration is provided', () => {
      it('throws a RangeError when workers is less than 1', () => {
        expect(() => semafor('bad-workers', { workers: 0 })).toThrow(RangeError)
      })

      it('throws a RangeError when wait is negative', () => {
        expect(() => semafor('bad-wait', { wait: -1 })).toThrow(RangeError)
      })

      it('throws a RangeError when limit is negative', () => {
        expect(() => semafor('bad-limit', { limit: -1 })).toThrow(RangeError)
      })
    })
  })

  describe('.semaphore', () => {
    describe('when using the handle API with aquire', () => {
      it('acquires the lock and executes the callback', async () => {
        const { aquire } = semaphore('handle-aquire', { wait: 1000, workers: 1, limit: 1000 })
        let executed = false

        await aquire(async (release) => {
          try {
            executed = true
          } finally {
            await release()
          }
        })

        expect(executed).toBe(true)
      })

      it('forces release of held locks via the outer release method', async () => {
        const { release, aquire } = semaphore('handle-force-release', {
          wait: 500,
          workers: 1,
        })
        let secondAcquired = false

        await aquire(async () => {
          // Do not release inside callback
        })

        await release()

        await aquire(async (innerRelease) => {
          try {
            secondAcquired = true
          } finally {
            await innerRelease()
          }
        })

        expect(secondAcquired).toBe(true)
      })

      it('forces release of all locks on the key when called before handle aquire', async () => {
        const key = 'external-force-release'
        let acquiredAfterForce = false

        await semafor(key, { workers: 1 }, async () => {
          // Leave locked
        })

        const { release, acquire } = semaphore(key, { wait: 100, workers: 1 })
        await release()

        await acquire(async (innerRelease) => {
          try {
            acquiredAfterForce = true
          } finally {
            await innerRelease()
          }
        })

        expect(acquiredAfterForce).toBe(true)
      })
    })

    describe('when scaling workers up dynamically on an existing key', () => {
      it('drains queued waiters up to the new worker limit', async () => {
        const key = 'dynamic-scale'
        const completed: number[] = []

        const first = await semafor(key, { workers: 1 }, async () => {
          // Hold initial slot
        })

        const queued = semafor(key, { wait: 500, workers: 2 }, async (release) => {
          try {
            completed.push(2)
          } finally {
            await release()
          }
        })

        await queued
        await first.release()

        expect(completed).toEqual([2])
      })
    })
  })

  describe('.resetSemaphores', () => {
    describe('when called while semaphores exist', () => {
      it('clears the semaphore registry', async () => {
        const key = 'reset-key'
        let acquiredFresh = false

        await semafor(key, { workers: 1 }, async () => {
          // Hold lock without releasing
        })

        resetSemaphores()

        await semafor(key, { wait: 50, workers: 1 }, async (release) => {
          try {
            acquiredFresh = true
          } finally {
            await release()
          }
        })

        expect(acquiredFresh).toBe(true)
      })
    })
  })
})
