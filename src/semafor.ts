import { SemaforLimitExceededError, SemaforWaitTimeoutError } from './errors'

export interface SemaforConfig {
  /**
   * How long (in ms) we will wait to acquire a lock before rejecting.
   */
  wait?: number
  /**
   * The number of concurrent runs of the callback that can be executing at any one point.
   * Defaults to 1.
   */
  workers?: number
  /**
   * How long (in ms) the work can take, otherwise it will be canceled and the lock released.
   */
  limit?: number
}

export type ReleaseFn = () => Promise<void>

export type SemaforCallback<T = unknown> = (
  release: ReleaseFn,
  signal: AbortSignal
) => Promise<T> | T

export interface SemaforResult<T = unknown> {
  release: ReleaseFn
  result: T
}

export interface SemaphoreHandle {
  release: ReleaseFn
  acquire: <T = unknown>(callback: SemaforCallback<T>) => Promise<SemaforResult<T>>
  aquire: <T = unknown>(callback: SemaforCallback<T>) => Promise<SemaforResult<T>>
}

interface WaitQueueEntry {
  grant: (release: ReleaseFn) => void
  reject: (error: Error) => void
  timer?: ReturnType<typeof setTimeout>
}

class KeyedSemaphore {
  readonly key: string
  private maxWorkers: number
  private activeReleases = new Set<ReleaseFn>()
  private queue: WaitQueueEntry[] = []

  constructor(key: string, workers = 1) {
    this.key = key
    this.maxWorkers = Math.max(1, workers)
  }

  updateWorkers(workers?: number): void {
    if (workers !== undefined && workers >= 1) {
      this.maxWorkers = workers
      this.drain()
    }
  }

  get activeCount(): number {
    return this.activeReleases.size
  }

  get pendingCount(): number {
    return this.queue.length
  }

  acquireSlot(wait?: number): Promise<ReleaseFn> {
    if (this.activeReleases.size < this.maxWorkers && this.queue.length === 0) {
      return Promise.resolve(this.createLease())
    }

    return new Promise<ReleaseFn>((resolve, reject) => {
      const entry: WaitQueueEntry = {
        grant: resolve,
        reject,
      }

      if (wait !== undefined && wait >= 0) {
        entry.timer = setTimeout(() => {
          const index = this.queue.indexOf(entry)
          if (index !== -1) {
            this.queue.splice(index, 1)
          }
          reject(new SemaforWaitTimeoutError(this.key, wait))
        }, wait)
      }

      this.queue.push(entry)
    })
  }

  private createLease(): ReleaseFn {
    let released = false

    const release: ReleaseFn = async () => {
      if (released) {
        return
      }
      released = true
      this.activeReleases.delete(release)
      this.drain()
    }

    this.activeReleases.add(release)
    return release
  }

  private drain(): void {
    while (this.activeReleases.size < this.maxWorkers && this.queue.length > 0) {
      const next = this.queue.shift()
      if (!next) {
        break
      }
      if (next.timer !== undefined) {
        clearTimeout(next.timer)
      }
      const lease = this.createLease()
      next.grant(lease)
    }
  }

  async releaseAll(): Promise<void> {
    const active = Array.from(this.activeReleases)
    for (const release of active) {
      await release()
    }
  }
}

const registry = new Map<string, KeyedSemaphore>()

function getOrCreateSemaphore(key: string, workers?: number): KeyedSemaphore {
  let sem = registry.get(key)
  if (!sem) {
    sem = new KeyedSemaphore(key, workers)
    registry.set(key, sem)
  } else {
    sem.updateWorkers(workers)
  }
  return sem
}

async function executeWithSemaphore<T>(
  sem: KeyedSemaphore,
  config: SemaforConfig,
  callback: SemaforCallback<T>,
  onLeaseAcquired?: (release: ReleaseFn) => void
): Promise<SemaforResult<T>> {
  const leaseRelease = await sem.acquireSlot(config.wait)
  onLeaseAcquired?.(leaseRelease)

  const abortController = new AbortController()
  let limitTimer: ReturnType<typeof setTimeout> | undefined

  const wrappedRelease: ReleaseFn = async () => {
    if (limitTimer !== undefined) {
      clearTimeout(limitTimer)
      limitTimer = undefined
    }
    await leaseRelease()
  }

  try {
    const workPromise = Promise.resolve().then(() =>
      callback(wrappedRelease, abortController.signal)
    )

    let result: T
    if (config.limit !== undefined && config.limit >= 0) {
      const limitMs = config.limit
      const timeoutPromise = new Promise<never>((_resolve, reject) => {
        limitTimer = setTimeout(() => {
          abortController.abort(new SemaforLimitExceededError(sem.key, limitMs))
          void wrappedRelease()
          reject(new SemaforLimitExceededError(sem.key, limitMs))
        }, limitMs)
      })

      result = await Promise.race([workPromise, timeoutPromise])
    } else {
      result = await workPromise
    }

    return {
      release: wrappedRelease,
      result,
    }
  } catch (error) {
    await wrappedRelease()
    throw error
  } finally {
    if (limitTimer !== undefined) {
      clearTimeout(limitTimer)
      limitTimer = undefined
    }
  }
}

/**
 * Create a semaphore handle for a given key, or immediately acquire and run a callback.
 */
export function semafor(key: string, config?: SemaforConfig): SemaphoreHandle
export function semafor<T>(
  key: string,
  config: SemaforConfig,
  callback: SemaforCallback<T>
): Promise<SemaforResult<T>>
export function semafor<T>(
  key: string,
  config: SemaforConfig = {},
  callback?: SemaforCallback<T>
): Promise<SemaforResult<T>> | SemaphoreHandle {
  if (config.workers !== undefined && config.workers < 1) {
    throw new RangeError('workers must be at least 1')
  }
  if (config.wait !== undefined && config.wait < 0) {
    throw new RangeError('wait must be a non-negative number')
  }
  if (config.limit !== undefined && config.limit < 0) {
    throw new RangeError('limit must be a non-negative number')
  }

  const sem = getOrCreateSemaphore(key, config.workers)

  if (callback) {
    return executeWithSemaphore(sem, config, callback)
  }

  const handleReleases = new Set<ReleaseFn>()

  const acquire = async <R = unknown>(cb: SemaforCallback<R>): Promise<SemaforResult<R>> => {
    return executeWithSemaphore(sem, config, cb, (leaseRelease) => {
      handleReleases.add(leaseRelease)
    })
  }

  const release: ReleaseFn = async () => {
    if (handleReleases.size > 0) {
      const releases = Array.from(handleReleases)
      handleReleases.clear()
      for (const r of releases) {
        await r()
      }
    } else {
      await sem.releaseAll()
    }
  }

  return {
    release,
    acquire,
    aquire: acquire,
  }
}

export const semaphore = semafor

/**
 * Clears all registered semaphores. Useful for testing or resetting state.
 */
export function resetSemaphores(): void {
  registry.clear()
}
