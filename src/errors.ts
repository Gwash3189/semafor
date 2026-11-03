export class SemaforError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SemaforError'
    Object.setPrototypeOf(this, new.target.prototype)
  }
}

export class SemaforWaitTimeoutError extends SemaforError {
  readonly key: string
  readonly wait: number

  constructor(key: string, wait: number) {
    super(`Timed out waiting ${wait}ms to acquire semaphore "${key}"`)
    this.name = 'SemaforWaitTimeoutError'
    this.key = key
    this.wait = wait
  }
}

export class SemaforLimitExceededError extends SemaforError {
  readonly key: string
  readonly limit: number

  constructor(key: string, limit: number) {
    super(`Semaphore "${key}" work exceeded execution limit of ${limit}ms and was canceled`)
    this.name = 'SemaforLimitExceededError'
    this.key = key
    this.limit = limit
  }
}
