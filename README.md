# semafor

A lightweight, zero-dependency TypeScript library for bounding concurrent work using keyed semaphores, acquisition wait timeouts, and execution time limits.

## Installation

```bash
npm install semafor
```

## Usage

### Direct Execution API (`semafor`)

Acquire a lock for a given key and immediately execute a callback:

```ts
import { semafor } from 'semafor'

// wait: how long (in ms) we will wait to acquire a lock
// workers: the number of concurrent runs of the callback that can be going at any one point
// limit: how long (in ms) the work can take, otherwise it'll be canceled
const { release, result } = await semafor(
  'key',
  { wait: 5_000, workers: 35, limit: 2_500 },
  async (release, signal) => {
    try {
      // ... do work (optionally pass `signal` to fetch / async operations)
      return 'done'
    } catch (e) {
      // handle error
    } finally {
      await release() // releases access to the semaphore
    }
  }
)

// Calling await release() from outside the callback forces the semaphore to be released
await release()
```

### Handle API (`semaphore` / `semafor`)

Create a reusable handle for a given key and configuration:

```ts
import { semaphore } from 'semafor'

const { release, aquire, acquire } = semaphore('key', {
  wait: 5_000,
  workers: 35,
  limit: 2_500,
})

await aquire(async (release) => {
  try {
    // ... do work
  } finally {
    await release()
  }
})

// Force release from outside if needed
await release()
```

## Configuration

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `workers` | `number` | `1` | Maximum number of concurrent executions allowed for the given `key`. |
| `wait` | `number` | `undefined` | Maximum time in milliseconds to wait in queue to acquire a semaphore slot before throwing `SemaforWaitTimeoutError`. |
| `limit` | `number` | `undefined` | Maximum time in milliseconds the callback can run before being canceled, releasing its slot, and throwing `SemaforLimitExceededError`. |

## Errors

- `SemaforError`: Base error class for all `semafor` errors.
- `SemaforWaitTimeoutError`: Thrown when waiting for a lock exceeds `config.wait`.
- `SemaforLimitExceededError`: Thrown when callback execution exceeds `config.limit`.

## Development

```bash
npm test          # Run tests with Vitest
npm run lint      # Run linter with Oxlint
npm run type-check # Run TypeScript type checker
npm run build     # Build ESM + CJS bundles with tsdown
```

## License

MIT
