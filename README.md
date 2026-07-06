# muscles-data-redis

Redis adapter package for `muscles-data`.

This package is intentionally separate from `muscles-data`: the core package
owns typed ports, resource runtime and diagnostics, while this package owns
Redis-backed `KeyValuePort`, `LockPort` and `StreamPort` implementations.

## Usage

Register the factory in the project composition root:

```python
from muscles_data.catalog import DataAdapterCatalog
from muscles_data.ports import KeyValuePort
from muscles_data.runtime import DataRuntime
from muscles_data_redis import RedisDataFactory

catalog = DataAdapterCatalog.with_defaults()
catalog.register(RedisDataFactory())

runtime = DataRuntime(config=config, catalog=catalog)
cache = runtime.require_port("cache.redis", KeyValuePort)
```

Resource config stays in the project:

```yaml
data:
  resources:
    cache.redis:
      type: redis
      url: ${REDIS_URL}
      namespace: app
      timeout: 3
      stream_group: workers
```

The adapter creates the Redis client lazily by key-value, lock, stream, explicit
native access or `data.doctor` operations. Application code should use
`KeyValuePort`, `LockPort` and `StreamPort`; direct client access is only an
advanced escape hatch with `native_client: true`.

See `muscular-example/example_data_redis_1` for an executable example.
