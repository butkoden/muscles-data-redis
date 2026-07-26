# muscles-data-redis

Redis adapter package for `muscles-data`.

This package is intentionally separate from `muscles-data`: the core package
owns typed ports, resource runtime and diagnostics, while this package owns
Redis-backed `KeyValuePort`, `LockPort` and `StreamPort` implementations.

## Related packages

- Core runtime and port contracts:
  [`muscles-data`](https://github.com/butkoden/muscles-data)
- Elasticsearch search adapter:
  [`muscles-data-elasticsearch`](https://github.com/butkoden/muscles-data-elasticsearch)
- OpenSearch search adapter:
  [`muscles-data-opensearch`](https://github.com/butkoden/muscles-data-opensearch)
- Qdrant vector adapter:
  [`muscles-data-qdrant`](https://github.com/butkoden/muscles-data-qdrant)
- MongoDB document-store adapter:
  [`muscles-data-mongodb`](https://github.com/butkoden/muscles-data-mongodb)
- S3 object-store adapter:
  [`muscles-data-s3`](https://github.com/butkoden/muscles-data-s3)
- SQLAlchemy direct SQL resource adapter:
  [`muscles-data-sqlalchemy`](https://github.com/butkoden/muscles-data-sqlalchemy)
- Executable example:
  [`example_data_redis_1`](https://github.com/butkoden/muscular-example/tree/master/example_data_redis_1)

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
      url_env: REDIS_URL
      namespace: app
      timeout: 3
      stream_group: workers
      consumer: index-worker
```

The adapter creates the Redis client lazily by key-value, lock, stream, explicit
native access or `data.doctor` operations. Application code should use
`KeyValuePort`, `LockPort` and `StreamPort`; direct client access is only an
advanced escape hatch with `native_client: true`.

Streams use `XADD`, `XREADGROUP` and `XACK`. The configured consumer group is
created on first read and messages use the versioned
`muscles.data.message.v1` envelope. Retry and dead-letter decisions stay with
the application worker.

See `muscular-example/example_data_redis_1` for an executable example.
