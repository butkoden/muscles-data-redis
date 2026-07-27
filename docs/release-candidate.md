# `muscles-data-redis` RC checklist

The package ships Redis implementations of `KeyValuePort`, `LockPort` and
`StreamPort`. The dependency on `muscles-data` is versioned as
`>=0.1.0,<1.0.0`.

Before publishing a GitHub Release, run:

```bash
PYTHONPATH=../muscles-data/src:src python -m pytest -q
python -m build --wheel --sdist
```

The integration scenario is enabled with `MUSCLES_DATA_INTEGRATION=1` and a
running Redis service configured through `REDIS_URL`. Lock ownership is
token-based and stream messages use the versioned framework envelope.

The PyPI workflow publishes only after a GitHub Release is published. It uses
the versioned `muscles-data` dependency and trusted publishing.
