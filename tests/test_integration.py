from __future__ import annotations

import os
import time
from uuid import uuid4

import pytest
from muscles_data.catalog import DataAdapterCatalog
from muscles_data.config import DataConfig
from muscles_data.ports import KeyValuePort, LockPort, StreamPort
from muscles_data.runtime import DataRuntime

from muscles_data.contracts import assert_key_value_contract, assert_lock_contract, assert_stream_contract
from muscles_data_redis import RedisDataFactory


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.getenv("MUSCLES_DATA_INTEGRATION"), reason="backend integration is disabled"),
]


def test_redis_real_kv_lock_stream_lifecycle():
    namespace = f"muscles-data-it-{uuid4().hex[:12]}"
    config = DataConfig.from_raw(
        {
            "data": {
                "resources": {
                    "cache.redis": {
                        "type": "redis",
                        "url_env": "REDIS_URL",
                        "namespace": namespace,
                        "stream_group": f"group-{namespace}",
                        "consumer": f"consumer-{namespace}",
                    }
                }
            }
        }
    )
    catalog = DataAdapterCatalog.with_defaults()
    catalog.register(RedisDataFactory())
    runtime = DataRuntime(config=config, catalog=catalog)

    try:
        key_value = runtime.require_port("cache.redis", KeyValuePort)
        lock = runtime.require_port("cache.redis", LockPort)
        stream = runtime.require_port("cache.redis", StreamPort)
        assert_key_value_contract(lambda: key_value)
        assert_lock_contract(lambda: lock)
        assert_stream_contract(lambda: stream)
        assert key_value.set("ttl", b"short", ttl_seconds=0.1).written == 1
        time.sleep(0.2)
        assert key_value.exists("ttl") is False
        assert runtime.doctor()["status"] == "ok"
    finally:
        runtime.close()
