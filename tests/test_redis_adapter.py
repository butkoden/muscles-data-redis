from __future__ import annotations

from typing import Any

import pytest
from muscles_data.catalog import DataAdapterCatalog
from muscles_data.config import DataConfig
from muscles_data.models import DataCapability, LockHandle
from muscles_data.ports import KeyValuePort, LockPort, StreamPort
from muscles_data.runtime import DataRuntime

from muscles_data_redis import RedisClientMissingError, RedisConfigError, RedisConnectionError, RedisDataFactory


class FakeRedisClient:
    def __init__(self, *, fail_ping: bool = False) -> None:
        self.fail_ping = fail_ping
        self.values: dict[str, Any] = {}
        self.sets: list[dict[str, Any]] = []
        self.deletes: list[tuple[str, ...]] = []
        self.streams: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        self.xacks: list[dict[str, Any]] = []
        self.pings = 0
        self.closed = False

    def set(self, name: str, value, **kwargs):
        self.sets.append({"name": name, "value": value, **kwargs})
        if kwargs.get("nx") and name in self.values:
            return False
        self.values[name] = value
        return True

    def get(self, name: str):
        return self.values.get(name)

    def delete(self, *names: str) -> int:
        self.deletes.append(tuple(names))
        deleted = 0
        for name in names:
            deleted += 1 if self.values.pop(name, None) is not None else 0
        return deleted

    def exists(self, *names: str) -> int:
        return sum(1 for name in names if name in self.values)

    def eval(self, _script: str, _numkeys: int, key: str, expected_token: str):
        if self.values.get(key) != expected_token:
            return 0
        self.values.pop(key, None)
        return 1

    def xadd(self, name: str, fields: dict[str, Any]):
        stream = self.streams.setdefault(name, [])
        message_id = f"{len(stream) + 1}-0"
        stream.append((message_id, dict(fields)))
        return message_id

    def xread(self, streams: dict[str, str], count: int | None = None, block: int | None = None):
        del block
        output = []
        for name, cursor in streams.items():
            messages = [
                (message_id, fields)
                for message_id, fields in self.streams.get(name, [])
                if cursor in {"0", "0-0"} or message_id > cursor
            ]
            output.append((name, messages[:count]))
        return output

    def xack(self, name: str, groupname: str, *ids: str) -> int:
        self.xacks.append({"name": name, "groupname": groupname, "ids": ids})
        known = {message_id for message_id, _fields in self.streams.get(name, [])}
        return sum(1 for message_id in ids if message_id in known)

    def ping(self) -> bool:
        self.pings += 1
        if self.fail_ping:
            raise TimeoutError("redis password=redis-secret timed out")
        return True

    def close(self) -> None:
        self.closed = True


def _config() -> dict:
    return {
        "data": {
            "resources": {
                "cache.redis": {
                    "type": "redis",
                    "url": "redis://:redis-secret@localhost:6379/0",
                    "namespace": "app",
                    "stream_group": "workers",
                    "native_client": True,
                }
            }
        }
    }


def _runtime(client: FakeRedisClient | None):
    catalog = DataAdapterCatalog.with_defaults()
    catalog.register(RedisDataFactory(client_factory=lambda _config: client))
    return DataRuntime(config=DataConfig.from_raw(_config()), catalog=catalog)


def test_redis_external_adapter_maps_key_value_lock_stream_and_native_access():
    client = FakeRedisClient()
    runtime = _runtime(client)

    listed = runtime.list_resources()[0]
    assert listed["type"] == "redis"
    assert {"key_value", "cache", "lock", "stream"} <= set(listed["capabilities"])
    assert listed["initialized"] is False

    cache = runtime.require_port("cache.redis", KeyValuePort)
    assert cache.set("cursor", b"cursor-1", ttl_seconds=2.5).written == 1
    assert client.sets[0]["name"] == "app:cursor"
    assert client.sets[0]["px"] == 2500
    assert cache.get("cursor") == b"cursor-1"
    assert cache.exists("cursor") is True

    lock = runtime.require_port("cache.redis", LockPort)
    handle = lock.acquire_lock("daily-job", ttl_seconds=5)
    assert isinstance(handle, LockHandle)
    assert lock.release_lock(handle).deleted == 1

    stream = runtime.require_port("cache.redis", StreamPort)
    assert stream.publish("events", {"kind": "created"}).written == 1
    read = stream.read("events", limit=10)
    assert read.messages == [{"stream": "events", "id": "1-0", "fields": {"kind": "created"}}]
    assert stream.ack("events", "1-0").matched == 1

    native = runtime.require_resource("cache.redis", DataCapability.NATIVE_CLIENT).native_client()
    assert native is client
    assert runtime.doctor()["status"] == "ok"
    assert client.pings == 1
    assert runtime.close()["status"] == "ok"
    assert client.closed is True


def test_redis_external_adapter_reports_safe_failures():
    with pytest.raises(RedisClientMissingError):
        _runtime(None).require_port("cache.redis", KeyValuePort).get("cursor")

    failing = _runtime(FakeRedisClient(fail_ping=True)).doctor()
    assert failing["status"] == "failed"
    assert "redis-secret" not in repr(failing)

    bad_client = FakeRedisClient()
    bad_client.get = lambda _name: (_ for _ in ()).throw(RuntimeError("redis://:redis-secret@localhost unavailable"))
    with pytest.raises(RedisConnectionError):
        _runtime(bad_client).require_port("cache.redis", KeyValuePort).get("cursor")

    catalog = DataAdapterCatalog.with_defaults()
    catalog.register(RedisDataFactory(client_factory=lambda _config: FakeRedisClient()))
    unsupported = DataRuntime(
        config=DataConfig.from_raw({"data": {"resources": {"cache.redis": {"type": "redis", "url": "redis://localhost", "unsafe": True}}}}),
        catalog=catalog,
    )
    with pytest.raises(RedisConfigError, match="Unsupported Redis resource options"):
        unsupported.require_port("cache.redis", KeyValuePort).get("cursor")
