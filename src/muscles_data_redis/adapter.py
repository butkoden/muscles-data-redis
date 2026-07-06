from __future__ import annotations

import importlib
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict
from typing import Any, Mapping
from urllib.parse import urlsplit

from muscles_data.config import DataResourceConfig
from muscles_data.errors import DataError
from muscles_data.models import DataCapability, HealthResult, InspectResult, LockHandle, StreamReadResult, WriteResult


_CLIENT_UNSET = object()
_ALLOWED_OPTIONS = {
    "url",
    "namespace",
    "decode_responses",
    "timeout",
    "socket_timeout",
    "socket_connect_timeout",
    "stream_group",
    "native_client",
}
_LOCK_RELEASE_SCRIPT = """
local token = redis.call('get', KEYS[1])
if not token or token ~= ARGV[1] then
    return 0
end
redis.call('del', KEYS[1])
return 1
"""


class RedisAdapterError(DataError):
    """Base error for Redis data adapter failures."""


class RedisConfigError(ValueError, RedisAdapterError):
    """Raised when a Redis resource config cannot be mapped safely."""


class RedisClientMissingError(RedisAdapterError):
    """Raised when Redis adapter is used without an available client."""


class RedisConnectionError(RedisAdapterError):
    """Raised when a Redis operation cannot reach or use the backend."""


class RedisDataAdapter:
    resource_type = "redis"

    def __init__(
        self,
        config: DataResourceConfig,
        *,
        client_factory: Callable[[DataResourceConfig], Any] | None = None,
    ) -> None:
        self.config = config
        self._client_factory = client_factory
        self._client: Any = _CLIENT_UNSET
        self._lock = threading.RLock()
        self.closed = False

    def get(self, key: str) -> bytes | None:
        try:
            value = self._client_instance().get(self._key(key))
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc
        return _bytes_or_none(value)

    def set(self, key: str, value: bytes, ttl_seconds: float | None = None) -> WriteResult:
        kwargs: dict[str, Any] = {}
        if ttl_seconds is not None:
            kwargs["px"] = _ttl_ms(ttl_seconds, "Redis key ttl_seconds")
        try:
            written = bool(self._client_instance().set(self._key(key), bytes(value), **kwargs))
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc
        return WriteResult(written=1 if written else 0, matched=1 if written else 0)

    def delete(self, key: str) -> WriteResult:
        try:
            deleted = int(self._client_instance().delete(self._key(key)) or 0)
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc
        return WriteResult(deleted=deleted, matched=deleted)

    def exists(self, key: str) -> bool:
        try:
            return bool(self._client_instance().exists(self._key(key)))
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc

    def acquire_lock(self, name: str, ttl_seconds: float):
        ttl_ms = _ttl_ms(ttl_seconds, "Redis lock ttl_seconds")
        token = uuid.uuid4().hex
        lock_key = self._lock_key(name)
        try:
            acquired = bool(self._client_instance().set(lock_key, token, nx=True, px=ttl_ms))
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc
        if not acquired:
            return None
        return LockHandle(name=str(name), token=token, expires_at=time.monotonic() + float(ttl_seconds))

    def release_lock(self, handle) -> WriteResult:
        if handle is None:
            return WriteResult()
        name = getattr(handle, "name", None)
        token = getattr(handle, "token", None)
        if not name or not token:
            raise RedisConfigError("Redis lock handle requires name and token")
        try:
            deleted = int(self._client_instance().eval(_LOCK_RELEASE_SCRIPT, 1, self._lock_key(str(name)), str(token)) or 0)
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc
        matched = 1 if deleted else 0
        return WriteResult(deleted=matched, matched=matched)

    def publish(self, stream: str, message: Mapping[str, Any]) -> WriteResult:
        try:
            self._client_instance().xadd(self._stream_key(stream), _stream_fields(message))
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc
        return WriteResult(written=1, matched=1)

    def read(self, stream: str, cursor: str | None = None, limit: int = 100) -> StreamReadResult:
        if limit <= 0:
            return StreamReadResult(messages=[], cursor=cursor)
        stream_name = str(stream)
        stream_key = self._stream_key(stream_name)
        try:
            response = self._client_instance().xread({stream_key: cursor or "0-0"}, count=max(0, int(limit)))
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc

        messages: list[dict[str, Any]] = []
        next_cursor = cursor
        for _raw_name, entries in response or []:
            for message_id, fields in entries:
                normalized_id = _text(message_id)
                messages.append(
                    {
                        "stream": stream_name,
                        "id": normalized_id,
                        "fields": _read_fields(fields),
                    }
                )
                next_cursor = normalized_id
        return StreamReadResult(messages=messages, cursor=next_cursor)

    def ack(self, stream: str, message_id: str) -> WriteResult:
        group = self.stream_group()
        try:
            matched = int(self._client_instance().xack(self._stream_key(stream), group, str(message_id)) or 0)
        except (RedisClientMissingError, RedisConfigError):
            raise
        except Exception as exc:
            raise RedisConnectionError(self._safe_error(exc)) from exc
        return WriteResult(matched=matched)

    def inspect(self) -> dict[str, Any]:
        return asdict(
            InspectResult(
                name=self.config.name,
                type=self.config.type,
                capabilities=[],
                initialized=self._client is not _CLIENT_UNSET,
                status="ok",
                options=self.config.safe_options(),
                details={
                    "backend": "redis",
                    "namespace_enabled": bool(self.namespace()),
                    "stream_group": self.stream_group(),
                },
            )
        )

    def health(self) -> HealthResult:
        try:
            ping = bool(self._client_instance().ping())
        except Exception as exc:
            return HealthResult(status="failed", message=self._safe_error(exc))
        if not ping:
            return HealthResult(status="failed", message="Redis ping failed")
        return HealthResult(status="ok", message="Redis connection is available")

    def close(self) -> None:
        if self._client is _CLIENT_UNSET:
            self.closed = True
            return
        close = getattr(self._client, "close", None)
        if callable(close):
            close()
        self.closed = True

    def native_client(self):
        return self._client_instance()

    def namespace(self) -> str:
        return str(self.config.options.get("namespace", "")).strip(":")

    def stream_group(self) -> str:
        group = str(self.config.options.get("stream_group", "default"))
        if not group:
            raise RedisConfigError("Redis stream_group must not be empty")
        return group

    def _key(self, key: str) -> str:
        normalized = _name(key, "Redis key")
        namespace = self.namespace()
        return f"{namespace}:{normalized}" if namespace else normalized

    def _lock_key(self, name: str) -> str:
        return self._key(f"lock:{_name(name, 'Redis lock name')}")

    def _stream_key(self, stream: str) -> str:
        return self._key(f"stream:{_name(stream, 'Redis stream name')}")

    def _client_instance(self):
        if self._client is _CLIENT_UNSET:
            with self._lock:
                if self._client is _CLIENT_UNSET:
                    self._validate_options()
                    client = (
                        self._client_factory(self.config)
                        if self._client_factory
                        else _default_redis_client(self.config)
                    )
                    if client is None:
                        raise RedisClientMissingError("Redis client is not available")
                    self._client = client
        return self._client

    def _validate_options(self) -> None:
        unknown = sorted(set(self.config.options) - _ALLOWED_OPTIONS)
        if unknown:
            names = ", ".join(unknown)
            raise RedisConfigError(f"Unsupported Redis resource options: {names}")
        if "url" not in self.config.options or not self.config.options["url"]:
            raise RedisConfigError("Redis resource requires url")
        for option in ("timeout", "socket_timeout", "socket_connect_timeout"):
            if option in self.config.options and float(self.config.options[option]) <= 0:
                raise RedisConfigError(f"Redis {option} must be positive")

    def _safe_error(self, exc: Exception) -> str:
        message = str(exc)
        url = str(self.config.options.get("url", ""))
        sensitive_values = {url}
        try:
            parsed = urlsplit(url)
            if parsed.username:
                sensitive_values.add(parsed.username)
            if parsed.password:
                sensitive_values.add(parsed.password)
        except Exception:  # pragma: no cover
            pass
        for value in sorted((item for item in sensitive_values if item), key=len, reverse=True):
            message = message.replace(value, "***")
        return message


class RedisDataFactory:
    resource_type = "redis"

    def __init__(self, *, client_factory: Callable[[DataResourceConfig], Any] | None = None) -> None:
        self._client_factory = client_factory

    def capabilities(self, config: DataResourceConfig) -> set[DataCapability]:
        native = {DataCapability.NATIVE_CLIENT} if bool(config.options.get("native_client")) else set()
        return {
            DataCapability.KEY_VALUE,
            DataCapability.CACHE,
            DataCapability.LOCK,
            DataCapability.STREAM,
            DataCapability.HEALTHCHECK,
        } | native

    def create(self, config: DataResourceConfig) -> RedisDataAdapter:
        return RedisDataAdapter(config, client_factory=self._client_factory)


def _default_redis_client(config: DataResourceConfig):
    try:
        redis_module = importlib.import_module("redis")
    except ImportError as exc:
        raise RedisClientMissingError("redis package is not installed; install muscles-data-redis") from exc
    redis_cls = getattr(redis_module, "Redis", None)
    from_url = getattr(redis_cls, "from_url", None) if redis_cls is not None else None
    if from_url is None:
        from_url = getattr(redis_module, "from_url", None)
    if from_url is None:
        raise RedisClientMissingError("redis package does not expose Redis.from_url")

    kwargs: dict[str, Any] = {
        "decode_responses": bool(config.options.get("decode_responses", False)),
    }
    timeout = config.options.get("timeout")
    if timeout is not None:
        kwargs["socket_timeout"] = float(timeout)
        kwargs["socket_connect_timeout"] = float(timeout)
    for option in ("socket_timeout", "socket_connect_timeout"):
        if option in config.options:
            kwargs[option] = float(config.options[option])
    return from_url(str(config.options["url"]), **kwargs)


def _ttl_ms(value: float, label: str) -> int:
    seconds = float(value)
    if seconds <= 0:
        raise RedisConfigError(f"{label} must be positive")
    return max(1, int(seconds * 1000))


def _name(value: str, label: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise RedisConfigError(f"{label} must not be empty")
    return normalized.strip(":")


def _bytes_or_none(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return str(value).encode("utf-8")


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _stream_fields(message: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in dict(message).items()}


def _read_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    return {_text(key): _decode_stream_value(value) for key, value in dict(fields).items()}


def _decode_stream_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value
