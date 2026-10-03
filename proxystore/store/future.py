"""Proxy-future interface implementation."""

from __future__ import annotations

import dataclasses
from typing import Any
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeVar

from proxystore.proxy import Proxy
from proxystore.serialize import BytesLike
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.store.types import ConnectorT
from proxystore.store.types import DeserializerT
from proxystore.store.types import SerializerT

if TYPE_CHECKING:
    from proxystore.store.factory import PollingStoreFactory

T = TypeVar('T')

# Prefix of exceptions set on a future. The prefix is checked before the
# (possibly custom) deserializer of the future is applied. This is part of
# the format of objects in a store so it must not change within a major
# version.
_EXCEPTION_PREFIX = b'proxystore.store.future.exception\n'


@dataclasses.dataclass(frozen=True)
class PollingPolicy:
    """Policy for polling a store until an object is available.

    Attributes:
        interval: Initial seconds to sleep between polling the store for the
            object.
        backoff_factor: Multiplicative factor applied to the interval after
            each unsuccessful poll.
        interval_limit: Maximum interval allowed. Prevents the backoff factor
            from increasing the interval to unreasonable values.
        timeout: Optional maximum number of seconds to poll for.
    """

    interval: float = 1
    backoff_factor: float = 1
    interval_limit: float | None = None
    timeout: float | None = None


@dataclasses.dataclass(frozen=True)
class _FutureException:
    exception: BaseException


def _serialize_exception(exception: Any) -> bytes:
    return _EXCEPTION_PREFIX + serialize(exception)


def _deserialize_with_exceptions(deserializer: DeserializerT) -> DeserializerT:
    def _deserialize(data: BytesLike) -> Any:
        view = memoryview(data).cast('B')
        prefix_length = len(_EXCEPTION_PREFIX)
        if view[:prefix_length] == _EXCEPTION_PREFIX:
            return _FutureException(deserialize(view[prefix_length:]))
        return deserializer(data)

    return _deserialize


class ProxyFuture(Generic[T]):
    """Future interface to a [`Store`][proxystore.store.base.Store].

    Tip:
        Create a [`ProxyFuture`][proxystore.store.future.ProxyFuture] with
        [`Store.future()`][proxystore.store.base.Store.future].

    Args:
        factory: Factory that can resolve the object once it is resolved.
            This factory should block when resolving until the object is
            available.
        serializer: Use a custom serializer when setting the result object
            of this future.
    """

    def __init__(
        self,
        factory: PollingStoreFactory[ConnectorT, T],
        *,
        serializer: SerializerT | None = None,
    ) -> None:
        self._factory = factory
        self._serializer = serializer

    def done(self) -> bool:
        """Check if the result or exception has been set yet."""
        return self._factory.get_store().exists(self._factory.key)

    def proxy(self) -> Proxy[T]:
        """Create a proxy which will resolve to the result of this future.

        If an exception is set on the future, resolving the proxy raises
        a [`ProxyResolveError`][proxystore.proxy.ProxyResolveError] caused by
        the exception.
        """
        return Proxy(self._factory)

    def result(self, timeout: float | None = None) -> T:
        """Get the result object of this future.

        Args:
            timeout: Maximum number of seconds to wait for the result. If
                `None`, the timeout of the
                [`PollingPolicy`][proxystore.store.future.PollingPolicy] of
                the future is used.

        Raises:
            TimeoutError: If the result is not available after `timeout`
                seconds.
            Exception: The exception set on the future with
                [`set_exception()`][proxystore.store.future.ProxyFuture.set_exception].
        """
        if timeout is None:
            timeout = self._factory.polling.timeout
        obj = self._factory._poll(timeout)
        if obj is None:
            raise TimeoutError(
                f'Result of the future was not available after {timeout} '
                'seconds.',
            )
        return obj[0]

    def set_exception(self, exception: BaseException) -> None:
        """Set the exception of this future.

        The exception is raised by
        [`result()`][proxystore.store.future.ProxyFuture.result] and when
        resolving the proxy of this future. The exception must be
        serializable with [`serialize()`][proxystore.serialize.serialize].

        Args:
            exception: Exception to raise.
        """
        self._factory.get_store()._set(
            self._factory.key,
            exception,
            serializer=_serialize_exception,
        )

    def set_result(self, obj: T) -> None:
        """Set the result object of this future.

        Args:
            obj: Result object.
        """
        self._factory.get_store()._set(
            self._factory.key,
            obj,
            serializer=self._serializer,
        )
