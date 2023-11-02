"""Utilities for client interactions with endpoints."""
from __future__ import annotations

import uuid

import requests
from requests.exceptions import RequestException  # noqa: F401

from proxystore.endpoint.constants import MAX_CHUNK_LENGTH
from proxystore.utils.data import chunk_bytes


def evict(
    address: str,
    key: str,
    endpoint: uuid.UUID | str | None = None,
    session: requests.Session | None = None,
) -> None:
    """Evict the object associated with the key.

    Args:
        address: Address of endpoint.
        key: Key associated with object to evict.
        endpoint: Optional UUID of remote endpoint to forward operation to.
        session: Session instance to use for making the request. Reusing the
            same session across multiple requests to the same host can improve
            performance.

    Raises:
        RequestException: If the endpoint request results in an unexpected
            error code.
    """
    endpoint_str = (
        str(endpoint) if isinstance(endpoint, uuid.UUID) else endpoint
    )
    post = requests.post if session is None else session.post
    response = post(
        f'{address}/evict',
        params={'key': key, 'endpoint': endpoint_str},
    )
    if not response.ok:
        raise requests.exceptions.RequestException(
            f'Endpoint returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )


def exists(
    address: str,
    key: str,
    endpoint: uuid.UUID | str | None = None,
    session: requests.Session | None = None,
) -> bool:
    """Check if an object associated with the key exists.

    Args:
        address: Address of endpoint.
        key: Key potentially associated with stored object.
        endpoint: Optional UUID of remote endpoint to forward operation to.
        session: Session instance to use for making the request. Reusing the
            same session across multiple requests to the same host can improve
            performance.

    Returns:
        If an object associated with the key exists.

    Raises:
        RequestException: If the endpoint request results in an unexpected
            error code.
    """
    endpoint_str = (
        str(endpoint) if isinstance(endpoint, uuid.UUID) else endpoint
    )
    get_ = requests.get if session is None else session.get
    response = get_(
        f'{address}/exists',
        params={'key': key, 'endpoint': endpoint_str},
    )
    if not response.ok:
        raise requests.exceptions.RequestException(
            f'Endpoint returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )
    return response.json()['exists']


def get(
    address: str,
    key: str,
    endpoint: uuid.UUID | str | None = None,
    session: requests.Session | None = None,
) -> bytes | None:
    """Get the serialized object associated with the key.

    Args:
        address: Address of endpoint.
        key: Key associated with object to retrieve.
        endpoint: Optional UUID of remote endpoint to forward operation to.
        session: Session instance to use for making the request. Reusing the
            same session across multiple requests to the same host can improve
            performance.

    Returns:
        Serialized object or `None` if the object does not exist.

    Raises:
        RequestException: If the endpoint request results in an unexpected
            error code.
    """
    endpoint_str = (
        str(endpoint) if isinstance(endpoint, uuid.UUID) else endpoint
    )
    get_ = requests.get if session is None else session.get
    response = get_(
        f'{address}/get',
        params={'key': key, 'endpoint': endpoint_str},
        stream=True,
    )

    # Status code 404 is only returned if there's no data associated with the
    # provided key.
    if response.status_code == 404:
        return None

    if not response.ok:
        raise requests.exceptions.RequestException(
            f'Endpoint returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )

    data = bytearray()
    for chunk in response.iter_content(chunk_size=None):
        data += chunk
    return bytes(data)


def put(
    address: str,
    key: str,
    data: bytes,
    endpoint: uuid.UUID | str | None = None,
    session: requests.Session | None = None,
) -> None:
    """Put a serialized object in the store.

    Args:
        address: Address of endpoint.
        key: Key associated with object to retrieve.
        data: Serialized data to put in the store.
        endpoint: Optional UUID of remote endpoint to forward operation to.
        session: Session instance to use for making the request. Reusing the
            same session across multiple requests to the same host can improve
            performance.

    Raises:
        RequestException: If the endpoint request results in an unexpected
            error code.
    """
    endpoint_str = (
        str(endpoint) if isinstance(endpoint, uuid.UUID) else endpoint
    )
    post = requests.post if session is None else session.post
    response = post(
        f'{address}/set',
        headers={'Content-Type': 'application/octet-stream'},
        params={'key': key, 'endpoint': endpoint_str},
        data=chunk_bytes(data, MAX_CHUNK_LENGTH),
        stream=True,
    )
    if not response.ok:
        raise requests.exceptions.RequestException(
            f'Endpoint returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )
