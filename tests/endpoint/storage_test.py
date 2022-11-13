from __future__ import annotations

import os
import pathlib

import pytest

from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.storage import Blob
from proxystore.endpoint.storage import BlobLocation
from proxystore.endpoint.storage import EndpointStorage
from proxystore.endpoint.storage import FileDumpNotAvailableError
from testing.compat import randbytes


def test_blob_in_memory_only() -> None:
    key = 'key'
    value = randbytes(100)
    blob = Blob(key, value)
    assert blob.location == BlobLocation.MEMORY
    assert blob.key == key
    assert blob.value == value
    assert blob.size == len(value)

    # No file so should do nothing
    blob.delete_file()
    # Already loaded so should do nothing
    blob.load()
    # No filepath was passed to constructor so should error
    with pytest.raises(FileDumpNotAvailableError):
        blob.dump()


def test_blob_filedump(tmp_path: pathlib.Path) -> None:
    os.makedirs(tmp_path, exist_ok=True)

    key = 'key'
    value = randbytes(100)
    filepath = os.path.join(tmp_path, key)

    blob = Blob(key, value, filepath)

    blob.dump()
    assert blob.location == BlobLocation.FILE
    # This is technically testing internal details but it is important
    # we check that we are not still storing data in memory
    assert blob._value is None

    # Accessing value should force a load
    blob.value
    assert blob.location == BlobLocation.MEMORY
    assert blob.value is not None
    assert not os.path.exists(filepath)


def test_endpoint_storage_acts_like_dict() -> None:
    storage = EndpointStorage()

    data = [(f'key{i}', f'value{i}'.encode()) for i in range(10)]
    for key, value in data:
        storage[key] = value
        assert storage[key] == value

    assert len(storage) == len(data)
    for key, _ in data:
        assert key in storage
    assert 'missingkey' not in storage

    key, value = data.pop()
    del storage[key]
    with pytest.raises(KeyError, match=key):
        storage[key]

    with pytest.raises(KeyError, match='missingkey'):
        del storage['missingkey']

    # Check storage is iterable
    for key in storage:
        assert isinstance(storage[key], bytes)

    storage.clear()
    assert len(storage) == 0

    # Should do nothing
    storage.cleanup()


def test_endpoint_storage_init_error() -> None:
    with pytest.raises(ValueError):
        EndpointStorage(max_size=100, dump_dir=None)

    with pytest.raises(ValueError):
        EndpointStorage(max_size=None, dump_dir='')


def test_endpoint_storage_dumping(tmp_path: pathlib.Path) -> None:
    storage = EndpointStorage(max_size=100, dump_dir=str(tmp_path))

    for i in range(20):
        storage[str(i)] = randbytes(10)

    # Keys 0-9 should be dumped to dir.
    files1 = [
        f
        for f in os.listdir(tmp_path)
        if os.path.isfile(os.path.join(tmp_path, f))
    ]
    assert len(files1) == 10

    # Access first 10 keys to bring them back into memory.
    # Should last 10 keys to be dumped to dir.
    for i in range(10):
        assert isinstance(storage[str(i)], bytes)

    files2 = [
        f
        for f in os.listdir(tmp_path)
        if os.path.isfile(os.path.join(tmp_path, f))
    ]
    assert len(files2) == 10
    assert len(set(files1) & set(files2)) == 0

    # Try deleting in memory and out of memory objects
    for i in range(5, 15):
        del storage[str(i)]

    # We don't bring objects back into memory when we delete and free up space
    files3 = [
        f
        for f in os.listdir(tmp_path)
        if os.path.isfile(os.path.join(tmp_path, f))
    ]
    assert len(files3) == 5

    storage.cleanup()
    assert not os.path.isdir(tmp_path)


def test_object_exceeds_memory(tmp_path: pathlib.Path) -> None:
    storage = EndpointStorage(max_size=100, dump_dir=str(tmp_path))
    with pytest.raises(ObjectSizeExceededError, match='memory limit'):
        storage['key'] = randbytes(200)


def test_object_exceeds_object_size(tmp_path: pathlib.Path) -> None:
    storage = EndpointStorage(max_object_size=100)
    with pytest.raises(ObjectSizeExceededError, match='object limit'):
        storage['key'] = randbytes(200)
