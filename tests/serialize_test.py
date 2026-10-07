from __future__ import annotations

import io
import re
import subprocess
import sys
from typing import Any
from unittest import mock

import numpy as np
import pandas as pd
import polars as pl
import pytest

from proxystore.serialize import _Data
from proxystore.serialize import _NumpySerializer
from proxystore.serialize import _PandasSerializer
from proxystore.serialize import _PolarsSerializer
from proxystore.serialize import _register_serializer
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize


@pytest.mark.parametrize(
    'obj',
    (
        [1, 2, 3],
        lambda: 42,  # Not picklable so cloudpickle is used.
        pd.DataFrame({'a': [1, 2, 3]}),
    ),
)
def test_pickle_protocol_is_fixed(obj: Any) -> None:
    # The protocol must not depend on the Python version so that data
    # can be read by every Python version ProxyStore supports.
    data = serialize(obj)
    _, pickled = data.split(b'\n', 1)
    # Protocol 2 and newer start with the PROTO opcode and the version.
    assert pickled[:2] == b'\x80\x05'


def test_register_duplicate_identifiers() -> None:
    class _TestSerializer:
        identifier = _NumpySerializer.identifier
        name = 'test'

        def supported(self, obj: Any) -> bool:
            raise NotImplementedError

        def serialize(self, obj: Any, buffer: io.BytesIO) -> None:
            raise NotImplementedError

        def deserialize(self, data: _Data) -> Any:
            raise NotImplementedError

    error = "Serializer named 'numpy' with identifier b'NP' already exists."
    with pytest.raises(AssertionError, match=error):
        _register_serializer(_TestSerializer)


@pytest.mark.parametrize(
    'obj',
    (
        b'binary-string',
        'normal-string',
        [1, 2, 3],
        np.array([[1, 2, 3], [4, 5, 6]]),
        pd.DataFrame([[1, 2, 3], [4, 5, 6]]),
        pl.DataFrame([[1, 2, 3], [4, 5, 6]]),
    ),
)
def test_serialize_objects(obj: Any) -> None:
    serialized = serialize(obj)
    deserialized = deserialize(serialized)

    if isinstance(obj, np.ndarray):
        assert np.array_equal(deserialized, obj)
    elif isinstance(obj, (pd.DataFrame, pl.DataFrame)):
        assert deserialized.equals(obj)
    else:
        assert deserialized == obj


@pytest.mark.parametrize('kind', (bytearray, memoryview))
@pytest.mark.parametrize(
    'obj',
    (
        b'binary-string',
        'normal-string',
        [1, 2, 3],
        np.array([[1, 2, 3], [4, 5, 6]]),
        pd.DataFrame([[1, 2, 3], [4, 5, 6]]),
        pl.DataFrame([[1, 2, 3], [4, 5, 6]]),
    ),
)
def test_deserialize_bytes_like(obj: Any, kind: type[Any]) -> None:
    deserialized = deserialize(kind(serialize(obj)))

    if isinstance(obj, np.ndarray):
        assert np.array_equal(deserialized, obj)
    elif isinstance(obj, (pd.DataFrame, pl.DataFrame)):
        assert deserialized.equals(obj)
    else:
        assert deserialized == obj
        assert type(deserialized) is type(obj)


def test_data_file() -> None:
    buffer = b'ID\nvalue'
    assert _Data(buffer, 3).file().read() == b'value'
    assert _Data(bytearray(buffer), 3).file().read() == b'value'


def test_serialize_lambda() -> None:
    b = serialize(lambda: [1, 2, 3])
    f = deserialize(b)
    assert f() == [1, 2, 3]


def test_deserialize_bad_input_type():
    with pytest.raises(ValueError, match='Expected data to be a bytes-like'):
        deserialize('non-bytes-input')  # type: ignore[arg-type]


def test_deserialize_bad_identifier():
    with pytest.raises(SerializationError):
        # No identifier
        deserialize(b'xxx')

    with pytest.raises(SerializationError):
        # Fake identifier
        deserialize(b'99\nxxx')

    with pytest.raises(SerializationError):
        # Valid identifier without a newline
        deserialize(b'BS')


def test_propagate_cloudpickle_dumps_error() -> None:
    with (
        mock.patch('cloudpickle.dump', side_effect=Exception()),
        pytest.raises(
            SerializationError,
            match=re.escape(
                "Object of type <class 'function'> is not supported.",
            ),
        ),
    ):
        serialize(lambda x: x + x)  # pragma: no cover


def test_propagate_pickle_loads_error() -> None:
    v = serialize([1, 2, 3])
    with mock.patch('pickle.loads', side_effect=Exception()):
        msg = 'Failed to deserialize object using the pickle serializer.'
        with pytest.raises(SerializationError, match=msg):
            deserialize(v)


def test_propagate_cloudpickle_loads_error() -> None:
    v = serialize(lambda x: x + x)  # pragma: no cover
    with mock.patch('cloudpickle.loads', side_effect=Exception()):
        msg = 'Failed to deserialize object using the cloudpickle serializer.'
        with pytest.raises(SerializationError, match=msg):
            deserialize(v)


def test_numpy_supported() -> None:
    serializer = _NumpySerializer()
    assert serializer.supported(np.array([1, 2, 3]))
    assert not serializer.supported([1, 2, 3])


def test_numpy_serializer() -> None:
    serializer = _NumpySerializer()
    xn = np.array([1, 2, 3])
    with io.BytesIO() as buffer:
        serializer.serialize(xn, buffer)
        deserialized = serializer.deserialize(_Data(buffer.getvalue()))
        assert np.array_equal(xn, deserialized)


def test_pandas_supported() -> None:
    serializer = _PandasSerializer()
    assert serializer.supported(pd.DataFrame({'a': [1, 2, 3]}))
    assert not serializer.supported({'a': [1, 2, 3]})


def test_pandas_serializer() -> None:
    serializer = _PandasSerializer()
    xp = pd.DataFrame({'a': [1, 2, 3]})
    with io.BytesIO() as buffer:
        serializer.serialize(xp, buffer)
        deserialized = serializer.deserialize(_Data(buffer.getvalue()))
        assert xp.equals(deserialized)


def test_polars_supported() -> None:
    serializer = _PolarsSerializer()
    assert serializer.supported(pl.DataFrame({'a': [1, 2, 3]}))
    assert not serializer.supported({'a': [1, 2, 3]})


def test_polars_serializer() -> None:
    serializer = _PolarsSerializer()
    xpl = pl.DataFrame({'a': [1, 2, 3]})
    with io.BytesIO() as buffer:
        serializer.serialize(xpl, buffer)
        deserialized = serializer.deserialize(_Data(buffer.getvalue()))
        assert xpl.equals(deserialized)


@pytest.mark.parametrize(
    ('serializer', 'module', 'obj'),
    (
        (_NumpySerializer(), 'numpy', np.array([1, 2, 3])),
        (_PandasSerializer(), 'pandas', pd.DataFrame({'a': [1, 2, 3]})),
        (_PolarsSerializer(), 'polars', pl.DataFrame({'a': [1, 2, 3]})),
    ),
)
def test_supported_module_not_imported(
    serializer: Any,
    module: str,
    obj: Any,
) -> None:
    with mock.patch.dict(sys.modules, {module: None}):
        assert not serializer.supported(obj)


def test_deserialize_module_not_installed() -> None:
    data = serialize(np.array([1, 2, 3]))
    with (
        mock.patch.dict(sys.modules, {'numpy': None}),
        pytest.raises(
            SerializationError,
            match='using the numpy serializer',
        ),
    ):
        deserialize(data)


def test_import_does_not_import_optional_modules() -> None:
    # Run in a subprocess because this test process has already imported
    # the modules.
    code = (
        'import sys\n'
        'import proxystore.serialize\n'
        "imported = {'numpy', 'pandas', 'polars'} & set(sys.modules)\n"
        'assert not imported, imported\n'
    )
    subprocess.run([sys.executable, '-c', code], check=True)
