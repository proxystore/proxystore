"""FileStore Unit Tests"""
import json
import numpy as np

from pytest import raises, warns

import proxystore as ps
from proxystore.store.globus import GlobusEndpoint
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusStore
from proxystore.store.globus import GlobusFactory

EP1 = GlobusEndpoint(
    uuid="4e55b15a-5c33-11ec-b2c1-1b99bfd4976a",
    endpoint_path="/~/Globus/proxystore-test",
    local_path="/mnt/c/Users/jgpaul/Documents/Globus/proxystore-test",
    host_regex="Alderaan",
)
EP2 = GlobusEndpoint(
    uuid="af7bda53-6d04-11e5-ba46-22000b92c6ec",
    endpoint_path="/~/proxystore-test",
    local_path="/home/jgpauloski/proxystore-test",
    host_regex="midway2-login1.rcc.local",
)
EPS = GlobusEndpoints(EP1, EP2)


def test_globus_endpoint_objects() -> None:
    """Test GlobusEndpoint(s) Objects"""
    with raises(TypeError):
        epa = GlobusEndpoint(
            uuid=1, endpoint_path="1", local_path="1", host_regex="1"
        )
    with raises(TypeError):
        epa = GlobusEndpoint(
            uuid="1", endpoint_path=1, local_path="1", host_regex="1"
        )
    with raises(TypeError):
        epa = GlobusEndpoint(
            uuid="1", endpoint_path="1", local_path=1, host_regex="1"
        )
    with raises(TypeError):
        epa = GlobusEndpoint(
            uuid="1", endpoint_path="1", local_path="1", host_regex=1
        )

    epa = GlobusEndpoint(
        uuid="1", endpoint_path="/path", local_path="/path", host_regex="host1"
    )
    epb = GlobusEndpoint(
        uuid="2",
        endpoint_path="/path",
        local_path="/path",
        host_regex=r"^\w{4}2$",
    )
    epc = GlobusEndpoint(
        uuid="1", endpoint_path="/path", local_path="/path", host_regex="host1"
    )
    assert epa != epb
    assert epa == epc

    # Check must pass at least one endpoint
    with raises(ValueError):
        GlobusEndpoints()
    with raises(ValueError):
        GlobusEndpoints([])

    # Check not able to pass multiple endpoints same UUID
    with raises(ValueError):
        GlobusEndpoints([epa, epc])

    eps = GlobusEndpoints([epa, epb])
    assert len(eps) == 2
    eps = GlobusEndpoints(epa, epb)
    assert len(eps) == 2

    assert eps[epa.uuid] == epa
    with raises(KeyError):
        assert eps["3"]

    for x, y in zip([epa, epb], eps):
        assert x == y

    assert eps.get_by_host("host1") == epa
    assert eps.get_by_host("host2") == epb
    with raises(ValueError):
        eps.get_by_host("host2_")
    with raises(ValueError):
        eps.get_by_host("host3")


def test_globus_endpoints_from_json() -> None:
    """Test GlobusEndpoints from JSON file"""
    data = {
        'UUID1': {
            'endpoint_path': '/~/',
            'local_path': '/home/user1/',
            'host_regex': 'host1',
        },
        'UUID2': {
            'endpoint_path': '/~/',
            'local_path': '/home/user2/',
            'host_regex': 'host2',
        },
    }
    filepath = '/tmp/endpoints.json'
    with open(filepath, 'w') as f:
        f.write(json.dumps(data))

    endpoints = GlobusEndpoints.from_json(filepath)

    assert len(endpoints) == 2
    assert endpoints['UUID1'].endpoint_path == '/~/'
    assert endpoints['UUID1'].local_path == '/home/user1/'
    assert endpoints['UUID1'].host_regex == 'host1'
    assert endpoints['UUID2'].endpoint_path == '/~/'
    assert endpoints['UUID2'].local_path == '/home/user2/'
    assert endpoints['UUID2'].host_regex == 'host2'


def test_globus_store_init() -> None:
    """Test GlobusStore Initialization"""
    GlobusStore('globus', endpoints=[EP1, EP2])

    ps.store.init_store(ps.store.STORES.GLOBUS, 'globus', endpoints=[EP1, EP2])
    ps.store.init_store(ps.store.STORES.GLOBUS, 'globus', endpoints=EPS)

    with raises(ValueError):
        # Negative cache_size error
        ps.store.init_store(
            ps.store.STORES.GLOBUS,
            'globus',
            endpoints=EPS,
            cache_size=-1,
        )

    with raises(ValueError):
        # Invalid endpoint type
        ps.store.init_store(
            ps.store.STORES.GLOBUS,
            'globus',
            endpoints=None,
            cache_size=-1,
        )


def test_globus_store_base() -> None:
    """Test GlobusStore Base Functionality"""
    store = GlobusStore('globus', endpoints=EPS, timeout=120)
    value = 'test_value'
    fakekey = "fakekey"

    # GlobusStore.set()
    key1 = store.set(str.encode(value))
    key2 = store.set(value)
    key3 = store.set(lambda: value)
    key4 = store.set(np.array([1, 2, 3]))

    # GlobusStore.get()
    assert store.get(key1) == str.encode(value)
    assert store.get(key2) == value
    assert store.get(key3).__call__() == value
    assert store.get(fakekey) is None
    assert store.get(fakekey, default='alt_value') == 'alt_value'
    assert np.array_equal(store.get(key4), np.array([1, 2, 3]))

    # GlobusStore.exists()
    assert store.exists(key1)
    assert store.exists(key2)
    assert store.exists(key3)
    assert not store.exists(fakekey)

    # GlobusStore.is_cached()
    assert store.is_cached(key1)
    assert store.is_cached(key2)
    assert store.is_cached(key3)
    assert not store.is_cached(fakekey)

    # GlobusStore.evict()
    store.evict(key2)
    assert not store.exists(key2)
    assert not store.is_cached(key2)
    store.evict(fakekey)

    store.cleanup()


def test_globus_store_caching() -> None:
    """Test GlobusStore Caching"""
    store = GlobusStore('globus', endpoints=EPS, timeout=120, cache_size=1)

    # Add our test value
    value = 'test_value'
    key1 = store.set(value)

    # Test caching
    assert not store.is_cached(key1)
    assert store.get(key1) == value
    assert store.is_cached(key1)

    # Add second value
    key2 = store.set(value)
    assert store.is_cached(key1)
    assert not store.is_cached(key2)

    # Check cached value flipped since cache size is 1
    assert store.get(key2) == value
    assert not store.is_cached(key1)
    assert store.is_cached(key2)

    # Now test cache size 0
    store = GlobusStore('globus', endpoints=EPS, cache_size=0)
    key1 = store.set(value)
    assert store.get(key1) == value
    assert not store.is_cached(key1)

    store.cleanup()


def test_globus_store_strict() -> None:
    """Test GlobusStore Strict Guarentees"""
    store = GlobusStore('globus', endpoints=EPS, cache_size=1)

    key = store.set('value')
    # GlobusStore objects are immutable so we raise a warning if the user
    # specifies strict=True
    store.get(key, strict=False)
    with warns(Warning):
        store.get(key, strict=True)

    store.cleanup()


def test_globus_store_custom_serialization() -> None:
    """Test Globus Store Custom Serialization"""
    store = GlobusStore('globus', endpoints=EPS, cache_size=1)

    # Pretend serialized string
    s = b'ABC'
    key = store.set(s, serialize=False)
    assert store.get(key, deserialize=False) == s

    with raises(Exception):
        # Should fail because the numpy array is not already serialized as bytes
        store.set(np.array([1, 2, 3]), serialize=False)

    store.cleanup()


def test_globus_factory() -> None:
    """Test GlobusFactory"""
    store = ps.store.init_store(
        ps.store.STORES.GLOBUS, 'globus', endpoints=EPS, timeout=120
    )
    key = store.set([1, 2, 3])

    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}

    f = GlobusFactory(
        key, 'globus', store_kwargs={'endpoints': EPS, 'sync_level': "mtime"}
    )
    assert f() == [1, 2, 3]

    # Test eviction when factory is called
    f2 = GlobusFactory(
        key,
        'globus',
        store_kwargs={'endpoints': EPS, 'sync_level': "mtime"},
        evict=True,
    )
    assert store.exists(key)
    assert f2() == [1, 2, 3]
    assert not store.exists(key)

    key = store.set([1, 2, 3])
    # Clear store to see if factory can reinitialize it async
    ps.store._stores = {}
    f = GlobusFactory(
        key,
        'globus',
        store_kwargs={'endpoints': EPS, 'sync_level': "mtime"},
    )
    f.resolve_async()
    assert f._obj_future is not None
    assert f() == [1, 2, 3]
    assert f._obj_future is None

    # Calling resolve_async should be no-op since value cached
    f.resolve_async()
    assert f._obj_future is None
    assert f() == [1, 2, 3]

    f_str = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_str)
    assert f() == [1, 2, 3]

    store.cleanup()


def test_globus_store_proxy() -> None:
    """Test GlobusStore Proxying"""
    store = ps.store.init_store(
        ps.store.STORES.GLOBUS, 'globus', endpoints=EPS, timeout=120
    )

    p = store.proxy([1, 2, 3])
    assert isinstance(p, ps.proxy.Proxy)

    assert p == [1, 2, 3]
    assert store.get(ps.proxy.get_key(p)) == [1, 2, 3]

    with raises(TypeError):
        # GlobusStore.proxy() requires obj be passed
        p2 = store.proxy(key=ps.proxy.get_key(p))
        assert p2 == [1, 2, 3]

    store.proxy([2, 3, 4], key='key')

    with raises(TypeError):
        # At least one of key or object must be passed
        store.proxy()

    with raises(Exception):
        # Array will not be serialized and should raise error when putting
        # array into Redis
        store.proxy(np.ndarray([1, 2, 3]), serialize=False)

    store.cleanup()


def test_proxy_recreates_store() -> None:
    """Test GlobusStore Proxy with FileFactory can Recreate the Store"""
    store = ps.store.init_store(
        ps.store.STORES.GLOBUS,
        'globus',
        endpoints=EPS,
        timeout=300,
        cache_size=0,
    )

    p = store.proxy([1, 2, 3])

    # Force delete store so proxy recreates it when resolved
    ps.store._stores = {}

    # Resolve the proxy
    assert p == [1, 2, 3]

    # The store that created the proxy had cache_size=0 so the restored
    # store should also have cache_size=0.
    key = ps.proxy.get_key(p)
    assert not ps.store.get_store('globus').is_cached(key)

    # Repeat above but with cache_size=1
    store = ps.store.init_store(
        ps.store.STORES.GLOBUS, 'globus', endpoints=EPS, cache_size=1
    )
    p = store.proxy([1, 2, 3])
    ps.store._stores = {}
    assert p == [1, 2, 3]
    key = ps.proxy.get_key(p)
    assert ps.store.get_store('globus').is_cached(key)

    store.cleanup()
