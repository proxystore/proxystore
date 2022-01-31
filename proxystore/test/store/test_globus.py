"""Globus Store Functionality Tests."""
import json
import os
import re

import globus_sdk
from pytest import fixture
from pytest import raises
from pytest import warns

import proxystore as ps
from proxystore.store.globus import GlobusEndpoint
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusStore
from proxystore.test.store.utils import GLOBUS_STORE
from proxystore.test.store.utils import mock_third_party_libs


EP1 = GlobusEndpoint(
    uuid="1",
    endpoint_path="/path",
    local_path="/path",
    host_regex="localhost",
)
EP2 = GlobusEndpoint(
    uuid="2",
    endpoint_path="/path",
    local_path="/path",
    host_regex="localhost",
)
EP3 = GlobusEndpoint(
    uuid="3",
    endpoint_path="/path",
    local_path="/path",
    host_regex="localhost",
)
EP4 = GlobusEndpoint(
    uuid="4",
    endpoint_path="/path",
    local_path="/path",
    host_regex=r"^\w{4}4$",
)
EP5 = GlobusEndpoint(
    uuid="1",
    endpoint_path="/path",
    local_path="/path",
    host_regex="localhost",
)


@fixture(scope="session", autouse=True)
def init() -> None:
    """Monkeypatch Globus and Parsl."""
    mpatch = mock_third_party_libs()
    yield mpatch
    mpatch.undo()


def test_globus_endpoint_objects() -> None:
    """Test GlobusEndpoint(s) Objects."""
    with raises(TypeError):
        GlobusEndpoint(
            uuid=1,
            endpoint_path="1",
            local_path="1",
            host_regex="1",
        )
    with raises(TypeError):
        GlobusEndpoint(
            uuid="1",
            endpoint_path=1,
            local_path="1",
            host_regex="1",
        )
    with raises(TypeError):
        GlobusEndpoint(
            uuid="1",
            endpoint_path="1",
            local_path=1,
            host_regex="1",
        )
    with raises(TypeError):
        GlobusEndpoint(
            uuid="1",
            endpoint_path="1",
            local_path="1",
            host_regex=1,
        )

    # GlobusEndpoint equality done by UUID
    assert EP1 != EP2
    assert EP1 == EP5

    # Check must pass at least one endpoint
    with raises(ValueError):
        GlobusEndpoints([])

    # Check not able to pass multiple endpoints same UUID
    with raises(ValueError):
        GlobusEndpoints([EP1, EP5])

    eps = GlobusEndpoints([EP1, EP2, EP3, EP4])
    assert len(eps) == 4

    assert eps[EP1.uuid] == EP1
    with raises(KeyError):
        assert eps["-1"]

    for x, y in zip([EP1, EP2], eps):
        assert x == y

    assert eps.get_by_host("localhost") == EP1
    assert eps.get_by_host("host4") == EP4
    with raises(ValueError):
        eps.get_by_host("host2_")
    with raises(ValueError):
        eps.get_by_host("host3")


def test_globus_endpoints_from_json() -> None:
    """Test GlobusEndpoints from JSON file."""
    data = {
        "UUID1": {
            "endpoint_path": "/~/",
            "local_path": "/home/user1/",
            "host_regex": "host1",
        },
        "UUID2": {
            "endpoint_path": "/~/",
            "local_path": "/home/user2/",
            "host_regex": "host2",
        },
    }
    filepath = "/tmp/endpoints-2458984621396.json"
    with open(filepath, "w") as f:
        f.write(json.dumps(data))

    endpoints = GlobusEndpoints.from_json(filepath)

    os.remove(filepath)

    assert len(endpoints) == 2
    assert endpoints["UUID1"].endpoint_path == "/~/"
    assert endpoints["UUID1"].local_path == "/home/user1/"
    assert endpoints["UUID1"].host_regex == "host1"
    assert endpoints["UUID2"].endpoint_path == "/~/"
    assert endpoints["UUID2"].local_path == "/home/user2/"
    assert endpoints["UUID2"].host_regex == "host2"


def test_globus_endpoints_from_dict() -> None:
    """Test GlobusEndpoints from JSON file."""
    data = {
        "UUID1": {
            "endpoint_path": "/~/",
            "local_path": "/home/user1/",
            "host_regex": "host1",
        },
        "UUID2": {
            "endpoint_path": "/~/",
            "local_path": "/home/user2/",
            "host_regex": "host2",
        },
    }
    endpoints = GlobusEndpoints.from_dict(data)
    assert endpoints.dict() == data

    # Ensure Patterns are converted to strings in .dict()
    data["UUID1"]["host_regex"] = re.compile("host1")
    endpoints = GlobusEndpoints.from_dict(data)
    assert isinstance(endpoints.dict()["UUID1"]["host_regex"], str)


def test_globus_store_init() -> None:
    """Test GlobusStore Initialization."""
    eps = GlobusEndpoints([EP1, EP2])

    GlobusStore("globus", endpoints=[EP1, EP2])

    s1 = ps.store.init_store(
        ps.store.STORES.GLOBUS,
        "globus",
        endpoints=[EP1, EP2],
    )
    s2 = ps.store.init_store(ps.store.STORES.GLOBUS, "globus", endpoints=eps)
    s3 = ps.store.init_store(
        ps.store.STORES.GLOBUS,
        "globus",
        endpoints=eps.dict(),
    )
    assert s1.kwargs == s2.kwargs == s3.kwargs

    with raises(ValueError):
        # Negative cache_size error
        ps.store.init_store(
            ps.store.STORES.GLOBUS,
            "globus",
            endpoints=eps,
            cache_size=-1,
        )

    with raises(ValueError):
        # Invalid endpoint type
        ps.store.init_store(
            ps.store.STORES.GLOBUS,
            "globus",
            endpoints=None,
        )

    with raises(ValueError):
        # Too many endpoints
        ps.store.init_store(
            ps.store.STORES.GLOBUS,
            "globus",
            endpoints=[EP1, EP2, EP3],
        )

    with raises(ValueError):
        # Not enough endpoints
        ps.store.init_store(
            ps.store.STORES.GLOBUS,
            "globus",
            endpoints=[EP1],
        )


def test_kwargs() -> None:
    """Test FileFactory kwargs."""
    store = GlobusStore("globus", **GLOBUS_STORE["kwargs"])
    full_kwargs = {
        **GLOBUS_STORE["kwargs"],
        "polling_interval": store.polling_interval,
        "sync_level": store.sync_level,
        "timeout": store.timeout,
        "cache_size": store.cache_size,
    }
    # store.kwargs returns endpoints as a dict rather than GlobusEndpoints
    full_kwargs["endpoints"] = full_kwargs["endpoints"].dict()
    assert store.kwargs == full_kwargs
    store.cleanup()


def test_globus_store_internals(monkeypatch) -> None:
    """Test GlobusStore internal mechanisms."""
    store = GlobusStore("globus", **GLOBUS_STORE["kwargs"])

    with warns(Warning):
        # Check that warning for not supporting strict is raised
        store.get("key", strict=True)

    class PatchedTransferClient400:
        def get_task(self, *args, **kwargs):
            class PatchedError(globus_sdk.TransferAPIError):
                def __init__(self):
                    self.http_status = 400

            raise PatchedError()

    store._transfer_client = PatchedTransferClient400()
    assert not store._validate_key("uuid:filename")

    class PatchedTransferClient401:
        def get_task(self, *args, **kwargs):
            class PatchedError(globus_sdk.TransferAPIError):
                def __init__(self):
                    self.http_status = 401

            raise PatchedError()

    store._transfer_client = PatchedTransferClient401()
    with raises(globus_sdk.TransferAPIError):
        store._validate_key("uuid:filename")

    class PatchedTransferClientTimeout:
        def task_wait(self, *args, **kwargs):
            return False

    store._transfer_client = PatchedTransferClientTimeout()
    with raises(RuntimeError):
        store._wait_on_tasks(["1234"])


def test_get_filepath(monkeypatch) -> None:
    """Test GlobusStore filepath building."""
    endpoints = GlobusEndpoints(
        [
            GlobusEndpoint(
                uuid="EP1UUID",
                endpoint_path="/~/",
                local_path="/tmp/proxystore-test-1",
                host_regex="localhost",
            ),
            GlobusEndpoint(
                uuid="EP2UUID",
                endpoint_path="/~/",
                local_path="/tmp/proxystore-test-2",
                host_regex="localhost",
            ),
        ],
    )

    store = GlobusStore("globus", endpoints=endpoints)

    filename = "test_file"
    for endpoint in endpoints:
        expected_path = os.path.join(endpoint.local_path, filename)
        assert store._get_filepath(filename, endpoint) == expected_path


def test_expand_user_path(monkeypatch) -> None:
    """Test GlobusStore expands user path."""
    store_dir = ".cache/proxystore_cache"
    short_path = os.path.join("~", store_dir)
    full_path = os.path.join(os.path.expanduser("~"), store_dir)

    ep1 = GlobusEndpoint(
        uuid="EP1UUID",
        endpoint_path="/~/",
        local_path=short_path,
        host_regex="localhost",
    )
    ep2 = GlobusEndpoint(
        uuid="EP2UUID",
        endpoint_path="/~/",
        local_path=full_path,
        host_regex="localhost",
    )

    store = GlobusStore("globus", endpoints=[ep1, ep2])

    filename = "test_file"
    assert "~" not in store._get_filepath(filename, ep1)
    assert store._get_filepath(filename, ep1) == store._get_filepath(
        filename,
        ep2,
    )
