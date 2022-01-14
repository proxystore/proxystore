"""RedisStore Unit Tests"""
import subprocess
import time

from pytest import fixture

from proxystore.store.redis import RedisStore

from proxystore.store.test.utils import REDIS_STORE, REDIS_PORT


@fixture(scope='session', autouse=True)
def init() -> None:
    """Launch Redis Server"""
    redis_handle = subprocess.Popen(
        ['redis-server', '--port', str(REDIS_PORT)], stdout=subprocess.DEVNULL
    )
    time.sleep(1)
    yield
    redis_handle.kill()


def test_kwargs() -> None:
    """Test RedisFactory kwargs"""
    store = RedisStore(REDIS_STORE["name"], **REDIS_STORE["kwargs"])
    assert store.kwargs == {
        **REDIS_STORE["kwargs"],
        'cache_size': store.cache_size,
    }
    store.cleanup()
