"""FileStore Unit Tests"""
import os

from proxystore.store.file import FileStore

STORE_DIR = '/tmp/proxystore-test-8456213966545'


def test_file_store_cleanup() -> None:
    """Test FileStore Cleanup"""
    store = FileStore('files', store_dir=STORE_DIR)

    assert os.path.exists(STORE_DIR)

    store.cleanup()

    assert not os.path.exists(STORE_DIR)
