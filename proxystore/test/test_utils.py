"""Utils Unit Tests."""
from proxystore import utils
from proxystore.factory import SimpleFactory


def test_create_key() -> None:
    """Test create_key()."""
    assert isinstance(utils.create_key(42), str)


def test_fullname() -> None:
    """Test fullname()."""
    assert utils.fullname(SimpleFactory) == "proxystore.factory.SimpleFactory"
    assert (
        utils.fullname(SimpleFactory("string"))
        == "proxystore.factory.SimpleFactory"
    )
    assert utils.fullname("string") == "str"
