import pytest

from typedflow.counted_cache import CacheTable


LIFE: int = 2


@pytest.fixture
def table() -> CacheTable:
    return CacheTable(life=LIFE)


def test_set_and_get(table):
    table.set(1, 'hi')
    assert table.get(1) == 'hi'


def test_life(table):
    table.set(1, 'hi')
    assert table.get(1) == 'hi'
    assert table.get(1) == 'hi'
    with pytest.raises(KeyError):
        table.get(1)


def test_multi_with_same_key(table):
    table.set(1, 'hi')
    assert table.get(1) == 'hi'
    table.set(1, 'hi')
    assert table.get(1) == 'hi'
    assert table.get(1) == 'hi'
    with pytest.raises(KeyError):
        table.get(1)


def test_multiple_keys(table):
    table.set(1, 'hi')
    assert table.get(1) == 'hi'
    table.set(2, 'hi')
    assert table.get(1) == 'hi'
    assert table.get(2) == 'hi'
    with pytest.raises(KeyError):
        table.get(1)
    assert table.get(2) == 'hi'
    with pytest.raises(KeyError):
        table.get(2)
    assert len(table.cache_table) == 0
