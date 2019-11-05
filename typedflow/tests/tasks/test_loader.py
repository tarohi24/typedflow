import pytest

from typedflow.tasks import DataLoader

from .base import Arg


@pytest.fixture
def loader():
    gen = (Arg(string='hi', int_value=i) for i in range(3))
    return DataLoader(gen=gen, batch_size=2)


def test_load(loader):
    gen = loader.load()
    # first batch
    batch_1 = next(iter(gen))
    assert batch_1.batch_id == 0
    strs = [a.string for a in batch_1.data]
    ints = [a.int_value for a in batch_1.data]
    assert strs == ['hi', 'hi']
    assert ints == [0, 1]

    # second batch
    batch_2 = next(iter(gen))
    assert batch_2.batch_id == 1
    strs = [a.string for a in batch_2.data]
    ints = [a.int_value for a in batch_2.data]
    assert strs == ['hi', ]
    assert ints == [2, ]

    # no more batch
    with pytest.raises(StopIteration):
        next(iter(gen))
    with pytest.raises(StopIteration):
        next(iter(gen))
