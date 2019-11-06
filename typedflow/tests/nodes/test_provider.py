import pytest

from typedflow.nodes import ProviderNode, LoaderNode
from typedflow.tasks import DataLoader


@pytest.fixture
def loader_node() -> LoaderNode[str]:
    lst: List[str] = ['hi', 'hello']
    loader: DataLoader[str] = DataLoader(gen=lst, batch_size=2)
    node: LoaderNode[str] = LoaderNode(loader=loader)
    return node


def test_init(loader_node):
    node = loader_node
    assert node.succ_count == 0
    assert len(node.cache_table) == 0
