from typing import List

from typedflow.nodes import task, loader, dumper
from typedflow.flow import Flow


@loader
def load() -> List[int]:
    return list(range(10))


@task
def task(a: int) -> str:
    return str(a)


@dumper
def dump(s: str) -> None:
    print(s)


def test_flow():
    (task < load)('a')
    (dump < task)('s')
    flow = Flow([dump, ])
    flow.typecheck()
