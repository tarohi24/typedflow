from typing import List, get_type_hints

from typedflow.nodes import task, loader, dumper, LoaderNode, TaskNode, DumpNode
from typedflow.flow import Flow



def test_flow():

    def load() -> List[int]:
        return list(range(10))


    @task
    def t(a: int) -> str:
        return str(a)


    @dumper
    def dump(s: str) -> None:
        print(s)

    loader = LoaderNode(load)
    assert isinstance(loader, LoaderNode)
    assert isinstance(t, TaskNode)
    assert isinstance(dump, DumpNode)
    (t < loader)('a')
    (dump < t)('s')
    flow = Flow([dump, ])
    flow.typecheck()


def test_classmethod():

    class A:
        def load(self) -> List[int]:
            return list(range(10))
        
        @task
        def tasker(self, a: int) -> str:
            return str(a)

        @dumper
        def dump(self, s: str) -> None:
            print(s)

    a = A()
    loader = LoaderNode(a.load)
    get_type_hints(a.tasker.func)
    get_type_hints(A.dump.func)
    (a.tasker < loader)('a')
    (A.dump < a.tasker)('s')
    flow = Flow([A.dump, ])
    flow.typecheck()
