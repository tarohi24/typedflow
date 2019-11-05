from typedflow.arguments import TaskArguments
from typedflow.batch import Batch
from typedflow.exceptions import FaultItem

import pytest


class InheritedArg(TaskArguments):
    pass


@pytest.fixture
def arg() -> TaskArguments:
    return TaskArguments()


@pytest.fixture
def inherited_arg() -> InheritedArg:
    return InheritedArg()


@pytest.fixture
def fault_item() -> FaultItem:
    return FaultItem()


def test_valid_arg(arg, inherited_arg, fault_item):
    Batch(batch_id=1, data=[arg])
    # valid inheritance
    Batch(batch_id=1, data=[inherited_arg])
    Batch(batch_id=1, data=[fault_item])
