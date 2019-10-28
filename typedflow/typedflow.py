from __future__ import annotations
from dataclasses import dataclass
from typing import ClassVar, Generic, List, TypeVar

from dataclasses_json import dataclass_json

from typedflow.settings import logdir


@dataclass
class Flow:
    id: int
    jobs: List[Job]

    def get_logpath(self):
        return logdir.joinpath(f'{str(self.id)}')

    def append_job(self,
                   job: Job) -> Flow:
        if len(self.jobs) > 0:
            tail: jobs = jobs[-1]
            assert jobs
        else:
            assert jobs.__args__[0] == InitialMessage


@dataclass_json
@dataclass
class Message:
    workflow: WorkFlow

    def dump(self,
             input_file: Path) -> Path:
        logpath = self.workflow.get_logpath()


class InitialMessage(Message):
    """
    Every workflow starts with a job that receive this instance
    """
    def load(self) -> Message:
        raise NotImplementedError('Implemented not yet')


T = TypeVar('T', bound=Message)
K = TypeVar('K', bound=Message)


@dataclass
class Job:
    """
    A job which receives an instance of T
    and sends an instace of K
    """
    workflow: WorkFlow
    in_class: ClassVar[T]
    out_class: ClassVar[K]

    def process(self,
                message: Job.in_class) -> Job.out_class:
        raise NotImplementedError('Implemented not yet')
