from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path
from typing import ClassVar, Dict, List, TypeVar

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
            tail: Job = self.jobs[-1]
            assert tail.out_class == job.in_class
        else:
            assert isinstance(job, InitialMessage)


@dataclass_json
@dataclass
class Message:

    def dump(self,
             path: Path,
             check_existence: bool = True) -> None:
        if check_existence and path.exists():
            raise AssertionError(f'{path} already exists')
        data: Dict = self.to_dict()
        with open(path, 'w') as fout:
            json.dump(data, fout)


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
    in_class and out_class should be overwritten when declaring
    the class
    """
    flow: Flow
    in_class: ClassVar[T] = Message
    out_class: ClassVar[K] = Message

    def process(self,
                message: Job.in_class) -> Job.out_class:
        raise NotImplementedError('Implemented not yet')
