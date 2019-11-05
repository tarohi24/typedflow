__all__ = ['BatchIsEmpty', 'EndOfBatch', 'FaultItem']


class BatchIsEmpty(Exception):
    pass


class EndOfBatch(Exception):
    pass


class FaultItem:
    pass
