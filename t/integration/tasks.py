# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from celery import chain, group, shared_task
from celery.exceptions import SoftTimeLimitExceeded
from celery.utils.log import get_task_logger

from celery_batches import Batches

logger = get_task_logger(__name__)


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Results(object):
    """A singleton for storing information about the result."""
    __metaclass__ = Singleton
    _results = []

    def set(self, res):
        self._results.append(res)

    def get(self):
        return self._results.pop()


@shared_task(base=Batches, flush_every=2, flush_interval=1)
def add(requests):
    """Add the first argument of each call."""
    result = 0
    for request in requests:
        result += request.args[0]

    Results().set(result)