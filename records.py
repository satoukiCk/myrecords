# -*- coding: utf-8 -*-

import os
from inspect import isclass

from collections import OrderedDict

DATABASE_URL = os.environ.get('DATABASE_URL')


def isexception(obj):
    if isinstance(obj,Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


class Record(object):
    __slots__ = ['_keys', '_values']

    def __init__(self, keys, values):
        self._keys = keys
        self._values = values

        assert len(self._keys) == len(self._values)

    def keys(self):
        return self._keys

    def values(self):
        return self._values

    def __repr__(self):
        return '<Record {}>'.format(self.export('json')[1:-1])


