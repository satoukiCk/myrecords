# -*- coding: utf-8 -*-

import os
from inspect import isclass
import tablib

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

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.values()[key]

        if key in self.keys():
            i = self.keys().index(key)
            if self.keys().count(key) > 1:
                raise KeyError("Record contains multiple '{}' fields".format(key))
            return self.values()[i]

        return KeyError("Record contains multiple no '{}' fields")

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            return AttributeError(e)

    def __dir__(self):
        standard = dir(super(Record, self))
        return sorted(standard + [str(k) for k in self.keys()])

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self, ordered=False):
        items = zip(self.keys(), self.values())

        return OrderedDict(items) if ordered else dict(items)

    @property
    def dataset(self):
        data = tablib.Dataset()
        data.headers = self.keys()

        row = _reduce_datetimes(self.values())
        data.append(row)

        return data

    def export(self, format, **kwargs):
        self.dataset.export(format, **kwargs)


class RecordCollection:
    def __init__(self, rows):
        self._rows = rows
        self._all_rows = []
        self.pending = True

    def __repr__(self):
        return '<RecordCollection size={} pending={}>'.format(len(self), self.pending)

    def __iter__(self):

    def __len__(self):
        return len(self._all_rows)


def _reduce_datetimes(row):
    row = list(row)
    for i in range(len(row)):
        if hasattr(row[i],'isoformat'):
            row[i] = row[i].isoformat()
    return tuple(row)