#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
"""
To-Do List Example
------------------

This is a simple to-do list example illustrating the usage of encore.storage.
The classes in this module are written so that any writeable encore.storage API
implementation can be used to hold the data.

"""

import sys
import datetime

from encore.storage.dict_memory_store import DictMemoryStore
from encore.storage.string_value import StringValue


class ToDoList(object):
    """ To-do list class

    This is a to-do list which stores its data in an encore.storage store.
    Any writeable store can be used for this (and a read-only store can even
    be used if it is pre-filled with appropriate data).

    Attributes
    ----------
    store : AbstractStore instance
        The store which holds the information.

    """

    def __init__(self, store=None):
        if store is None:
            store = DictMemoryStore()
        self.store = store
        self.store.connect()

    def add_todo(self, who, what, where, when):
        """ Add an item to the to-do list

        Parameters
        ----------
        who : string
            The person involved in the item
        what : string
            A string describing the item
        where : string
            The location of the item
        when : datetime
            The time when the item will occur

        """
        metadata = {
            'who': who,
            'where': where,
            'year': when.year,
            'month': when.month,
            'day': when.day,
            'hour': when.hour,
            'minute': when.minute,
            'second': when.second,
            'date': [when.year, when.month, when.day],
            'time': [when.hour, when.minute, when.second],
        }
        value = StringValue(what, metadata)
        key = when.isoformat()+'-'+who
        self.store.set(key, value)
        return key

    def remove_todo(self, when, who):
        """ Remove an item to the to-do list

        Parameters
        ----------
        when : datetime
            The time when the item to be removed occurs.
        who : str
            The person involved in the datetime to be removed.

        """
        keys = self.model.store.query_keys(year=when.year, month=when.month,
            day=when.day, hour=when.hour, minute=when.minute, second=when.second, who=who)
        for key in keys:
            self.store.delete(key)

    def todo_for_date(self, date):
        """ Return the list of items for a given date

        Parameters
        ----------
        date : datetime.date
            The date to be shown.

        """
        items = self.store.query(year=date.year, month=date.month, day=date.day)
        return sorted((self.store.get(key) for key, metadata in items),
            key=lambda item: item[1]['time'])


class ToDoView(object):
    """ To-do view class

    This is a class which provides a simple shell-based interface to the
    ToDoList class.

    Attributes
    ----------
    model : ToDoList instance
        The model which holds the to-do list.
    time_format : str
        A format string suitable for printing a time using strftime.
    date_format : str
        A format string suitable for printing a date using strftime.

    """

    def __init__(self, model=None, time_format='%I:%M %p', date_format='%d/%m/%y'):
        if model is None:
            model = ToDoList()
        self.model = model
        self.time_format = time_format
        self.date_format = date_format

    def show_summary(self, value, show_date=False):
        """ Display an item's summary """
        metadata = value.metadata
        if show_date:
            date = datetime.date(*metadata['date'])
            time_str = date.strftime(self.date_format)
        else:
            time_str = ''
        time = datetime.time(*metadata['time'])
        time_str += time.strftime(self.time_format)
        print('{0} - {who} @ {where}'.format(time_str, **metadata))
        what = value.data.read(73)
        if '\n' in what:
            what = what.split('\n')[0]+'...'
        elif len(what) == 73:
            what = what[:72]+'...'
        print('   {0}'.format(what))

    def show_item(self, value, show_date=False):
        """ Display an item's full information """
        metadata = value.metadata
        if show_date:
            date = datetime.date(*metadata['date'])
            time_str = date.strftime(self.date_format)
        else:
            time_str = ''
        time = datetime.time(*metadata['time'])
        time_str += time.strftime(self.time_format)
        print('''Time:  {0}
Who:   {who}
Where: {where}
'''.format(time_str, **metadata))
        for chunk in value.iterdata():
            sys.stdout.write(chunk)

    def show_day(self, date=None):
        """ Display all items in a day """
        if date is None:
            date = datetime.date.today()
        print(date.strftime(self.date_format))
        for value in self.model.todo_for_date(date):
            self.show_summary(value)

    def add_todo(self, date=None, time=None, who=None, where=None):
        """ Add an item to the to-do list"""
        if date is None:
            date = datetime.datetime.today()
        if time is None:
            time = datetime.time(datetime.datetime.now().hour+1)
        when = datetime.datetime.combine(date, time)
        if who is None:
            who = raw_input('Who:   ')
        if where is None:
            where = raw_input('Where: ')
        what = sys.stdin.read()
        self.model.add_todo(who, what, where, when)

    def remove_todo(self, date=None, time=None, who=None):
        """ Remove an item to the to-do list"""
        if date is None:
            date = datetime.datetime.today()
        if time is None:
            time = datetime.time(datetime.datetime.now().hour+1)
        if who is None:
            who = raw_input('Who:   ')
        self.model.remove_todo(datetime.datetime.combine(date, time), who)
