#!/usr/bin/env python
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
This example can be run as a command line utility to manage a to-do list: run
with the -h option to see all the possible options.

All the commands support a --store argument which allows the user to switch the
underlying storage implementation which is used between an in-memory (and
transient) store, a file-system based store, and a sqlite based store.  Since
the code is written strictly to the API, this requires no additional work to
support.

"""

import argparse
import datetime

from todo_list import ToDoView, ToDoList

def parsedate(datestring):
    """ Parse a string into a date

    This tries to use the parsedatetime package (http://pypi.python.org/pypi/parsedatetime)
    if it is installed, otherwise if falls back to the simple strptime format
    %m/%d/%y.

    """
    try:
        import parsedatetime.parsedatetime
        import parsedatetime.parsedatetime_consts
    except ImportError:
        dt = datetime.datetime.strptime(datestring, '%m/%d/%y')
        return dt.date()
    else:
        constants = parsedatetime.parsedatetime_consts.Constants()
        calendar = parsedatetime.parsedatetime.Calendar(constants)
        time_struct, result = calendar.parse(datestring)
        if result in [0, 2]:
            raise ValueError("could not parse '%s' as a date" % datestring)
        return datetime.date(*time_struct[:3])

def parsetime(timestring):
    """ Parse a string into a time

    This tries to use the parsedatetime package (http://pypi.python.org/pypi/parsedatetime)
    if it is installed, otherwise if falls back to the simple strptime format
    %H:%M.

    """
    try:
        import parsedatetime.parsedatetime
        import parsedatetime.parsedatetime_consts
    except ImportError:
        dt = datetime.datetime.strptime(timestring, '%H:%M')
        return dt.time()
    else:
        constants = parsedatetime.parsedatetime_consts.Constants()
        calendar = parsedatetime.parsedatetime.Calendar(constants)
        time_struct, result = calendar.parse(timestring)
        if result in [0, 1]:
            raise ValueError("could not parse '%s' as a date" % timestring)
        return datetime.time(*time_struct[3:6])


def _common_arguments(parser):
    """ Setup common arguments that we want all commands to parse """
    parser.add_argument('--store', choices=['file', 'sqlite'], action='store')
    parser.add_argument('--location', action='store')
    parser.add_argument('--date', type=parsedate, default=datetime.date.today(), action='store')

def parse():
    """ Parse commandline arguments """
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    show_parser = subparser.add_parser('show')
    _common_arguments(show_parser)
    show_parser.set_defaults(func=show)

    add_parser = subparser.add_parser('add')
    _common_arguments(add_parser)
    add_parser.add_argument('--time', metavar='TIME', type=parsetime, default=datetime.datetime.now().time(), action='store')
    add_parser.add_argument('--who', type=str, action='store')
    add_parser.add_argument('--where', type=str, action='store')
    add_parser.set_defaults(func=add)

    remove_parser = subparser.add_parser('remove')
    _common_arguments(remove_parser)
    remove_parser.add_argument('--time', metavar='TIME', type=parsetime, default=datetime.datetime.now().time(), action='store')
    remove_parser.add_argument('--who', type=str, action='store')
    remove_parser.set_defaults(func=remove)

    test_parser = subparser.add_parser('test')
    _common_arguments(test_parser)
    test_parser.set_defaults(func=test)

    return parser.parse_args()

def main():
    """ Main entrypoint """
    args = parse()
    if args.store == 'file':
        import os
        from encore.storage.filesystem_store import init_shared_store, FileSystemStore
        if args.location == None:
            args.location = 'file_store'
        if not os.path.exists(args.location):
            os.mkdir(args.location)
        init_shared_store(args.location)
        store = FileSystemStore(args.location)
    elif args.store == 'sqlite':
        from encore.storage.sqlite_store import SqliteStore
        if args.location == None:
            args.location = ':memory:'
        store = SqliteStore(args.location)
    else:
        store = None
    todo_list = ToDoList(store)
    view = ToDoView(todo_list)

    args.func(view, args)

def show(view, args):
    view.show_day(args.date)

def add(view, args):
    view.add_todo(args.date, args.time, args.who, args.where)

def remove(view, args):
    view.remove_todo(args.date, args.time, args.who)

def test(view, args):
    view.model.add_todo(
        who='Eric',
        what='Write webinar',
        where='Enthought',
        when=datetime.datetime.combine(args.date, datetime.time(hour=9)),
    )
    view.model.add_todo(
        who='Corran',
        what='Write Encore examples',
        where='Enthought',
        when=datetime.datetime.combine(args.date, datetime.time(hour=8, minute=30)),
    )
    view.model.add_todo(
        who='Chris',
        what='Test enaml code',
        where='Enthought',
        when=datetime.datetime.combine(args.date, datetime.time(hour=10, minute=30)),
    )
    view.model.add_todo(
        who='Everyone',
        what='Beer',
        where='Gingerman',
        when=datetime.datetime.combine(args.date, datetime.time(hour=18, minute=30)),
    )
    view.show_day(args.date)

if __name__ == "__main__":
    main()
