#
# (C) Copyright 2014 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

from functools import wraps


# Encapsulate re-dispatch of result. Allow on_result to be passed, which should
# be called with self, result.
# dispatch('dispatch', on_result=partial(ui_dispatch, setattr,
# 'my_trait')
# partial(ui_dispatch,
def dispatch(dispatcher=None, call=None):
    """ Dispatch method calls using a dispatcher.

    All calls made to the decorated method are sumbitted to a "dispatcher" (an
    executor, work scheduler, or anything else with a "submit" method), or via
    some other call. The decorated function does not await any feedback from
    the dispatcher. For example, futures returned by an executor are ignored.
    The decorated method returns nothing.function

    Parameters
    ----------
    dispatcher : dispatcher or str, optional
        The object used to dispatch calls. A dispatcher is any object with a
        "submit" method with the Executor.submit call signature. If this is a
        string, it must identify an attribute on the instance to which the
        method is bound.
    call : callable or str, optional
        A callable used to dispatch calls, or a string identifying
        a callable on the instance to which the method is bound. The callable
        must support the Executor.call signature.

    Notes
    -----
    Exactly one of ``dispatcher`` or ``call`` must be specified.

    """
    if (dispatcher, call).count(None) != 1:
        msg = "Provide exactly one of 'dispatch' or 'call'"
        raise ValueError(msg)

    def decorate_with_dispatcher(func):

        lookup_dispatcher = isinstance(dispatcher, basestring)

        @wraps(func)
        def wrapper(self, *args, **kw):

            if lookup_dispatcher:
                dispatcher_ = getattr(self, dispatcher)
            else:
                dispatcher_ = dispatcher

            dispatcher_.submit(func, self, *args, **kw)

        return wrapper

    def decorate_with_call(func):

        lookup_call = isinstance(call, basestring)

        @wraps(func)
        def wrapper(self, *args, **kw):

            if lookup_call:
                call_ = getattr(self, call)
            else:
                call_ = call

            call_(func, self, *args, **kw)

        return wrapper

    return decorate_with_dispatcher if call is None else decorate_with_call
