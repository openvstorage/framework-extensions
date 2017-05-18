# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010 Incubaid BVBA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Utility functions'''

import __builtin__
import uuid
import logging
import functools
import itertools

LOGGER = logging.getLogger(__name__)
'''Logger for code in this module''' #pylint: disable=W0105


def update_argspec(*argnames): #pylint: disable=R0912
    '''Wrap a callable to use real argument names

    When generating functions at runtime, one often needs to fall back to
    ``*args`` and ``**kwargs`` usage. Using these features require
    well-documented code though, and renders API documentation tools less
    useful.

    The decorator generated by this function wraps a decorated function,
    which takes ``**kwargs``, into a function which takes the given argument
    names as parameters, and passes them to the decorated function as keyword
    arguments.

    The given argnames can be strings (for normal named arguments), or tuples
    of a string and a value (for arguments with default values). Only a couple
    of default value types are supported, an exception will be thrown when an
    unsupported value type is given.

    Example usage::

        >>> @update_argspec('a', 'b', 'c')
        ... def fun(**kwargs):
        ...     return kwargs['a'] + kwargs['b'] + kwargs['c']

        >>> import inspect
        >>> tuple(inspect.getargspec(fun))
        (['a', 'b', 'c'], None, None, None)

        >>> print fun(1, 2, 3)
        6
        >>> print fun(1, c=3, b=2)
        6

        >>> print fun(1, 2)
        Traceback (most recent call last):
            ...
        TypeError: fun() takes exactly 3 arguments (2 given)


        >>> @update_argspec()
        ... def g():
        ...     print 'Hello'

        >>> tuple(inspect.getargspec(g))
        ([], None, None, None)

        >>> g()
        Hello

        >>> @update_argspec('name', ('age', None))
        ... def hello(**kwargs):
        ...     name = kwargs['name']
        ...
        ...     if kwargs['age'] is None:
        ...         return 'Hello, %s' % name
        ...     else:
        ...         age = kwargs['age']
        ...         return 'Hello, %s, who is %d years old' % (name, age)

        >>> tuple(inspect.getargspec(hello))
        (['name', 'age'], None, None, (None,))

        >>> hello('Nicolas')
        'Hello, Nicolas'
        >>> hello('Nicolas', 25)
        'Hello, Nicolas, who is 25 years old'

    :param argnames: Names of the arguments to be used
    :type argnames: iterable of :class:`str` or `(str, object)`

    :return: Decorator which wraps a given callable into one with a correct
        argspec
    :rtype: `callable`
    '''

    argnames_ = tuple(itertools.chain(argnames, ('', )))

    # Standard execution context, contains only what we actually need in the
    # function template
    context = {
        '__builtins__': None,
        'dict': __builtin__.dict,
        'zip': __builtin__.zip,
        'True': True,
        'False': False,
    }

    # Template for the function which will be compiled later on
    def _format(value):
        '''Format a value for display in a function signature'''

        if isinstance(value, unicode):
            return 'u\'%s\'' % value
        elif isinstance(value, str):
            return '\'%s\'' % value
        elif isinstance(value, bool):
            return 'True' if value else 'False'
        elif isinstance(value, (int, long)):
            return '%d' % value
        elif value is None:
            return 'None'
        else:
            raise TypeError

    def _generate_signature(args):
        '''Format arguments for display in a function signature'''

        for arg in args:
            if isinstance(arg, str):
                yield '%s' % arg
            else:
                arg, default = arg
                yield '%s=%s' % (arg, _format(default))

    template_signature = ', '.join(_generate_signature(argnames_))
    template_args = ', '.join(name if isinstance(name, str) else name[0] \
        for name in argnames_) if argnames_ else ''
    template_argnames = ', '.join(
        '\'%s\'' % (name if isinstance(name, str) else name[0])
        for name in argnames_) if argnames_ else ''

    fun_def_template = '''
def %%(name)s(%(signature)s):
    %%(kwargs_name)s = dict(zip((%(argnames)s), (%(args)s)))

    return %%(orig_name)s(**%%(kwargs_name)s)
''' % {
        'signature': template_signature,
        'args': template_args,
        'argnames': template_argnames,
    }

    def wrapper(fun):
        '''
        Decorating which wraps the decorated function in a callable which uses
        named arguments

        :param fun: Callable to decorate
        :type fun: `callable`

        :see: :func:`update_argspec`
        '''

        # We need unique names for the variables used in the function template,
        # they shouldn't conflict with the arguments
        random_suffix = lambda: str(uuid.uuid4()).replace('-', '')

        orig_function_name = None
        while (not orig_function_name) or (orig_function_name in argnames_):
            orig_function_name = '_orig_%s' % random_suffix()

        kwargs_name = None
        while (not kwargs_name) or (kwargs_name in argnames_):
            kwargs_name = '_kwargs_%s' % random_suffix()


        # Fill in function template
        fun_def = fun_def_template % {
            'name': fun.__name__,
            'orig_name': orig_function_name,
            'kwargs_name': kwargs_name,
        }

        # Compile function to a code object
        code = compile(fun_def, '<update_argspec>', 'exec', 0, 1)

        # Create evaluation context
        env = context.copy()
        env[orig_function_name] = fun

        # Evaluate the code object in the evaluation context
        eval(code, env, env)

        # Retrieve the compiled/evaluated function
        fun_wrapper = env[fun.__name__]

        # Update __*__ attributes
        updated = functools.update_wrapper(fun_wrapper, fun)

        return updated

    return wrapper


def format_doc(doc):
    '''Try to format a docstring

    This function will split the given string on line boundaries, strip all
    lines, and stitch everything back together.

    :param doc: Docstring to format
    :type doc: :class:`str` or :class:`unicode`

    :return: Formatted docstring
    :rtype: :class:`unicode`
    '''

    if isinstance(doc, str):
        doc = doc.decode('utf-8')

    return u'\n'.join(line.strip() for line in doc.splitlines())


def kill_coroutine(coroutine, log_fun=None):
    '''Kill a coroutine by injecting :exc:`StopIteration`

    If the coroutine has exited already, we ignore any errors.

    The provided `log_fun` function will be called when an unexpected error
    occurs. It should take a *message* argument.

    Example:

        >>> import sys

        >>> def f():
        ...     a = yield 1
        ...     b = yield 3
        ...     c = yield 5

        >>> f_ = f()
        >>> print f_.next()
        1
        >>> print f_.send('2')
        3
        >>> kill_coroutine(f_)

        >>> def incorrect():
        ...     try:
        ...         yield 1
        ...         a = yield 2
        ...     except:
        ...         raise Exception
        ...     yield 3

        >>> i = incorrect()
        >>> print i.next()
        1
        >>> kill_coroutine(i,
        ...     lambda msg: sys.stdout.write('Error: %s' % msg))
        Error: Failure while killing coroutine

    :param coroutine: Coroutine to kill
    :type coroutine: `generator`
    :param log_fun: Function to call when an exception is encountered
    :type log_fun: `callable`
    '''

    try:
        coroutine.close()
    except: #pylint: disable=W0702
        try:
            if log_fun:
                log_fun('Failure while killing coroutine')
        except: #pylint: disable=W0702
            pass


def process_blocking(message, stream):
    '''Process a message using a blocking stream API

    The given `message` will be serialized and written to the stream. Once the
    message was written, the result will be read using :func:`read_blocking`.

    The given stream object should implement `write` and `read` methods,
    somewhat like the file interface.

    :param message: Message to process
    :type message: :class:`pyrakoon.protocol.Message`
    :param stream: Stream to work on
    :type stream: :obj:`object`

    :return: Result of the command execution
    :rtype: :obj:`object`

    :see: :meth:`pyrakoon.client.AbstractClient._process`
    :see: :meth:`pyrakoon.protocol.Message.serialize`
    :see: :meth:`pyrakoon.protocol.Message.receive`
    '''

    for bytes_ in message.serialize():
        stream.write(bytes_)

    return read_blocking(message.receive(), stream.read)


def read_blocking(receiver, read_fun):
    '''Process message result parsing using a blocking stream read function

    Given a function to read a given amount of bytes from a result channel,
    this function handles the interaction with the parsing coroutine of a
    message (as passed to :meth:`pyrakoon.client.AbstractClient._process`).

    :param receiver: Message result parser coroutine
    :type receiver: :obj:`generator`
    :param read_fun: Callable to read a given number of bytes from a result
        stream
    :type read_fun: `callable`

    :return: Message result
    :rtype: :obj:`object`

    :raise TypeError:
        Coroutine didn't return a :class:`~pyrakoon.protocol.Result`

    :see: :meth:`pyrakoon.protocol.Message.receive`
    '''

    from ovs_extensions.db.arakoon.pyrakoon.pyrakoon import protocol

    request = receiver.next()

    while isinstance(request, protocol.Request):
        value = read_fun(request.count)
        request = receiver.send(value)

    if not isinstance(request, protocol.Result):
        raise TypeError

    kill_coroutine(receiver, LOGGER.exception)

    return request.value