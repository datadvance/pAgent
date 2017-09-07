#
# coding: utf-8
# Copyright (c) 2017 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import asyncio
import logging
import pathlib
import sys

import pytest

from .helper_agent_process import AgentProcess


def helper_process_common_args(name):
    TEST_PROCESS_DIR = 'test_processes'
    process_code = str(
        pathlib.Path(__file__)
        .absolute()
        .parent
        .joinpath('test_processes', name)
    )
    return [sys.executable, process_code]


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    """Session-wide logging setup."""
    root_logger = logging.getLogger()
    # root logger accepts all messages, filter on handlers level
    root_logger.setLevel(logging.DEBUG)
    for handler in [logging.StreamHandler(sys.stdout)]:
        handler.setFormatter(
            logging.Formatter(
                '[%(process)d] [%(asctime)s] '
                '[%(name)s] [%(levelname)s] %(message)s'
            )
        )
        root_logger.addHandler(handler)


@pytest.fixture
def mock_server_command():
    MOCK_SERVER_FILENAME = 'mock_server.py'

    def command_factory(socket_count=1, exit_delay=60):
        args = helper_process_common_args(MOCK_SERVER_FILENAME)
        args.extend([
            '--socket-count=%d' % (socket_count,),
            '--exit-delay=%f' % (exit_delay,)
        ])
        return args
    return command_factory


@pytest.fixture
def http_server_command():
    HTTP_SERVER_FILENAME = 'http_server.py'

    def command_factory(exit_delay=60):
        args = helper_process_common_args(HTTP_SERVER_FILENAME)
        args.extend([
            '--exit-delay=%f' % (exit_delay,)
        ])
        return args
    return command_factory



@pytest.fixture
def agent_process(event_loop):
    def factory(*args, **kwargs):
        return AgentProcess(*args, **kwargs, loop=event_loop)
    return factory


@pytest.fixture
def event_loop():
    """Get the eventloop instance for async tests."""
    if sys.platform == 'win32':
        asyncio.set_event_loop(asyncio.ProactorEventLoop())
    else:
        asyncio.set_event_loop(asyncio.SelectorEventLoop())
    return asyncio.get_event_loop()


@pytest.fixture(scope='function')
def test_log(request):
    logger = logging.getLogger('pytest:' + request.function.__name__)
    # write a single log line to circumvent absence of \n at the end of the test
    # name in 'pytest -sv' output
    logger.info('Logger initialized')
    return logger


ASYNC_TEST_MARKER = 'async_test'
EVENTLOOP_FIXTURE = 'event_loop'


def pytest_pyfunc_call(pyfuncitem):
    """Run marked test functions in an event loop instead of a direct
    function call.

    Insipred by pytest-asyncio.
    """
    if ASYNC_TEST_MARKER in pyfuncitem.keywords:
        event_loop = pyfuncitem.funcargs[EVENTLOOP_FIXTURE]

        funcargs = pyfuncitem.funcargs
        testargs = {
          arg: funcargs[arg] for arg in pyfuncitem._fixtureinfo.argnames
        }

        event_loop.run_until_complete(pyfuncitem.obj(**testargs))
        return True
