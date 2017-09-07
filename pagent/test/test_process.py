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

import sys

import pytest

from pagent import polled_process


@pytest.mark.async_test
async def test_basic_run(event_loop, test_log, mock_server_command):
    """Test positive case with a well-behaved child process - it publishes
    a single endpoint and properly 'waits for events'.
    """
    process = polled_process.PolledProcess(
        mock_server_command(),
        None
    )
    assert process.state == polled_process.ProcessState.NEW
    assert process.port is None

    await process.start()
    assert process.state == polled_process.ProcessState.RUNNING
    assert process.port is not None
    assert 0 < process.port < 65536
    test_log.info('Child listen port: %d', process.port)

    await process.kill()
    await process.wait()
    assert process.state == polled_process.ProcessState.FINISHED


@pytest.mark.async_test
async def test_on_finished(event_loop, test_log, mock_server_command):
    """Check that PolledProcess.on_finished signal is actually emitted and
    ensure that process state is properly updated at the time of the signal.
    """
    callback_results = {
        'status': None,
        'port': None
    }

    async def on_finished(proc):
        test_log.info('on_finished callback called')
        callback_results['status'] = proc.state
        callback_results['port'] = proc.port

    process = polled_process.PolledProcess(
        mock_server_command(),
        None
    )
    process.on_finished.append(on_finished)

    await process.start()
    assert process.port is not None

    await process.kill()
    await process.wait()
    assert process.state == polled_process.ProcessState.FINISHED
    assert callback_results['status'] == polled_process.ProcessState.FINISHED
    assert callback_results['port'] is None


@pytest.mark.async_test
async def test_no_port(event_loop, test_log, mock_server_command):
    """Check that process that didn't bind any endpoints is handled correctly.
    """
    # Case 1: process didn't bind an endpoint in time.
    PORT_TIMEOUT = 0.5
    process = polled_process.PolledProcess(
        mock_server_command(socket_count=0),
        None
    )
    with pytest.raises(polled_process.ProcessStartTimeoutError):
        await process.start(port_timeout=PORT_TIMEOUT)
    # Case 2: process exits without binding an endpoint.
    process = polled_process.PolledProcess(
        mock_server_command(socket_count=0, exit_delay=0),
        None
    )
    with pytest.raises(polled_process.ProcessExitedError):
        await process.start()


@pytest.mark.async_test
async def test_notify(event_loop, test_log, mock_server_command):
    """Check external port discovery mode.
    """
    STATE_POLL_INTERVAL = 0.001
    PORTS = [80, 8080]
    process = polled_process.PolledProcess(
        mock_server_command(socket_count=0),
        None
    )
    async def notify_process(process, ports):
        while process.state == polled_process.ProcessState.NEW:
            asyncio.sleep(STATE_POLL_INTERVAL, loop=event_loop)
        assert process.state == polled_process.ProcessState.PENDING
        process.notify(ports)

    event_loop.create_task(notify_process(process, PORTS))
    await process.start(
        port_discovery=polled_process.ProcessPortDiscovery.EXTERNAL,
        port_poll_interval=1,
        port_expected_count=len(PORTS)
    )
    try:
        assert process.port is None
        assert process.ports == PORTS
    finally:
        await process.kill()
        await process.wait()
