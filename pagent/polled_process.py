#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import asyncio
import enum
import logging
import os
import subprocess

import psutil

import prpc.utils


@enum.unique
class ProcessState(enum.Enum):
    """Process FSM states."""
    NEW = enum.auto()
    PENDING = enum.auto()
    RUNNING = enum.auto()
    FINISHED = enum.auto()


@enum.unique
class ProcessPortDiscovery(enum.Enum):
    """Process port discovery modes.

    * NONE: Disable port discovery.
    * POLL_CONNECTIONS: Discover ports automatically by polling
        list of its active connections using OS-specific API.
    * EXTERNAL: Wait for external notification that process successfully started
        (and, optionally, listen for some ports).
    """
    NONE = enum.auto()
    POLL_CONNECTIONS = enum.auto()
    EXTERNAL = enum.auto()


class ProcessError(Exception):
    """Base process-related error."""


class ProcessStartTimeoutError(ProcessError):
    """Process did not publish the endpoint in time."""


class ProcessPortCountError(ProcessError):
    """Process published multiple endpoints."""


class ProcessExitedError(ProcessError):
    """Process has finished unexpectedly."""


class PolledProcess(object):
    """Mixed sync-async subprocess implementation.

    Uses async polling task combined with sync subprocesss API to
    control the process lifetime.

    This avoids ties with the ProactorEventLoop on windows and
    allows using those processes alongside the network-io loops.
    Reason: IOCP-based ProactorEventLoop is a one special snowflake
    incompatible with the many network libraries due to absence
    of add_reader/add_writer methods.

    Args:
      args (list): Process argument list
      env (dict or None): Modified process environment
      poll_interval (float): Process polling interval
      logger: Logger to use.
      loop: asyncio event loop to use.
    """
    DEFAULT_LOG_NAME = 'pagent.PolledProcess'

    EXPECTED_SOCKET_TYPE = 'tcp'

    def __init__(self, args, env, poll_interval=0.1, logger=None, loop=None):
        # 'Constant' state
        #
        self._loop = loop or asyncio.get_event_loop()
        self._log = logger or logging.getLogger(self.DEFAULT_LOG_NAME)
        self._state_changed = asyncio.Condition()
        self._on_finished = prpc.utils.Signal()
        # Configuration.
        #
        assert poll_interval > 0
        self._args = args
        self._env = env
        self._poll_interval = poll_interval
        # Transient state.
        #
        self._state = ProcessState.NEW
        # subprocess.Popen used for lifetime control.
        self._process = None
        # psutil.Process used for property monitoring.
        self._process_api = None
        self._poll_task = None
        self._port_notification = None
        self._ports = []

    @property
    def state(self):
        """Get current (known) process state."""
        return self._state

    @property
    def port(self):
        """Get port number used by the process."""
        if len(self._ports) == 1:
            return self._ports[0]
        return None

    @property
    def ports(self):
        """Get port numbers used by the process."""
        return list(self._ports)

    @property
    def on_finished(self):
        """Get Signal instance that will be notified on process exit."""
        return self._on_finished

    async def start(self, stdout=None, stderr=None, workdir=None,
                    port_discovery=ProcessPortDiscovery.POLL_CONNECTIONS,
                    port_expected_count=1, port_poll_interval=0.001,
                    port_timeout=30.):
        """Start the process and (optionally) wait for it to bind the port.

        Args:
            stdout: Process stdout destination (see subprocess docs)
            stderr: Process stderr destination (see subprocess docs).
            workdir: Process working directory (see subprocess docs).
            port_discovery: Port discovery mode.
            port_expected_count: Expected number of ports
                to be bound by process.
            port_poll_interval: Ports polling interval, meaning
                depends on port discovery mode, see note below.
            port_timeout: Max time to wait for ports.

        Note:
            Port discovery parameters depend on selected port discovery mode.
            If port_discovery is NONE, all other discovery parameters are
            silently ignored.
            If port_discovery is POLL_CONNECTIONS, port_poll_interval
            sets the minimal time both between polling the process connections
            and between checks of process status.
            If port_discovery is EXTERNAL, port_poll_interval
            sets the time between checking process status. However,
            if 'notify()' is called, it will awaken immediately.

        Warning:
            Combination of 'port_discovery == POLL_CONNECTIONS' and
            'port_expected_count == 1' is especially unsafe. Issue is that
            many event frameworks (esp. on win32) will temporarily
            bind a single socket to support immediate interrupts in 'select'
            loops.
        """
        if self._state not in (ProcessState.NEW, ProcessState.FINISHED):
            raise Exception('process is already running')
        self._log.debug(
            'Starting the process...'
            '\nCommand: %s\nStdout: %s\nStderr: %s',
            self._args,
            stdout if stdout is not None else "<inherited>",
            stderr if stderr is not None else "<inherited>"
        )
        self._state = ProcessState.PENDING
        if workdir is not None:
            workdir = os.fspath(workdir)
        if port_discovery == ProcessPortDiscovery.EXTERNAL:
            self._port_notification = self._loop.create_future()
        # TODO: Try to validate args[0] + workdir to get
        # more helpful error if program does not exist?
        self._process = subprocess.Popen(
            self._args,
            env=self._env,
            stdout=stdout,
            stderr=stderr,
            cwd=workdir
        )
        self._process_api = psutil.Process(self._process.pid)
        if port_discovery != ProcessPortDiscovery.NONE:
            await self._wait_for_listen_ports(
                port_discovery,
                port_expected_count,
                port_poll_interval,
                port_timeout
            )

        self._state = ProcessState.RUNNING
        self._poll_task = self._loop.create_task(self._monitor())
        self._log.debug(
            'Process started, pid: %s, ports: %s',
            self._process.pid, self._ports
        )

    async def kill(self):
        """Kill the process (mercilessly)."""
        # TODO: recursive kill?
        if self._state not in (ProcessState.PENDING, ProcessState.RUNNING):
            return
        assert self._process is not None
        self._log.debug('Killing the process.')
        self._process.kill()
        if self._poll_task is not None:
            await self._poll_task

    async def wait(self):
        """Block until the process has finished."""
        while self._state in (ProcessState.PENDING, ProcessState.RUNNING):
            async with self._state_changed:
                await self._state_changed.wait()

    def notify(self, ports):
        """Notify process that it is ready and running.

        Args:
            ports: List of ports used by process.
        """
        if self._state != ProcessState.PENDING:
            return
        if self._port_notification is None:
            raise ProcessError(
                'process can be notified only in EXTERNAL port discovery mode'
            )
        for port in ports:
            assert isinstance(port, int)
            assert 0 < port <= 65535
        # Make a shallow copy to be sure that port list never changes.
        self._port_notification.set_result(list(ports))

    async def _monitor(self):
        """Monitor the process status. Should be run as an async task."""
        assert self._state == ProcessState.RUNNING
        while self._process.poll() is None:
            # TODO: Additional process monitoring goes here, e.g.
            #   RSS memory: self._process_api.memory_info().rss
            # (don't forget to wrap each separate psapi call in try/catch)
            await asyncio.sleep(self._poll_interval, loop=self._loop)
        if self._process.returncode:
            self._log.warning(
                'Process finished with non-zero exit code (%s)',
                self._process.returncode
            )
        else:
            self._log.debug('Process finished')
        self._state = ProcessState.FINISHED
        self._clear()
        try:
            await self._on_finished.send(self)
        except Exception:
            self._log.exception('on_finished callback failed')
        async with self._state_changed:
            self._state_changed.notify_all()

    async def _wait_for_listen_ports(self, discovery, expected_count,
                                     poll_interval, timeout):
        """Wait for process to bind the tcp listen port."""
        assert poll_interval >= 0
        try:
            # Loop clock expected to be a proper monotonic time source.
            start = self._loop.time()
            while self._process.poll() is None:
                if timeout is not None and self._loop.time() - start > timeout:
                    raise ProcessStartTimeoutError(
                        'process bootstrap timed out'
                    )
                ports_found = await self._get_listen_ports(
                    discovery, expected_count, poll_interval
                )
                if ports_found:
                    assert len(self._ports) == expected_count
                    break
                else:
                    continue
            if len(self._ports) != expected_count:
                raise ProcessExitedError(
                    'process died before binding required ports'
                )
        except Exception as ex:
            self._log.exception('Process failed to start')
            # TODO: recursive kill?
            if self._process.returncode is None:
                self._process.kill()
            self._state = ProcessState.FINISHED
            self._clear()
            raise

    async def _get_listen_ports(self, discovery_type,
                                expected_count, poll_interval):
        if discovery_type == ProcessPortDiscovery.POLL_CONNECTIONS:
            # Sleep immediately (i.e. at the start of the loop),
            # as child process is extremely unlikely to bind the port
            # in the time it takes this process to handle a few lines
            # of python above.
            await asyncio.sleep(poll_interval, loop=self._loop)
            try:
                listen_sockets = [
                    conn for conn in self._process_api.connections(
                        self.EXPECTED_SOCKET_TYPE
                    )
                    if conn.status == psutil.CONN_LISTEN
                ]
            # When process is dying, we can get AccessDenied errors.
            # There are cases when we get legitimate access errors
            # (e.g. running smth with suid bit set like ping),
            # but we're unlikely to distinguish them from
            # concurrency issues.
            #
            # If access is actually denied, our only hope is timeout.
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                return False
            if len(listen_sockets) == expected_count:
                self._ports = [
                    connection.laddr[1]
                    for connection in listen_sockets
                ]
                return True
            elif len(listen_sockets) < expected_count:
                # Not yet, just wait and retry.
                return False
            else:
                # Not really reliable branch, due to uncontrollable
                # concurrency we can always detect a some bound ports of
                # many and continue with 'successfull' start.
                raise ProcessPortCountError('too many listen ports are open')
        elif discovery_type == ProcessPortDiscovery.EXTERNAL:
            assert self._port_notification is not None
            done, _ = await asyncio.wait(
                [self._port_notification], timeout=poll_interval
            )
            if done:
                ports = self._port_notification.result()
                if len(ports) != expected_count:
                    raise ProcessPortCountError('wrong number of open ports')
                self._ports = ports
                return True
            return False

    def _clear(self):
        """Clears the transient state of the process."""
        self._process = None
        self._process_api = None
        self._ports = []
        self._port_notification = None
        self._poll_task = None
