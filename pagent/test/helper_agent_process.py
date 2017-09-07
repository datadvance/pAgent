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
import http
import logging
import pathlib
import sys
import uuid

import aiohttp
import prpc
import yarl

from pagent import agent_app
from pagent import control_app
from pagent import job_app
from pagent import identity
from pagent import polled_process


LOCALHOST_WS_URL = yarl.URL("ws://127.0.0.1")


class AgentProcess(object):
    def __init__(self, *args, connect=True, loop=None):
        self._log = logging.getLogger('AgentProcess')
        self._loop = loop
        self._connect = connect
        self._accepted_token = uuid.uuid4().hex
        process_args = [sys.executable, '-m', 'pagent']
        process_args.extend([
            '--connection-debug'
        ])
        process_args.extend([
            '--set',
            'server.accept_tokens=["%s"]' % (self._accepted_token,)
        ])
        process_args.extend(args)
        self._process = polled_process.PolledProcess(
            process_args,
            None,
            loop=self._loop
        )
        self._session = None
        self._connection = None
        self._port_control = None
        self._port_agent = None
        self._port_jobs = None
        self._running = False

    @property
    def process(self):
        return self._process

    @property
    def running(self):
        return self._running

    @property
    def endpoint_control(self):
        return LOCALHOST_WS_URL.with_port(self._port_control)

    @property
    def endpoint_agent(self):
        return LOCALHOST_WS_URL.with_port(self._port_agent).with_path(
            agent_app.ROUTE_RPC_SERVER
        )

    @property
    def connection(self):
        return self._connection

    @property
    def session(self):
        return self._session

    @property
    def accepted_token(self):
        return self._accepted_token

    def make_authorized_identity(self):
        return {
            identity.KEY_AUTH: {
                identity.KEY_NAME: 'pytest',
                identity.KEY_UID: uuid.uuid4().bytes,
                identity.KEY_TOKEN: self._accepted_token
            }
        }

    async def __aenter__(self):
        self._log.info('Starting agent process')
        await self._process.start(
            workdir=pathlib.Path(__file__).absolute().parents[2],
            port_expected_count=3
        )
        self._session = aiohttp.ClientSession(loop=self._loop)
        try:
            assert len(self._process.ports) == 3
            self._log.info('Startup successful, detecting control port')
            ports = list(self._process.ports)
            for port in self._process.ports:
                response = await self._session.get(
                    LOCALHOST_WS_URL.with_port(port).with_path(
                        control_app.ROUTE_INFO
                    )
                )
                if response.status != http.HTTPStatus.NOT_FOUND:
                    self._port_control = port
                    break
            assert self._port_control is not None
            ports.remove(self._port_control)
            self._log.info('Detecting job feedback port')
            for port in self._process.ports:
                response = await self._session.get(
                    LOCALHOST_WS_URL.with_port(port).with_path(
                        job_app.ROUTE_JOB_STARTED
                    )
                )
                if response.status != http.HTTPStatus.NOT_FOUND:
                    self._port_jobs = port
                    break
            assert self._port_jobs is not None
            ports.remove(self._port_jobs)
            self._port_agent, = ports
            self._log.info('Control port: %d', self._port_control)
            self._log.info('Agent port:   %d', self._port_agent)
            self._log.info('Jobs port:    %d', self._port_jobs)
            if self._connect:
                self._log.info('Connecting to agent')
                self._connection = prpc.Connection(debug=True)
                await prpc.platform.ws_aiohttp.connect(
                    self._connection, str(self.endpoint_agent),
                    handshake_data=self.make_authorized_identity()
                )
        except Exception:
            await self._process.kill()
            raise
        self._running = True
        return self

    async def __aexit__(self, *ex_args):
        KILL_DELAY = 5.
        if self._process.state == polled_process.ProcessState.RUNNING:
            if self._connection:
                await self._connection.close()
            self._log.debug('Sending shutdown command using control API')
            await self._session.post(
                self.endpoint_control.with_path(control_app.ROUTE_SHUTDOWN)
            )
            try:
                await asyncio.wait_for(
                    self._process.wait(), KILL_DELAY, loop=self._loop
                )
                self._log.debug('Agent finalized successfully')
            except asyncio.TimeoutError:
                await self._process.kill()
        self._port_control = None
        self._port_agent = None
        self._port_jobs = None
        self._session.close()
        self._running = False
        return False
