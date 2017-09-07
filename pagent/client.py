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

import prpc
import yarl


class Client(object):
    CLIENT_RPC_PATH = '/rpc/v1'
    DEFAULT_LOG_NAME = 'pagent.Client'

    def __init__(self, agent_service, identity, connection_manager,
                 address, reconnect_delay, exit_on_fail, exit_handler,
                 logger=None, loop=None):
        self._identity = identity
        self._conn_manager = connection_manager
        self._address = address
        self._url = None
        self._reconnect_delay = reconnect_delay
        self._exit_on_fail = exit_on_fail
        self._exit_handler = exit_handler

        self._log = logger or logging.getLogger(self.DEFAULT_LOG_NAME)
        self._loop = loop or asyncio.get_event_loop()

        self._connection = prpc.Connection(
            agent_service, debug=self._conn_manager.debug
        )
        self._connect_task = None
        self._disconnect_event = asyncio.Event()

        self._connection.on_close.append(self._on_disconnect)

    async def start(self):
        if self._connect_task is not None:
            raise RuntimeError('client is already running')
        if self._url is None:
            self._url = yarl.URL.build(
                scheme='http',
                host=self._address,
                path=self.CLIENT_RPC_PATH
            )
        self._connect_task = self._loop.create_task(self._connect())

    async def stop(self):
        if self._connect_task is not None:
            self._connect_task.cancel()
            await asyncio.wait([self._connect_task])
        await self._connection.close()

    async def _connect(self):
        while True:
            try:
                self._disconnect_event.clear()
                await prpc.platform.ws_aiohttp.connect(
                    self._connection, self._url,
                    handshake_data=self._identity.get_client_handshake(),
                    connect_callback=self._on_connect
                )
                self._log.info('Client connection established')
                assert self._connection.connected
                await self._disconnect_event.wait()
                self._log.info('Client connection dropped')
                self._disconnect_event.clear()
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self._log.debug(
                    'Client connection failed, retrying in %.3f s, '
                    'reason: %s',
                    self._reconnect_delay,
                    ex
                )
                if self._exit_on_fail:
                    self._log.warning('Exit on failure enabled, exiting...')
                    self._exit_handler(exit_code=1)
                # Can also raise CancelledError, which is fine.
                await asyncio.sleep(self._reconnect_delay, loop=self._loop)


    def _on_connect(self, connection, handshake):
        self._conn_manager.register(connection, handshake)

    def _on_disconnect(self, connection):
        assert connection == self._connection
        self._disconnect_event.set()
