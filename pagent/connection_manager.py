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


import logging

import prpc

from . import identity


class ConnectionManager(object):
    """Connection registry.

    Roles:
      * Register/unregister each connection
      * List all connections (for debug/UI)
      * Close all connections (on application exit)
    """

    DEFAULT_LOG_NAME = 'pagent.ConnectionManager'

    def __init__(self, debug=False, logger=None):
        self._log = logger or logging.getLogger(self.DEFAULT_LOG_NAME)
        self._debug = debug
        self._connections = {}

        self._on_connection_lost = prpc.utils.signal.Signal()

    @property
    def debug(self):
        """Config parameter enabling debug mode for all connections."""
        return self._debug

    @property
    def on_connection_lost(self):
        return self._on_connection_lost

    def get_connections(self):
        """Get all active connections."""
        return list(self._connections.values())

    def register(self, connection, handshake):
        """Try to register new connection with given handshake."""
        assert connection.mode in prpc.ConnectionMode
        assert connection.mode != prpc.ConnectionMode.NEW
        # In fact, we should wait for key for some time
        # before raising.
        #
        # However, proper implementation (condition etc)
        # is unfeasibly complicated for now and polling
        # is too ugly.
        if connection.id in self._connections:
            raise ValueError(
                'connection \'%s\' (mode: %s) is already registered' %
                (connection.id, connection.mode)
            )
        connection.on_close.append(self._unregister)
        self._connections[connection.id] = connection
        self._log.info(
            'New connection: id %s, mode: %s, peer: %s, token: %s',
            connection.id,
            connection.mode.name,
            identity.Identity.get_uid(handshake),
            identity.Identity.get_token(handshake)
        )

    async def _unregister(self, connection):
        """Unregisters connection when it is closed."""
        del self._connections[connection.id]
        self._log.info(
            'Dropped connection: id %s, mode: %s, peer: %s, token: %s',
            connection.id,
            connection.mode.name,
            identity.Identity.get_uid(connection.handshake_data),
            identity.Identity.get_token(connection.handshake_data)
        )
        connection.on_close.remove(self._unregister)
        await self._on_connection_lost.send(connection.id)
