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

import prpc

from . import common


KEY_AGENT_SERVICE = 'agent_service'


async def accept_connection(request):
    """Handle an incoming rpc connection."""
    identity = request.app[common.KEY_IDENTITY]
    conn_mgr = request.app[common.KEY_CONN_MANAGER]
    agent_service = request.app[KEY_AGENT_SERVICE]

    connection = prpc.Connection(agent_service, debug=conn_mgr.debug)

    async def on_connected(connection, handshake):
        identity.validate_incoming_handshake(handshake)
        conn_mgr.register(connection, handshake)

    return await prpc.platform.ws_aiohttp.accept(
        connection, request,
        handshake_data=identity.get_server_handshake(),
        connect_callback=on_connected
    )
