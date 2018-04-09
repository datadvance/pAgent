#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
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

import aiohttp.web

from . import handlers


# Route listening for incoming RPC connections.
ROUTE_RPC_SERVER = '/rpc/v1'


ROUTES = [
    ('GET', ROUTE_RPC_SERVER, handlers.rpc.accept_connection)
]


def get_application(agent_service, connection_manager, identity, logger):
    app = aiohttp.web.Application(logger=logger)
    app[handlers.rpc.KEY_AGENT_SERVICE] = agent_service
    app[handlers.common.KEY_CONN_MANAGER] = connection_manager
    app[handlers.common.KEY_IDENTITY] = identity

    for method, route, handler in ROUTES:
        app.router.add_route(method, route, handler)

    return app
