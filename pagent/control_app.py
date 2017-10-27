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

import aiohttp.web

from . import handlers


ROUTE_INFO = "/info"
ROUTE_JOBS = "/jobs"
ROUTE_CONNECTIONS = "/connections"
ROUTE_SHUTDOWN = "/shutdown"


ROUTES = [
    ('GET', ROUTE_INFO, handlers.admin.info),
    ('GET', ROUTE_JOBS, handlers.admin.jobs),
    ('GET', ROUTE_CONNECTIONS, handlers.admin.connections),
    ('POST', ROUTE_SHUTDOWN, handlers.admin.shutdown)
]


def get_application(job_manager, connection_manager,
                    identity, exit_handler, logger):
    """Creates the control web application for agent administration."""
    app = aiohttp.web.Application(logger=logger)
    app[handlers.common.KEY_CONN_MANAGER] = connection_manager
    app[handlers.common.KEY_IDENTITY] = identity
    app[handlers.common.KEY_JOB_MANAGER] = job_manager
    app[handlers.admin.KEY_EXIT_HANDLER] = exit_handler

    app.on_response_prepare.append(handlers.signals.disable_cache)

    for method, route, handler in ROUTES:
        app.router.add_route(method, route, handler)

    return app
