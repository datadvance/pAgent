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

import aiohttp.web

from . import common


KEY_EXIT_HANDLER = 'exit_handler'


async def info(request):
    """Return info about agent instance."""
    return aiohttp.web.json_response(
        data=request.app[common.KEY_IDENTITY].get_server_handshake()
    )


async def jobs(request):
    job_manager = request.app[common.KEY_JOB_MANAGER]
    jobs = []
    for job in job_manager.get_jobs():
        job_desc = {
            'uid': job.uid,
            'name': job.name,
            'sandbox': str(job.sandbox),
            'sender': job.sender._asdict(),
            'port': job.port,
            'state': job.state.name
        }
        jobs.append(job_desc)
    return aiohttp.web.json_response(
        data={
            'jobs': jobs
        }
    )


async def connections(request):
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    connections = []
    for connection in conn_manager.get_connections():
        connection_desc = {
            'uid': connection.id,
            'mode': connection.mode.name,
            'peer': connection.handshake_data
        }
        connections.append(connection_desc)
    return aiohttp.web.json_response(
        data={
            'connections': connections
        }
    )


async def shutdown(request):
    """Handle an incoming shutdown request."""
    request.app[KEY_EXIT_HANDLER]()
    # Return valid empty response.
    return aiohttp.web.Response()
