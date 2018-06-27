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

import http

import aiohttp.web

from . import common


QUERY_KEY_JOB_ID = 'job_id'
QUERY_KEY_PORT = 'port'


async def job_started(request):
    """Return info about agent instance."""
    job_manager = request.app[common.KEY_JOB_MANAGER]
    try:
        if len(request.query) != 2:
            raise ValueError('too many query parameters')
        job_id = request.query.getone(QUERY_KEY_JOB_ID)
        port = request.query.getone(QUERY_KEY_PORT)
        job_manager.job_notify(job_id, int(port))
        return aiohttp.web.Response(
            text='OK'
        )
    except Exception as ex:
        return aiohttp.web.Response(
            status=http.HTTPStatus.BAD_REQUEST,
            text=str(ex)
        )
