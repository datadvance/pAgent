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
import logging
import pathlib

import pytest
import yarl

import prpc
from pagent import job_app, jobs


@pytest.fixture(scope='function')
def manager(tmpdir, event_loop):
    return jobs.JobManager(
        temp_root=tmpdir,
        loop=event_loop
    )


@pytest.mark.async_test
async def test_run_job(event_loop, manager, mock_server_command):
    EXIT_DELAY = 1.
    EXIT_POLL_DELAY = 0.1
    async with manager:
        uid = manager.job_create(
            jobs.Sender(
                prpc.ConnectionMode.SERVER,
                'connection_id',
                'router_uid',
                'Router Name',
                '==token=='
            )
        )
        sandbox = manager.job_sandbox(uid)
        assert manager.job_port(uid) is None
        assert sandbox.exists()
        await manager.job_start(
            uid, mock_server_command(exit_delay=EXIT_DELAY), {}, 0, False
        )
        assert manager.job_port(uid) is None
        while manager.job_info(uid).state == jobs.JobState.RUNNING:
            await asyncio.sleep(EXIT_POLL_DELAY)
        assert manager.job_info(uid).state == jobs.JobState.FINISHED
        assert manager.job_port(uid) is None
        assert sandbox.joinpath(jobs.Job.FILENAME_STDOUT).exists()
        assert sandbox.joinpath(jobs.Job.FILENAME_STDERR).exists()
        await manager.job_remove(uid)
        assert not sandbox.exists()
