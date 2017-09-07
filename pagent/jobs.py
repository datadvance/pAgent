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
import collections
import enum
import logging
import os
import pathlib
import shutil
import tempfile
import uuid

from prpc.utils import repr_builder

from . import polled_process


class JobNotFoundError(Exception):
    """JobManager API called with invalid job uid."""


class JobManagerInvalidStateError(Exception):
    """Operation is incompatible with the current job manager state."""


class JobInvalidStateError(Exception):
    """Operation is incompatible with the current job state."""


Sender = collections.namedtuple(
    'Sender', ['direction', 'connection', 'uid', 'name', 'token']
)


JobInfo = collections.namedtuple(
    'JobInfo', ['uid', 'name', 'state', 'sender']
)


@enum.unique
class JobManagerState(enum.Enum):
    """JobManager FSM states."""
    RUNNING = enum.auto()
    CLOSED = enum.auto()


@enum.unique
class JobState(enum.Enum):
    """Job FSM states."""
    NEW = enum.auto()
    PENDING = enum.auto()
    RUNNING = enum.auto()
    FINISHED = enum.auto()
    CLOSED = enum.auto()


class JobManager(object):
    DEFAULT_WORKDIR_PREFIX = 'pagent-'
    DEFAULT_LOG_NAME = 'pagent.JobManager'
    DEFAULT_JOB_NAME = '<unnamed job>'

    PREFIX_PROPERTIES = 'DA_PAGENT_'
    PREFIX_COMMAND = 'DA_P7_'

    PROPERTY_JOB_ENDPOINT = 'JOB_ENDPOINT'
    PROPERTY_JOB_ID = 'JOB_ID'

    def __init__(self, workdir_root=None, properties=None,
                 logger=None, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._log = logger or logging.getLogger(self.DEFAULT_LOG_NAME)
        self._properties = properties or {}
        self._job_start_endpoint = None
        if workdir_root is not None:
            workdir_root = os.fspath(workdir_root)
        self._session_root = pathlib.Path(
            tempfile.mkdtemp(
                prefix=self.DEFAULT_WORKDIR_PREFIX,
                dir=workdir_root
            )
        )

        self._jobs = {}
        self._jobs_by_connection = collections.defaultdict(dict)
        self._state = JobManagerState.RUNNING
        # TODO: Jobs priority queue for cleanup?
        # TODO: As an idea, cleanup old session roots?
        self._log.debug(
          'Initialized, workdir root: %s' % (self._session_root,)
        )

    async def close(self):
        if self._state == JobManagerState.CLOSED:
            return
        self._state = JobManagerState.CLOSED
        # File operations may take a while, so issue double log message.
        self._log.debug("Shutting down")
        for job in self._jobs.values():
            # TODO: Kill jobs recursively?
            await job.close()
        self._jobs.clear()
        self._jobs_by_connection.clear()
        # Can be done synchronously,
        # we are not accepting any new requests by now.
        shutil.rmtree(str(self._session_root), ignore_errors=True)
        self._log.debug("Shutdown complete")

    def set_job_start_endpoint(self, job_endpoint):
        self._job_start_endpoint = job_endpoint

    def get_jobs(self):
        return list(self._jobs.values())

    async def job_remove_all_by_connection(self, connection_id):
        if connection_id not in self._jobs_by_connection:
            return
        to_remove = list(self._jobs_by_connection[connection_id].values())
        for job in to_remove:
            await self.job_remove(job.uid)

    def job_count_by_connection(self, connection_id):
        # Just len(self._jobs_by_connection[connection_id]) would
        # work too, but it can insert unwanted values. So let's check
        # connection id separately.
        if connection_id not in self._jobs_by_connection:
            return 0
        return len(self._jobs_by_connection[connection_id])

    def job_create(self, sender, name=None):
        self._require_running()
        name = name or self.DEFAULT_JOB_NAME
        job_id = uuid.uuid4().hex
        assert job_id not in self._jobs
        assert sender is not None
        assert sender.connection
        job = Job(
            job_id,
            name,
            str(self._session_root.joinpath(job_id)),
            sender,
            self._loop
        )
        self._jobs[job_id] = job
        self._jobs_by_connection[sender.connection][job_id] = job
        self._log.debug('Created job %s', job)
        return job_id

    async def job_remove(self, uid):
        self._require_running()
        job = self._get_job(uid)
        await job.close()
        del self._jobs[uid]
        del self._jobs_by_connection[job.sender.connection][uid]
        if len(self._jobs_by_connection[job.sender.connection]) == 0:
            del self._jobs_by_connection[job.sender.connection]
        self._log.debug('Removed job %s', job)

    async def job_start(self, uid, args, env, port_expected_count):
        self._require_running()
        if port_expected_count and self._job_start_endpoint is None:
            raise RuntimeError(
                'cannot run server job: '
                'job start endpoint is not set'
            )
        process_env = dict(os.environ)
        # TODO: Make properties case-insensitve, forced uppercase?
        self._extend_with_prefix(
            process_env,
            self._properties,
            self.PREFIX_PROPERTIES
        )
        if port_expected_count:
            self._extend_with_prefix(
                process_env,
                {
                    self.PROPERTY_JOB_ENDPOINT: self._job_start_endpoint,
                    self.PROPERTY_JOB_ID: str(uid)
                },
                self.PREFIX_PROPERTIES
            )
        if env is not None:
            self._extend_with_prefix(process_env, env, self.PREFIX_COMMAND)

        await self._get_job(uid).start(args, process_env, port_expected_count)
        # TODO: Support job queueing?
        # (and finite amount of job slots on an agent instance)

    async def job_kill(self, uid):
        self._require_running()
        await self._get_job(uid).kill()

    async def job_wait(self, uid):
        self._require_running()
        await self._get_job(uid).wait()

    def job_workdir(self, uid):
        self._require_running()
        # TODO: job lock?
        return self._get_job(uid).workdir

    def job_port(self, uid):
        self._require_running()
        return self._get_job(uid).port

    def job_info(self, uid):
        self._require_running()
        job = self._get_job(uid)
        return JobInfo(job.uid, job.name, job.state, job.sender)

    def job_notify(self, uid, port):
        self._require_running()
        self._get_job(uid).notify([port])

    def _require_running(self):
        if self._state != JobManagerState.RUNNING:
            raise JobManagerInvalidStateError("job manager is not running")

    def _get_job(self, uid):
        """Aux - get job instance by id."""
        try:
            return self._jobs[uid]
        except KeyError:
            raise JobNotFoundError('job "%s" is not found' % (uid,))

    @staticmethod
    def _extend_with_prefix(base, extensions, prefix):
        for key, value in extensions.items():
            base[prefix + key] = value

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        await self.close()
        return False


class Job(object):
    FILENAME_STDOUT = '.pagent.stdout'
    FILENAME_STDERR = '.pagent.stderr'

    def __init__(self, uid, name, workdir, sender, loop):
        # TODO: Add some timestamps (at least 'created').
        # May be done later, when we'll add UI.
        self._uid = uid
        self._name = name
        self._workdir = pathlib.Path(workdir)
        self._sender = sender
        self._loop = loop

        # TODO: Job locking for async operations?
        self._state = JobState.NEW
        self._process = None

        if self._workdir.exists():
            # For now, we demand a new workdir for each job.
            # This nice class runs 'rm -rf' sometimes, so
            # it better be on a temp directory.
            raise ValueError('working directory already exists')
        else:
            pathlib.Path(self._workdir).mkdir(parents=True)

    @property
    def uid(self):
        return self._uid

    @property
    def name(self):
        return self._name

    @property
    def workdir(self):
        return self._workdir

    @property
    def sender(self):
        return self._sender

    @property
    def port(self):
        if self._state == JobState.RUNNING:
            return self._process.port
        return None

    @property
    def state(self):
        return self._state

    async def start(self, args, env, port_expected_count):
        if self._state != JobState.NEW:
            raise JobInvalidStateError('job cannot be restarted')
        self._process = polled_process.PolledProcess(
            args, env
        )
        self._process.on_finished.append(self._process_finished)
        self._state = JobState.PENDING
        stdout_path = self.workdir.joinpath(self.FILENAME_STDOUT)
        stderr_path = self.workdir.joinpath(self.FILENAME_STDERR)
        if port_expected_count:
            port_discovery = polled_process.ProcessPortDiscovery.EXTERNAL
        else:
            port_discovery = polled_process.ProcessPortDiscovery.NONE
        try:
            with open(stdout_path, 'wb') as stdout:
                with open(stderr_path, 'wb') as stderr:
                    await self._process.start(
                        stdout=stdout, stderr=stderr,
                        port_discovery=port_discovery,
                        workdir=str(self.workdir),
                        port_expected_count=port_expected_count
                    )
                    self._state = JobState.RUNNING
        except Exception:
            self._state = JobState.FINISHED
            raise

    async def kill(self):
        if self._state in (JobState.PENDING, JobState.RUNNING):
            await self._process.kill()
        else:
            raise JobInvalidStateError('job is not running')

    async def wait(self):
        if self._state in (JobState.PENDING, JobState.RUNNING):
            await self._process.wait()

    def notify(self, ports):
        if self._state == JobState.PENDING:
            self._process.notify(ports)

    async def close(self):
        if self._state == JobState.CLOSED:
            return
        if self._state in (JobState.PENDING, JobState.RUNNING):
            await self._process.kill()
        # Important - run remove workdir in executor so we don't block
        # the event loop with this potentially long process.
        await self._loop.run_in_executor(None, self._remove_workdir)
        self._state = JobState.CLOSED

    def _remove_workdir(self):
        if self._workdir.exists():
            shutil.rmtree(str(self._workdir), ignore_errors=True)

    def _process_finished(self, process):
        """Process on_finished signal handler."""
        self._state = JobState.FINISHED

    def __repr__(self):
        """Aux - string representation for debug and logging."""
        return (
            repr_builder.ReprBuilder(self)
            .add_value('uid', self._uid)
            .add_value('state', self._state.name)
            .add_value('workdir', str(self._workdir))
            .add_value('sender_uid', self._sender.uid)
            .format()
        )
