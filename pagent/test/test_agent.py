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


import filecmp
import http
import io
import logging
import os
import pathlib
import pickle
import sys
import tarfile
import uuid

import multidict
import pytest

import prpc
from pagent import agent_service, identity, jobs


async def read_from_stream(stream, max_chunk_size=None):
    buffer = io.BytesIO()
    async for msg in stream:
        assert isinstance(msg, bytes)
        if max_chunk_size:
            assert 0 < len(msg) <= max_chunk_size
        buffer.write(msg)
    return buffer.getvalue()


async def http_get_request(connection, job_uid, path,
                           query=None, headers=None):
    query = query or []
    headers = headers or []
    async with connection.call_bistream(
        agent_service.AgentService.http_request.__name__,
        [job_uid, 'GET', path, query, headers]
    ) as call:
        await call.stream.send(b'')
        response = await http_read_response(call)
    return response


async def http_read_response(call):
    status = await call.stream.receive()
    headers = await call.stream.receive()
    body = await read_from_stream(call.stream)
    await call.result
    return status, headers, body


async def download_file(connection, job_uid, filename, decode=True):
    async with connection.call_istream(
        agent_service.AgentService.file_download.__name__,
        [job_uid, filename]
    ) as call:
        header = await call.stream.receive()
        if header is None:
            await call.result
        data = await read_from_stream(call.stream)
        bytes_sent = await call.result
        assert bytes_sent == len(data)
        if decode:
            return data.decode(sys.stdout.encoding)
        else:
            return data


async def job_output(connection, job_uid, logger):
    SEPARATOR = '-' * 40
    stdout = await download_file(
        connection,
        job_uid,
        jobs.Job.FILENAME_STDOUT
    )
    logger.info(
        'Job stdout:\n%s\n%s',
        SEPARATOR,
        stdout
    )
    logger.info('End of job stdout\n%s', SEPARATOR)
    stderr = await download_file(
        connection,
        job_uid,
        jobs.Job.FILENAME_STDERR
    )
    logger.info(
        'Job stderr:\n%s\n%s',
        SEPARATOR,
        stderr
    )
    logger.info('End of job stderr\n%s', SEPARATOR)
    return stdout, stderr


async def get_environment(agent_process, env, properties):
    async with agent_process('--set', 'properties=' + repr(properties)) as agent:
        # Create a job.
        job_info = await agent.connection.call_simple(
            agent_service.AgentService.job_create.__name__
        )
        job_uid = job_info['uid']
        try:
            await agent.connection.call_simple(
                agent_service.AgentService.job_start.__name__,
                job_uid,
                [
                    sys.executable, '-c',
                    'import os, pickle; '
                    'pickle.dump(dict(os.environ), open("env.pickle", "wb")); '
                    'print("Successfull environment dump")'
                ],
                env,
                port_expected_count=0,
                forward_stdout=False
            )
            await agent.connection.call_simple(
                agent_service.AgentService.job_wait.__name__,
                job_uid
            )
            await job_output(
                agent.connection,
                job_uid,
                logging.getLogger('EnvironmentJob')
            )
            job_env = pickle.loads(
                await download_file(
                    agent.connection, job_uid, 'env.pickle', decode=False
                )
            )
            return job_env
        finally:
            await agent.connection.call_simple(
                agent_service.AgentService.job_remove.__name__, job_uid
            )


class HTTPServerJob(object):
    ROUTE_HELLO = '/'
    ROUTE_QUERY_ECHO = '/query_echo'
    ROUTE_UPLOAD = '/upload'
    ROUTE_SHUTDOWN = '/shutdown'
    ROUTE_WS_ECHO = '/ws_echo'
    ROUTE_WS_STREAM = '/ws_stream'
    ROUTE_STATIC = '/static'

    HELLO_RESPONSE = b'hello world!'
    WS_STREAM_MESSAGE_COUNT = 100

    def __init__(self, connection, command):
        self._connection = connection
        self._command_args = command
        self._log = logging.getLogger(type(self).__name__)
        self._job_uid = None

    @property
    def job_uid(self):
        return self._job_uid

    async def __aenter__(self):
        # Create a job.
        job_info = await self._connection.call_simple(
            agent_service.AgentService.job_create.__name__
        )
        self._job_uid = job_info['uid']
        # Start the job.
        await self._connection.call_simple(
            agent_service.AgentService.job_start.__name__,
            self._job_uid, self._command_args, None,
            port_expected_count=1,
            forward_stdout=False
        )
        return self

    async def __aexit__(self, *ex_tuple):
        # Shutdown the server.
        await http_get_request(
            self._connection,
            self._job_uid,
            self.ROUTE_SHUTDOWN
        )
        await job_output(
            self._connection,
            self._job_uid,
            self._log
        )
        # Remove job (and its workdir).
        await self._connection.call_simple(
            agent_service.AgentService.job_remove.__name__,
            self._job_uid
        )
        return False


@pytest.mark.async_test
async def test_handshake(event_loop, agent_process):
    """Test various handshake scenarios."""
    # TODO: implement 'agent info' in handshake response and validate it.
    async with agent_process(connect=False) as agent:
        connection = prpc.Connection()
        # Missing handshake data.
        with pytest.raises(prpc.RpcConnectionClosedError):
            await prpc.platform.ws_aiohttp.connect(
                connection, str(agent.endpoint_agent),
                handshake_data=None
            )
        # Invalid token.
        with pytest.raises(prpc.RpcConnectionClosedError):
            handshake = agent.make_authorized_identity()
            handshake[identity.KEY_AUTH][identity.KEY_TOKEN] = 'invalid'
            await prpc.platform.ws_aiohttp.connect(
                connection, str(agent.endpoint_agent),
                handshake_data=handshake
            )
        # Positive case.
        handshake = agent.make_authorized_identity()
        await prpc.platform.ws_aiohttp.connect(
            connection, str(agent.endpoint_agent),
            handshake_data=handshake
        )


@pytest.mark.async_test
async def test_run_script(event_loop, agent_process):
    """Test batch job execution.
    """
    SCRIPT_NAME = 'run.py'
    SCRIPT_OUTPUT = 'it seems to work'
    SCRIPT_BODY = '''print('%s', end='')''' % (SCRIPT_OUTPUT,)
    async with agent_process() as agent:
        # Create a job.
        job_info = await agent.connection.call_simple(
            agent_service.AgentService.job_create.__name__
        )
        # Check that we have 1 job in current connection.
        job_count = await agent.connection.call_simple(
            agent_service.AgentService.job_count_current_connection.__name__
        )
        assert job_count == 1
        # Upload script.
        async with agent.connection.call_ostream(
            agent_service.AgentService.file_upload.__name__,
            [job_info['uid'], SCRIPT_NAME]
        ) as call:
            data = SCRIPT_BODY.encode()
            await call.stream.send(data)
            bytes_written = await call.result
            assert bytes_written == len(data)
        # Start the job.
        await agent.connection.call_simple(
            agent_service.AgentService.job_start.__name__,
            job_info['uid'], [sys.executable, SCRIPT_NAME], None,
            port_expected_count=0,
            forward_stdout=False
        )
        # Wait until it is completed.
        await agent.connection.call_simple(
            agent_service.AgentService.job_wait.__name__,
            job_info['uid']
        )
        stdout, _ = await job_output(
            agent.connection,
            job_info['uid'],
            logging.getLogger('Script')
        )
        assert stdout == SCRIPT_OUTPUT
        # Remove job (and its workdir).
        await agent.connection.call_simple(
            agent_service.AgentService.job_remove.__name__,
            job_info['uid']
        )
        # Check that we have no jobs in current connection.
        job_count = await agent.connection.call_simple(
            agent_service.AgentService.job_count_current_connection.__name__
        )
        assert job_count == 0


@pytest.mark.async_test
async def test_server_get(event_loop, agent_process,
                          http_server_command):
    """Test basic proxy features - GET request."""
    async with agent_process() as agent:
        http_job = HTTPServerJob(agent.connection, http_server_command())
        async with http_job:
            # Positive case.
            status, headers, body = await http_get_request(
                agent.connection, http_job.job_uid, HTTPServerJob.ROUTE_HELLO
            )
            headers = dict(headers)
            assert status == http.HTTPStatus.OK
            assert 'Content-Type' in headers
            assert 'Content-Length' in headers
            assert 'Date' in headers
            assert 'Server' in headers
            assert 'TestHeader' in headers
            assert body == HTTPServerJob.HELLO_RESPONSE
            # Not found.
            status, headers, body = await http_get_request(
                agent.connection, http_job.job_uid, '/invalid-route'
            )
            assert status == http.HTTPStatus.NOT_FOUND
            assert str(http.HTTPStatus.NOT_FOUND.value).encode() in body
            # POST-only route.
            status, headers, body = await http_get_request(
                agent.connection, http_job.job_uid, HTTPServerJob.ROUTE_UPLOAD
            )
            assert status == http.HTTPStatus.METHOD_NOT_ALLOWED
            assert (
                str(http.HTTPStatus.METHOD_NOT_ALLOWED.value).encode() in body
            )


@pytest.mark.async_test
async def test_server_post(event_loop, agent_process,
                           http_server_command):
    """Test basic proxy features - POST request."""
    async with agent_process() as agent:
        http_job = HTTPServerJob(agent.connection, http_server_command())
        async with http_job:
            async with agent.connection.call_bistream(
                agent_service.AgentService.http_request.__name__,
                [http_job.job_uid, 'POST', HTTPServerJob.ROUTE_UPLOAD, [], []]
            ) as call:
                total_size = 0
                for _ in range(1000):
                    chunk = uuid.uuid4().bytes * 1024
                    await call.stream.send(chunk)
                    total_size += len(chunk)
                await call.stream.send(b'')
                status, headers, body = await http_read_response(call)
                assert status == http.HTTPStatus.OK
                assert int(body) == total_size


@pytest.mark.async_test
async def test_server_query(event_loop, agent_process,
                            http_server_command):
    """Test basic proxy features - GET request."""
    TEST_QUERY = [
        ('key', 'value'),
        ('repeated_ley', 'value1'),
        ('repeated_ley', 'value2'),
        ('key_equals', 'not=fun')
    ]
    async with agent_process() as agent:
        http_job = HTTPServerJob(agent.connection, http_server_command())
        async with http_job:
            # Empty query.
            status, headers, body = await http_get_request(
                agent.connection, http_job.job_uid,
                HTTPServerJob.ROUTE_QUERY_ECHO,
                query=[]
            )
            assert status == http.HTTPStatus.OK
            assert body == b''
            status, headers, body = await http_get_request(
                agent.connection, http_job.job_uid,
                HTTPServerJob.ROUTE_QUERY_ECHO,
                query=TEST_QUERY
            )
            assert status == http.HTTPStatus.OK
            query = multidict.MultiDict(
                [
                    tuple(line.split('=', 1))
                    for line in body.decode().split('\n')
                    if line
                ]
            )
            assert multidict.MultiDict(TEST_QUERY) == query


@pytest.mark.async_test
async def test_server_ws_echo(event_loop, agent_process,
                              http_server_command):
    """Test WS proxy features - echo service."""
    ECHO_DATA = (
        uuid.uuid4().bytes,
        uuid.uuid4().hex,
        b'',
        ''
    )
    async with agent_process() as agent:
        http_job = HTTPServerJob(agent.connection, http_server_command())
        async with http_job:
            async with agent.connection.call_bistream(
                agent_service.AgentService.ws_connect.__name__,
                [http_job.job_uid, HTTPServerJob.ROUTE_WS_ECHO, [], []]
            ) as call:
                connected = await call.stream.receive()
                assert connected
                for data in ECHO_DATA:
                    data = uuid.uuid4().bytes
                    await call.stream.send(data)
                    echo = await call.stream.receive()
                    assert data == echo
                await call.result


@pytest.mark.async_test
async def test_server_ws_stream(event_loop, agent_process,
                                http_server_command):
    """Test WS proxy features - incoming data stream.

    Important difference with ws_echo test is that the stream
    is closed by remote server.
    """
    async with agent_process() as agent:
        http_job = HTTPServerJob(agent.connection, http_server_command())
        async with http_job:
            async with agent.connection.call_bistream(
                agent_service.AgentService.ws_connect.__name__,
                [http_job.job_uid, HTTPServerJob.ROUTE_WS_STREAM, [], []]
            ) as call:
                connected = await call.stream.receive()
                assert connected
                count = len([msg async for msg in call.stream])
                assert count == HTTPServerJob.WS_STREAM_MESSAGE_COUNT
                await call.result


@pytest.mark.async_test
async def test_server_ws_rejected(event_loop, agent_process,
                                  http_server_command):
    """Test WS proxy features - failed/rejected connection."""
    PATHS = [
        # Endpoint doesn't exist.
        '/does_not_exist',
        # GET endpoint without WS support.
        HTTPServerJob.ROUTE_HELLO,
        # POST-only endpoint.
        HTTPServerJob.ROUTE_UPLOAD
    ]
    async with agent_process() as agent:
        http_job = HTTPServerJob(agent.connection, http_server_command())
        async with http_job:
            for path in PATHS:
                async with agent.connection.call_bistream(
                    agent_service.AgentService.ws_connect.__name__,
                    [http_job.job_uid, path, [], []]
                ) as call:
                    connected = await call.stream.receive()
                    assert not connected
                    eof = await call.stream.receive()
                    assert eof is None
                    assert call.stream.is_closed
                    with pytest.raises(prpc.RpcMethodError):
                        await call.result


@pytest.mark.async_test
async def test_environment(event_loop, agent_process):
    """Test environment variables mapping (as command args and as properties).
    """
    TEST_ENV = {
        'ASCII FOREVER 1': 'ASCII FOREVER',
        'ASCII FOREVER 2': 'ЮНИКОД großer',
        'ЮНИКОД èüîû 1 god why': 'ASCII FOREVER',
        'ЮНИКОД èüîû 2 god why': 'ЮНИКОД großer èüîû god why',
    }
    # Win32 env is case-insensitive, all variables will be uppercased by system.
    if sys.platform == 'win32':
    	TEST_ENV = {k.upper(): v for (k, v) in TEST_ENV.items()}
    job_env = await get_environment(
        agent_process, TEST_ENV, TEST_ENV
    )
    # Properties are mapped with prefix.
    properties_env = {
        k[len(jobs.JobManager.PREFIX_PROPERTIES):]: v
        for (k, v) in job_env.items()
        if k.startswith(jobs.JobManager.PREFIX_PROPERTIES)
    }
    # Command env is added directly.
    assert properties_env == TEST_ENV
    for key, value in TEST_ENV.items():
        assert job_env[key] == value


@pytest.mark.async_test
async def test_archive_upload_download(event_loop, agent_process, tmpdir):
    async def get_archive(path, include_mask=None,
                          exclude_mask=None, compress=False):
        path = str(path)
        arcpath = path + '.tar'
        async with agent.connection.call_istream(
            agent_service.AgentService.archive_download.__name__,
            [job_info['uid'], include_mask, exclude_mask, compress]
        ) as call:
            header = await call.stream.receive()
            if header is None:
                await call.result
            data = await read_from_stream(call.stream)
            with open(arcpath, 'wb') as arc:
                arc.write(data)
        # Unpack it.
        with tarfile.open(arcpath, 'r') as arc:
            arc.extractall(path)

    def compare_dirs(lhs, rhs):
        assert os.path.isdir(lhs)
        assert os.path.isdir(rhs)
        dircmp = filecmp.dircmp(lhs, rhs)
        assert not dircmp.left_only
        assert not dircmp.right_only
        for filename in dircmp.common_files:
            with open(lhs.joinpath(filename), 'rb') as lhs_file:
                with open(rhs.joinpath(filename), 'rb') as rhs_file:
                    assert lhs_file.read() == rhs_file.read()
        for dirname in dircmp.common_dirs:
            compare_dirs(lhs.joinpath(dirname), rhs.joinpath(dirname))
    # Generate test files.
    DATA_PATH = pathlib.Path(tmpdir.mkdir('data'))
    FILES = [
        'something.py',
        'whatever.dat',
        'großer/stuff.txt',
        'nested/èüîû god why.py'
    ]
    UPLOAD_PATH = pathlib.Path(tmpdir.join('upload.tar'))
    DOWNLOAD_FULL = pathlib.Path(tmpdir.join('download_full'))
    DOWNLOAD_INCLUDE_PY = pathlib.Path(tmpdir.join('download_py'))
    DOWNLOAD_EXCLUDE = pathlib.Path(tmpdir.join('download_exclude'))
    for filename in FILES:
        target = DATA_PATH.joinpath(filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, 'w') as target_file:
            target_file.write(uuid.uuid4().hex * 10)
    with tarfile.open(UPLOAD_PATH, 'w') as arc:
        for file_path in DATA_PATH.iterdir():
            arc.add(file_path, arcname=file_path.name)
    async with agent_process() as agent:
        # Create a job.
        job_info = await agent.connection.call_simple(
            agent_service.AgentService.job_create.__name__
        )
        # Upload archive.
        async with agent.connection.call_ostream(
            agent_service.AgentService.archive_upload.__name__,
            [job_info['uid']]
        ) as call:
            with open(UPLOAD_PATH, 'rb') as archive:
                await call.stream.send(archive.read())
            await call.result
        # Download full archive.
        await get_archive(DOWNLOAD_FULL, compress=True)
        compare_dirs(DOWNLOAD_FULL, DATA_PATH)

        # Download filtered archive.
        await get_archive(DOWNLOAD_INCLUDE_PY, '*.py')
        files_to_remove = (
            set(FILES) - set(['something.py', 'nested/èüîû god why.py'])
        )
        for filtered_file in files_to_remove:
            path = DATA_PATH.joinpath(filtered_file)
            path.unlink()
            if not os.listdir(path.parent):
                path.parent.rmdir()
        compare_dirs(DOWNLOAD_INCLUDE_PY, DATA_PATH)

        await get_archive(DOWNLOAD_EXCLUDE, '*.py', 'something*')
        DATA_PATH.joinpath('something.py').unlink()
        compare_dirs(DOWNLOAD_EXCLUDE, DATA_PATH)

        # Remove job (and its workdir).
        await agent.connection.call_simple(
            agent_service.AgentService.job_remove.__name__,
            job_info['uid']
        )
