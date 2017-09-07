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
import http
import io
import logging
import sys
import uuid

import multidict
import prpc
import pytest

from pagent import agent_service
from pagent import identity
from pagent import jobs


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
            self._job_uid, self._command_args, None, 1
        )
        return self

    async def __aexit__(self, *ex_tuple):
        # Shutdown the server.
        await http_get_request(
            self._connection,
            self._job_uid,
            self.ROUTE_SHUTDOWN
        )
        # Download the stdout.
        async with self._connection.call_istream(
            agent_service.AgentService.file_download.__name__,
            [self._job_uid, jobs.Job.FILENAME_STDOUT]
        ) as call:
            header = await call.stream.receive()
            data = await read_from_stream(call.stream)
            bytes_sent = await call.result
            assert bytes_sent == len(data)
            captured_stdout = data.decode(sys.stdout.encoding)
            self._log.info(
                'Server stdout (%.2f Kb):\n%s\n%s',
                len(data) / 1024., '-' * 40, captured_stdout
            )
            self._log.info('End of server stdout\n%s', '-' * 40)
        # Download the stderr.
        async with self._connection.call_istream(
            agent_service.AgentService.file_download.__name__,
            [self._job_uid, jobs.Job.FILENAME_STDERR]
        ) as call:
            header = await call.stream.receive()
            data = await read_from_stream(call.stream)
            bytes_sent = await call.result
            assert bytes_sent == len(data)
            captured_stderr = data.decode(sys.stdout.encoding)
            self._log.info(
                'Server stderr (%.2f Kb):\n%s\n%s',
                len(data) / 1024., '-' * 40, captured_stderr
            )
            self._log.info('End of server stderr\n%s', '-' * 40)
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

        # TODO: duplicate uid are allowed for now.
        # Review after router gets more stable.
        return

        # Valid token, but duplicate client uid.
        with pytest.raises(prpc.RpcConnectionClosedError):
            second_connection = prpc.Connection()
            await prpc.platform.ws_aiohttp.connect(
                second_connection, str(agent.endpoint_agent),
                handshake_data=handshake
            )
        # Close first connection and wait for ability to reconnect.
        #
        # TODO: should be updated if connection manger is taught to
        # wait for connection to die out.
        await connection.close()
        RECONNECT_TIMEOUT = 10.
        RECONNECT_DELAY = 0.1
        reconnect_start = event_loop.time()
        while event_loop.time() - reconnect_start < RECONNECT_TIMEOUT:
            await asyncio.sleep(RECONNECT_DELAY, loop=event_loop)
            try:
                await prpc.platform.ws_aiohttp.connect(
                    second_connection, str(agent.endpoint_agent),
                    handshake_data=handshake
                )
                break
            except prpc.RpcConnectionClosedError:
                continue
        assert second_connection.connected
        await second_connection.close()


@pytest.mark.async_test
async def test_run_script(event_loop, agent_process):
    """Test batch job execution.
    """
    SCRIPT_NAME = 'run.py'
    SCRIPT_OUTPUT = 'it seems to work'
    SCRIPT_BODY = '''print('%s', end='')''' % (SCRIPT_OUTPUT,)
    READ_CHUNK_SIZE = 3
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
            job_info['uid'], [sys.executable, SCRIPT_NAME], None, 0
        )
        # Wait until it is completed.
        await agent.connection.call_simple(
            agent_service.AgentService.job_wait.__name__,
            job_info['uid']
        )
        # Download the stdout.
        async with agent.connection.call_istream(
            agent_service.AgentService.file_download.__name__,
            [job_info['uid'], jobs.Job.FILENAME_STDOUT],
            {"chunk_size": READ_CHUNK_SIZE}
        ) as call:
            header = await call.stream.receive()
            data = await read_from_stream(call.stream, READ_CHUNK_SIZE)
            bytes_sent = await call.result
            assert bytes_sent == len(data)
            captured_stdout = data.decode(sys.stdout.encoding)
            assert captured_stdout == SCRIPT_OUTPUT
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
        b"",
        ""
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
        # Endpoint doesn't exists.
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
