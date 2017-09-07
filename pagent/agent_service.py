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
import enum
import http
import os
import pathlib

import aiohttp
import multidict
import prpc
import yarl

from . import identity
from . import jobs


@enum.unique
class WSMessageDirection(enum.Enum):
    """Aux enumeration representing websocket message source/destination.

    Used in WS proxy event demultiplexing.
    """
    JOB_TO_CLIENT = enum.auto()
    CLIENT_TO_JOB = enum.auto()


class AgentService(object):
    """Implementation of RPC methods provided by agent.
    """
    LOCALHOST_BASE_URL = yarl.URL('http://127.0.0.1')
    WS_PROXY_EVENT_QUEUE_DEPTH = 16
    MAX_FILE_CHUNK_SIZE = 1024 * 1024

    def __init__(self, job_manager):
        self._job_manager = job_manager

    @prpc.method
    async def job_create(self, ctx, name=None):
        """Create a new job.

        Args:
            ctx: prpc call context
        """
        sender = self._get_sender(ctx.connection)
        job_uid = self._job_manager.job_create(sender, name)
        return self._job_info(job_uid)

    @prpc.method
    async def job_remove(self, ctx, job_uid):
        """Remove existing job.

        Kills the job process if it is running.

        Args:
            ctx: prpc call context
            job_uid: Job id to remove.
        """
        info = self._job_info(job_uid)
        await self._job_manager.job_remove(job_uid)
        return info

    @prpc.method
    async def job_wait(self, ctx, job_uid):
        """Wait for job to complete.

        Args:
            ctx: prpc call context.
            job_uid: Job id.
        """
        await self._job_manager.job_wait(job_uid)
        return self._job_info(job_uid)

    @prpc.method
    async def job_start(self, ctx, job_uid, args, env, port_expected_count=1):
        """Start a command inside a job.

        Args:
            ctx: prpc call context.
            job_uid: Job id.
            args: Command line arguments (list of strings).
            env: Environment variables to add.
            port_expected_count: Number of listen ports to expect.

        Note:
            Proxy features only work for jobs with port_expected_count==1.
        """
        await self._job_manager.job_start(
            job_uid, args, env, port_expected_count
        )
        return self._job_info(job_uid)

    @prpc.method
    async def job_info(self, ctx, job_uid):
        """Get job info.

        Args:
            ctx: prpc call context.
            job_uid: Job id.
        """
        return self._job_info(job_uid)

    @prpc.method
    async def job_count_current_connection(self, ctx):
        """Get number of active jobs started from current connection.

        Args:
            ctx: prpc call context.
        """
        return self._job_manager.job_count_by_connection(ctx.connection.id)

    def _job_info(self, job_uid):
        """Aux - return job info as JSON.
        """
        info = self._job_manager.job_info(job_uid)
        return {
            'uid': info.uid,
            'name': info.name,
            'state': info.state.name,
            # Don't send token in the job info.
            'sender': {
                'direction': info.sender.direction.name,
                'uid': info.sender.uid,
                'name': info.sender.name
            }
        }

    @prpc.method
    async def http_request(self, ctx, job_uid, method, path, query, headers):
        assert ctx.call_type == prpc.CallType.BISTREAM
        port = self._job_manager.job_port(job_uid)
        if port is None:
            raise ValueError('job does not contain a running server')
        url = self.LOCALHOST_BASE_URL.with_port(
            port
        ).with_path(
            path
        ).with_query(
            multidict.MultiDict(query)
        )

        input_stream = aiohttp.StreamReader(loop=ctx.loop)

        task = ctx.loop.create_task(
            self._http_request(
                url, method, headers, input_stream, ctx.stream
            )
        )
        async for msg in ctx.stream:
            if msg:
                input_stream.feed_data(msg)
            else:
                break
        input_stream.feed_eof()
        await task

    async def _http_request(self, url, method, headers,
                            input_stream, output_stream):
        # Use a session-per-request for now.
        # If needed, we can substantially improve performance on
        # a multiple small requests adding a job-bound session
        # on JobManager level. However, it will introduce
        # additional error conditions (like closing the session
        # due to job disposal while the request is in progress),
        # so let's delay this.
        async with aiohttp.ClientSession() as session:
            try:
                # Proxy needs to be able to decode content by itself,
                # and aiohttp client does not support fancy encodings
                # supported by e.g. Chrome like 'sdch'.
                headers = multidict.CIMultiDict(headers)
                # Will be automatically added back by aiohttp.
                headers.popall('Accept-Encoding', None)
                response = await session.request(
                    method, url, headers=headers, data=input_stream
                )
            except Exception as ex:
                # Exceptions mostly signal issues outside of the HTTP realm,
                # e.g. wrong port (and consequently failed socket.connect).
                #
                # Disregrarding errors in agent code, this can legitemately
                # occur if server process will unexpectedly exit.
                await output_stream.send(http.HTTPStatus.BAD_GATEWAY)
                await output_stream.send([])
                return
            try:
                # Make an editable copy of headers.
                headers = response.headers.copy()
                # Aiohttp decodes content automatically, so encoding
                # header should be dropped.
                headers.popall('Content-Encoding', None)
                await output_stream.send(response.status)
                await output_stream.send(list(headers.items()))
                while True:
                    chunk = await response.content.readany()
                    if not chunk:
                        break
                    await output_stream.send(chunk)
            finally:
                response.close()


    @prpc.method
    async def ws_connect(self, ctx, job_uid, path, query, headers):
        assert ctx.call_type == prpc.CallType.BISTREAM
        port = self._job_manager.job_port(job_uid)
        if port is None:
            raise ValueError('job does not contain a running server')
        url = self.LOCALHOST_BASE_URL.with_port(
            port
        ).with_path(
            path
        ).with_query(
            multidict.MultiDict(query)
        )

        # Use a session-per-request for now.
        # If needed, we can substantially improve performance on
        # a multiple small requests adding a job-bound session
        # on JobManager level. However, it will introduce
        # additional error conditions (like closing the session
        # due to job disposal while the request is in progress),
        # so let's delay this.
        async with aiohttp.ClientSession() as session:
            # TODO: Websocket response (in theory) may contain
            # some custom headers. However, aiohttp does not
            # seem to support this.
            #
            # https://github.com/aio-libs/aiohttp/issues/2053
            try:
                # Even if client supports WS extensions, aiohttp does not.
                headers = multidict.CIMultiDict(headers)
                headers.popall('Accept-Encoding', None)
                headers.popall('Sec-WebSocket-Extensions', None)
                # Will be ignored anyway, so let's keep it clean.
                # Independent WS connection uses independent key.
                # More to that, it would be in spirit of aiohttp
                # to start raising exceptions on seeing it.
                headers.popall('Sec-WebSocket-Key', None)
                websocket = await session.ws_connect(
                    url, headers=headers
                )
                await ctx.stream.send(True)
            except Exception:
                await ctx.stream.send(False)
                raise
            # Use event queue to merge to event streams
            # (messages from/to client).
            #
            # Queue is significantly faster than
            #   asyncio.wait([event_source_1, event_source_2])
            # as it does not create new Tasks and async context switches.
            #
            # Websockets connections are likely to carry a lot of small
            # messages, so this optmization is pretty important (~3x speedup).
            event_queue = asyncio.Queue(self.WS_PROXY_EVENT_QUEUE_DEPTH)
            ws_listen_task = ctx.loop.create_task(
                self._ws_listen(websocket, event_queue)
            )
            stream_listen_task = ctx.loop.create_task(
                self._stream_listen(ctx.stream, event_queue)
            )
            try:
                while True:
                    direction, data = await event_queue.get()
                    if data is None:
                        break
                    elif direction == WSMessageDirection.JOB_TO_CLIENT:
                        await ctx.stream.send(data)
                    elif direction == WSMessageDirection.CLIENT_TO_JOB:
                        if isinstance(data, str):
                            await websocket.send_str(data)
                        elif isinstance(data, bytes):
                            await websocket.send_bytes(data)
                        else:
                            break
            except Exception:
                # Any errors just close both sockets.
                pass
            finally:
                await websocket.close()
                await ctx.stream.close()
            await asyncio.wait(
                [ws_listen_task, stream_listen_task], loop=ctx.loop
            )

    async def _ws_listen(self, websocket, event_queue):
        """Aux - put all messages from the job websocket to the event queue.
        """
        async for msg in websocket:
            if msg.type not in (aiohttp.WSMsgType.BINARY,
                                aiohttp.WSMsgType.TEXT):
                break
            await event_queue.put((WSMessageDirection.JOB_TO_CLIENT, msg.data))
        await event_queue.put((WSMessageDirection.JOB_TO_CLIENT, None))

    async def _stream_listen(self, stream, event_queue):
        """Aux - put all messages from the rpc stream to the event queue.
        """
        async for msg in stream:
            await event_queue.put((WSMessageDirection.CLIENT_TO_JOB, msg))
        await event_queue.put((WSMessageDirection.CLIENT_TO_JOB, None))

    @prpc.method
    async def file_upload(self, ctx, job_uid, filename):
        """Receive a file from a client.

        File content is transferred using prpc streaming.

        Args:
            ctx: prpc call context.
            job_uid: Job uid identifying the sandbox to put the file in.
            filename: File name relative to the job sandbox.
        """
        assert ctx.call_type == prpc.CallType.OSTREAM
        if pathlib.PurePath(filename).is_absolute():
            raise ValueError('file path must be relative')
        job_workdir = self._job_manager.job_workdir(job_uid)
        target_path = job_workdir.joinpath(filename)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, 'wb') as target_file:
            async for chunk in ctx.stream:
                target_file.write(chunk)
            return target_file.tell()

    @prpc.method
    async def file_download(self, ctx, job_uid,
                            filename, chunk_size=64*1024):
        """Send a requested file to a client.

        File content is transferred using prpc streaming.

        Args:
            ctx: prpc call context.
            job_uid: Job uid identifying the sandbox with the file.
            filename: File name relative to the job sandbox.
            chunk_size: Max chunk size for a single message.
        """
        assert ctx.call_type == prpc.CallType.ISTREAM
        assert chunk_size > 0
        if pathlib.PurePath(filename).is_absolute():
            raise ValueError('file path must be relative')
        chunk_size = min(chunk_size, self.MAX_FILE_CHUNK_SIZE)
        job_workdir = self._job_manager.job_workdir(job_uid)
        target_path = job_workdir.joinpath(filename)
        with open(target_path, 'rb') as target_file:
            target_file.seek(0, os.SEEK_END)
            file_size = target_file.tell()
            target_file.seek(0, os.SEEK_SET)
            await ctx.stream.send({
                'size': file_size
            })
            chunk = target_file.read(chunk_size)
            while chunk:
                await ctx.stream.send(chunk)
                chunk = target_file.read(chunk_size)
            return target_file.tell()

    def _get_sender(self, connection):
        auth_data = connection.handshake_data[identity.KEY_AUTH]
        auth_token = ''
        if connection.mode == prpc.ConnectionMode.SERVER:
            auth_token = auth_data[identity.KEY_TOKEN]
        return jobs.Sender(
            connection.mode,
            connection.id,
            auth_data[identity.KEY_UID],
            auth_data[identity.KEY_NAME],
            auth_token
        )
