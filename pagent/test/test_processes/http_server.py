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


import argparse
import asyncio
import html
import http
import io
import logging
import os
import pathlib
import socket
import sys
import textwrap
import uuid

import aiohttp
import aiohttp.web
import psutil
import yarl


def setup_logging():
    root_logger = logging.getLogger()
    # root logger accepts all messages, filter on handlers level
    root_logger.setLevel(logging.DEBUG)
    for handler in [logging.StreamHandler(sys.stdout)]:
        handler.setFormatter(
            logging.Formatter(
                '[%(process)d] [%(asctime)s] '
                '[%(name)s] [%(levelname)s] %(message)s'
            )
        )
        root_logger.addHandler(handler)


async def root_handler(request):
    response = aiohttp.web.Response(text='hello world!')
    response.headers['TestHeader'] = 'hello world!'
    return response


async def query_handler(request):
    buffer = io.StringIO()
    for key, value in request.query.items():
        buffer.write(key)
        buffer.write('=')
        buffer.write(value)
        buffer.write('\n')
    return aiohttp.web.Response(text=buffer.getvalue())


async def proc_handler(request):
    response = aiohttp.web.StreamResponse()
    response.headers.add('Content-Type', 'text/html')
    await response.prepare(request)

    await response.write(
        textwrap.dedent(
            '''
            <!doctype html>
            <html lang="en">
            <head>
                <meta charset="utf-8">
                <title>Test server - running processes</title>
            </head>
            <body>
            <table border="1" style="width:100%">
            <tr>
                <th>PID</th>
                <th>User</th>
                <th>RSS</th>
                <th style="max-width:400px;word-wrap:break-word;">Name</th>
            </tr>
            '''
        ).encode()
    )

    for process in psutil.process_iter():
        cmdline = '<unavailable>'
        mem_rss = '<unavailable>'
        username = '<unavailable>'
        try:
            cmdline = ' '.join(process.cmdline()) or process.name()
        except Exception:
            pass
        try:
            mem_rss = str(process.memory_info().rss // 1024) + ' Kb'
        except Exception:
            pass
        try:
            username = process.username()
        except Exception:
            pass
        cmdline = html.escape(cmdline)
        mem_rss = html.escape(mem_rss)
        username = html.escape(username)
        table_row = textwrap.dedent(
            '''
            <tr>
                <td>%d</td>
                <td>%s</td>
                <td>%s</td>
                <td style="max-width:400px;word-wrap:break-word;">%s</td>
            </tr>
            '''
        ) % (process.pid, username , mem_rss, cmdline)
        await response.write(table_row.encode())
        await response.drain()

    await response.write(b'</table>')
    await response.write(b'</body>')
    await response.write(b'</html>')
    await response.write_eof()
    return response


async def upload_handler(request):
    start = request.app.loop.time()
    content_size = 0
    while True:
        data = await request.content.readany()
        if not data:
            break
        content_size += len(data)
    elapsed = request.app.loop.time() - start
    request.app.logger.info(
        'received %db in %.3fs (%.3fKb/s)' %
        (content_size, elapsed, content_size / elapsed / 1024.)
    )
    return aiohttp.web.Response(text='%d' % (content_size,))


async def shutdown_handler(request):
    loop = request.app.loop
    loop.call_soon(loop.stop)
    return aiohttp.web.Response()


async def ws_echo_handler(request):
    print('WS echo handler. Headers: ', dict(request.headers))
    response = aiohttp.web.WebSocketResponse()
    await response.prepare(request)
    async for msg in response:
        if msg.type == aiohttp.WSMsgType.BINARY:
            await response.send_bytes(msg.data)
        elif msg.type == aiohttp.WSMsgType.TEXT:
            await response.send_str(msg.data)
        else:
            await response.close()
    return response


async def ws_stream_handler(request):
    MSG_COUNT = 100
    response = aiohttp.web.WebSocketResponse()
    await response.prepare(request)
    for _ in range(MSG_COUNT):
        await response.send_bytes(uuid.uuid4().bytes)
    return response


def main():
    setup_logging()
    log = logging.getLogger('http_server')

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--port', type=int, default=0,
        help='port to bind to'
    )
    parser.add_argument(
        '--exit-delay', type=float, default=60,
        help='delay before finishing the process, never exit if zero'
    )
    log.info('Parsing command line...')
    args = parser.parse_args()
    log.info('Arguments: %s', args)

    loop = asyncio.get_event_loop()
    async def stop_loop_after_delay(app):
        loop.call_later(args.exit_delay, loop.stop)

    app = aiohttp.web.Application()
    # Note: zero (or near-zero) exit delay is problematic
    # due to implementation details of aiohttp.web.run_app.
    if args.exit_delay > 0:
        app.on_startup.append(stop_loop_after_delay)
    app.router.add_get('/', root_handler)
    app.router.add_get('/query_echo', query_handler)
    app.router.add_get('/proc', proc_handler)
    app.router.add_get('/shutdown', shutdown_handler)
    app.router.add_post('/upload', upload_handler)
    app.router.add_get('/ws_echo', ws_echo_handler)
    app.router.add_get('/ws_stream', ws_stream_handler)
    app.router.add_static('/static', pathlib.Path.cwd())


    sock = socket.socket()
    if os.name == 'posix':
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', args.port))
    sock.listen(128)
    _, port = sock.getsockname()

    async def notify_agent():
        url = yarl.URL(os.environ['DA__PAGENT__JOB_ENDPOINT']).with_query(
            {
                'job_id': os.environ['DA__PAGENT__JOB_ID'],
                'port': str(port)
            }
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                assert response.status == http.HTTPStatus.OK
    if os.environ.get('DA__PAGENT__JOB_ENDPOINT'):
        loop.run_until_complete(notify_agent())

    aiohttp.web.run_app(app, sock=sock)


if __name__ == '__main__':
    main()
