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
import logging
import socket
import sys
import time


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


def main():
    setup_logging()
    log = logging.getLogger('mock_server')

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--exit-delay', type=float, default=60,
        help='delay before finishing the process'
    )
    parser.add_argument(
        '--socket-count', type=int, default=1,
        help='number of sockets to bind (>=0)'
    )
    log.info('Parsing command line...')
    args = parser.parse_args()
    log.info('Arguments: %s', args)
    assert args.exit_delay >= 0
    assert args.socket_count >= 0

    socks = []
    for idx in range(args.socket_count):
        log.info('Binding socket #%d', idx + 1)
        sock = socket.socket()
        sock.bind(('127.0.0.1', 0))
        log.info('Socket bound: %s:%d', *sock.getsockname())
        sock.listen()
        socks.append(sock)
    log.info('Sockets done, sleeping for %f seconds', args.exit_delay)
    time.sleep(args.exit_delay)
    for sock in socks:
        sock.close()


if __name__ == '__main__':
    main()
