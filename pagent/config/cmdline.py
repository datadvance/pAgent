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


import argparse

from . import schemas
from . import utils


def parse(args=None):
    """Parse command line arguments and return resulting namespace."""
    parser = argparse.ArgumentParser(
        description='pAgent - remote process manager and reverse proxy.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=utils.config_description(schemas.CONFIG)
    )
    parser.add_argument('--config', type=str, help='config file')
    parser.add_argument(
        '--log-level', type=lambda val: str(val).lower(),
        choices={'debug', 'info', 'warning', 'error', 'fatal'},
        default='debug',
        help='output log level'
    )
    parser.add_argument(
        '--log-format',
        default=(
            '[%(process)d] [%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
        ),
        help='log format string'
    )
    parser.add_argument(
        '--connection-debug', action='store_true',
        default=False,
        help='enable additional debug output for RPC connections'
    )
    parser.add_argument(
        '--set', action='append',
        help=(
            'set config parameter, format is \'config_key=value\', '
            'values are interpreted as python literals, '
            'may appear multiple times'
        )
    )
    parser.add_argument(
        '--property', action='append',
        help=(
            'set property as environment variable, format is \'name=value\', '
            'values are interpreted as strings, may appear multiple times'
        )
    )
    return parser.parse_args(args)
