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

import ast
import logging
import uuid

import jsonschema

from . import cmdline
from . import file
from . import schemas


def initialize():
    args = cmdline.parse()
    config = file.load_default()
    if args.config:
        user_config = file.load(args.config)
        config.update(user_config)
    _validate_schema(config, 'invalid config file parameter')
    if args.set:
        for arg in args.set:
            key, value = arg.split('=', 2)
            try:
                value = ast.literal_eval(value)
            except Exception as ex:
                raise ValueError(
                    'invalid value for config key "%s"' % (key,)
                )
            config_path = key.split('.')
            config_node = config
            for key_element in config_path[:-1]:
                config_node = config[key_element]
            config_node[config_path[-1]] = value
        _validate_schema(config, 'invalid command line config parameter')
    return args, config


def validate(config):
    log = logging.getLogger('pagent.config')
    if not config['server']['enabled'] and not config['client']['enabled']:
        log.fatal('Both client and server modes are disabled')
        raise ValueError(
            'incorrect config: both client and server modes disabled'
        )
    if config['server']['enabled'] and not config['server']['accept_tokens']:
        log.warning('Server mode is enabled, but there are no accepted tokens')


def _validate_schema(config, message):
    validator = jsonschema.Draft4Validator(schemas.CONFIG)
    try:
        error = jsonschema.exceptions.best_match(validator.iter_errors(config))
        if error:
            raise error
    except Exception as ex:
        raise ValueError(message) from ex
