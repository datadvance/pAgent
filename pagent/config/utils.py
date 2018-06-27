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

import io


def config_description(schema, header='Config parameters:\n\n', indent='  '):
    """Generate config parameters description from JSON-schema."""
    assert isinstance(schema, dict)
    assert schema['type'] == 'object'
    buffer = io.StringIO()
    buffer.write(header)
    _config_description(schema, '', indent, buffer)
    return buffer.getvalue()


def _config_description(current_schema, current_key,
                        indent, buffer):
    if current_schema.get('type') == 'object':
        properties = current_schema.get('properties')
        if properties:
            for key in sorted(properties.keys()):
                next_key = '%s.%s' % (current_key, key) if current_key else key
                _config_description(
                    properties[key],
                    next_key,
                    indent,
                    buffer
                )
            buffer.write('\n')
        else:
            _emit_description(current_schema, current_key, indent, buffer)
            buffer.write('\n')
    elif current_schema.get('anyOf'):
        for subschema in current_schema['anyOf']:
            if subschema['type'] == 'null':
                continue
            _config_description(
                subschema,
                current_key,
                indent,
                buffer
            )
            break
    else:
        _emit_description(current_schema, current_key, indent, buffer)


def _emit_description(current_schema, current_key, indent, buffer):
    description = current_schema.get('description')
    buffer.write(indent)
    buffer.write(current_key)
    buffer.write(' - ')
    if description:
        buffer.write(description)
    else:
        buffer.write('<undocumented>')
    buffer.write('\n')
