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


import platform

import pytest

from pagent import identity


def test_handshake_content():
    """Check that handshake functions produce sensible output.
    """
    UID = 'uid'
    NAME = 'name'
    ACCEPT_TOKENS = []
    TOKEN = 'token'
    PROPERTIES = {
        'some_prop': 'some_value'
    }
    instance = identity.Identity(
        UID, NAME, ACCEPT_TOKENS, TOKEN, PROPERTIES
    )
    client_handshake = instance.get_client_handshake()
    assert client_handshake[identity.KEY_AUTH][identity.KEY_UID] == UID
    assert client_handshake[identity.KEY_AUTH][identity.KEY_NAME] == NAME
    assert client_handshake[identity.KEY_AUTH][identity.KEY_TOKEN] == TOKEN
    assert client_handshake[identity.KEY_PLATFORM] == platform.uname()._asdict()
    assert client_handshake[identity.KEY_PROPERTIES] == PROPERTIES
    # Server handshake differs from client one only by 'auth/token' key.
    server_handshake = instance.get_server_handshake()
    del client_handshake[identity.KEY_AUTH][identity.KEY_TOKEN]
    assert client_handshake == server_handshake


def test_handshake_validation():
    """Check handshake validation errors."""
    ACCEPTED_TOKEN = 'secret'
    instance = identity.Identity(
        'uid', 'name', [ACCEPTED_TOKEN], 'client', {}
    )
    VALID_HANDSHAKE = {
        identity.KEY_AUTH: {
            identity.KEY_UID: 'nonempty',
            identity.KEY_NAME: 'nonempty',
            identity.KEY_TOKEN: ACCEPTED_TOKEN
        }
    }
    instance.validate_incoming_handshake(VALID_HANDSHAKE)
    for wrong_type in [None, [], 'str', b'bytes']:
        with pytest.raises(TypeError):
            instance.validate_incoming_handshake(wrong_type)
    with pytest.raises(TypeError):
        instance.validate_incoming_handshake({
            identity.KEY_AUTH: ''
        })
    for wrong_type in [None, [], 'str', b'bytes']:
        with pytest.raises(TypeError):
            instance.validate_incoming_handshake({
                identity.KEY_AUTH: wrong_type
            })
    with pytest.raises(identity.AuthError):
        instance.validate_incoming_handshake({
            identity.KEY_AUTH: {
                identity.KEY_UID: ''
            }
        })
    with pytest.raises(TypeError):
        instance.validate_incoming_handshake({
            identity.KEY_AUTH: {
                identity.KEY_UID: 'nonempty',
                identity.KEY_NAME: b'oops, bytes',
            }
        })
    with pytest.raises(identity.AuthError):
        instance.validate_incoming_handshake({
            identity.KEY_AUTH: {
                identity.KEY_UID: 'nonempty',
                identity.KEY_NAME: 'nonempty',
                identity.KEY_TOKEN: 'wrong token'
            }
        })
