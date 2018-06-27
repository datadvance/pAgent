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


import logging
import platform
import uuid


KEY_AUTH = 'auth'
KEY_UID = 'uid'
KEY_NAME = 'name'
KEY_TOKEN = 'token'
KEY_PLATFORM = 'platform'
KEY_PROPERTIES = 'properties'


class AuthError(Exception):
    """Raised when peer credentials are rejected."""


class Identity(object):
    """Simple manager for app's auth/handshake data."""
    DEFAULT_LOGGER_NAME = 'pagent.Identity'

    @classmethod
    def get_token(cls, handshake):
        """Extract token from the handshake.

        Returns none if token is not present (but handshake structure is
        still checked).
        """
        # Token is absent in outgoing connection remote handshakes,
        # by design. None will do.
        return handshake[KEY_AUTH].get(KEY_TOKEN)

    @classmethod
    def get_uid(cls, handshake):
        """Extract uid from the handshake."""
        # Uid should always present, no additional checks.
        return handshake[KEY_AUTH][KEY_UID]

    def __init__(self, uid, name, server_tokens,
                 client_token, properties, logger=None):
        self._log = logger or logging.getLogger(self.DEFAULT_LOGGER_NAME)
        if uid is None:
            self._log.info('Agent uid is not set, generating a new one')
            uid = uuid.uuid4().hex
        self._uid = uid
        self._name = name
        self._server_tokens = set(server_tokens)
        self._client_token = client_token
        self._properties = properties or {}

        for server_token in self._server_tokens:
            self._check_token(server_token)
        self._check_token(self._client_token)

    @property
    def uid(self):
        """Get the application instance uid."""
        return self._uid

    @property
    def name(self):
        """Get the application instance name."""
        return self._name

    def get_client_handshake(self):
        """Compose handshake to use in active (client) connections."""
        handshake = self.get_server_handshake()
        handshake[KEY_AUTH][KEY_TOKEN] = self._client_token
        return handshake

    def get_server_handshake(self):
        """Compose handshake to use in passive (server) connections."""
        return {
            KEY_AUTH: {
                KEY_UID: self._uid,
                KEY_NAME: self._name,
            },
            KEY_PLATFORM: dict(platform.uname()._asdict()),
            KEY_PROPERTIES: self._properties
        }

    def validate_incoming_handshake(self, handshake):
        """Validate incoming handshake on a passive connection."""
        if not isinstance(handshake, dict):
            raise TypeError('handshake data expected to be a dict')
        auth_data = handshake.get(KEY_AUTH)
        if not isinstance(auth_data, dict):
            raise TypeError('auth data data expected to be a dict')
        if not auth_data.get(KEY_UID):
            raise AuthError('peer uid is invalid')
        if not isinstance(auth_data.get(KEY_NAME), str):
            raise TypeError('peer name is invalid')
        peer_token = auth_data.get(KEY_TOKEN)
        self._check_token(peer_token)
        if not peer_token in self._server_tokens:
            raise AuthError('peer token is not authorized')

    def _check_token(self, token):
        """Aux - check token 'format'."""
        if not isinstance(token, (str, bytes)):
            raise TypeError('token is expected to be of str or bytes type')
        if not token:
            raise ValueError('token cannot be empty')
