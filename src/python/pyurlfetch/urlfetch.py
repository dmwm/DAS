# -*- coding: utf-8 -*-
#
# Copyright 2010 Tobias Rodäbel
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""URL Fetch client."""

import logging
import operator
import os
import socket
import struct

DEFAULT_ADDR   = ('localhost', 10190)
MAX_CHUNK_SIZE = 1024
ERROR          = 'ERROR'
NOT_FOUND      = 'NOT_FOUND'
TERMINATOR     = '\tEOF\n'

class DownloadError(Exception):
    """Raised when a download fails."""

def pack(*args):
    """Returns a data packet."""
    data = ''
    for arg in args:
        if  isinstance(arg, int):
            data += struct.pack("!B", arg)
        if  isinstance(arg, basestring):
            data += arg.encode('ascii', 'ignore')
    return data


def command(*args):
    """Creates a command from the given arguments."""
    if len(args) == 1:
        return args[0]
    pairs = [[len(arg), arg] for arg in args[:-1]]
    return pack(*reduce(list.__add__, pairs)+[args[-1]])


def read_int4(s):
    """Convert first four bytes from a string to an unsigned 32-bit integer.

    In Python on a 32-bit machine, integers are signed and within the range
    -2^31 .. (2^31 - 1). If the 32-bit integer fits within this range, an
    integer is returned, otherwise a long is returned.

    This method is taken from Tomas Abrahamsson's Py-Interface package.

    Args:
        s: A string.

    Returns:
        Integer or long.
    """
    l4 = ((long(ord(s[0])) << 24) +
          (ord(s[1]) << 16) +
          (ord(s[2]) <<  8) +
          (ord(s[3]) <<  0))
    try:
        i4 = int(l4)
        return i4
    except OverflowError:   # pragma: no cover
        return l4


def encode_headers(headers):
    """Encodes HTTP headers.

    Args:
        headers: Dictionary of HTTP headers.

    Returns:
        String containing encoded HTTP headers.
    """
    return '\n'.join(["%s: %s"%(k,headers[k]) for k in sorted(headers.keys())])


def decode_headers(headers):
    """Decode HTTP headers.

    Args:
        headers: String containing encoded HTTP headers.

    Returns:
        Dictionary of HTTP headers.
    """
    def strip(l):
        a, b = l
        return (a.strip(), b.strip())
    return dict([strip(h.split(':', 1)) for h in headers.split('\n')])


class URLFetchClient(object):
    """Simple client for the URL Fetch Service."""

    def __init__(self, addr=DEFAULT_ADDR):
        """Constructor.

        Args:
            addr: The URL Fetch Service address.
        """
        self.address = addr
        self._socket = None

    def _open(self):
        "Initialize AF_INET socket and connect to class' address"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.address)
        self._socket = sock

    def close(self):
        """Close client connection."""
        if self._socket: 
            self._socket.close()
        self._socket = None

    def start_fetch(self, url, payload="", method="get", headers={}):
        """Initiate an URL Fetch call.

        Args:
            url: A HTTP or HTTPS URL.
            payload: Body content for a POST or PUT request.
            method: The HTTP method.
            headers: Dictionary with HTTP headers to include with the request.

        Returns:
            String containing the fetch call id.
        """
        if self._socket is None:
            self._open()

        method = method.lower()

        headers = encode_headers(headers)

        cmd_seq = command("FETCH_ASYNC", method, url, payload, headers)

        sent = 0

        try:
            sent = self._socket.send(cmd_seq)
        except socket.error:    # pragma: no cover
            raise DownloadError

        res = self._socket.recv(32)

        if not sent or not res or res == ERROR:
            raise DownloadError 

        logging.debug("Fetching %s - %s", url, res)

        return res

    def get_result(self, fid, nowait=False):
        """Get results.

        Raises DownloadError.

        Args:
            fid: The fetch call id.
            nowait: Boolean which specifies if the client waits for results.

        Returns:
            Results of the URL Fetch call.
        """
        logging.debug("Getting results for %s", fid)

        body = ''

        if self._socket is None:
            self._open()

        if nowait:
            self._socket.send(command("GET_RESULT_NOWAIT", fid))
        else:
            self._socket.send(command("GET_RESULT", fid))

        while True:
            data = self._socket.recv(MAX_CHUNK_SIZE)

            if not body and data:
                status_code = read_int4(data[:4])
                data = data[4:]

            if not data or data in (NOT_FOUND,):
                raise DownloadError
            elif data.endswith(TERMINATOR):
                body += data[:-len(TERMINATOR)]
                break

            body += data

        try:
            headers, body = body.split('\n\n', 1)
        except ValueError:      # pragma: no cover
            raise DownloadError

        return (status_code, body, decode_headers(headers))
