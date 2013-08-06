#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2012 Nigel Small
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

"""Incremental JSON parser.
"""

import sys
__PY2 = sys.version_info[0] == 2

import string
if __PY2:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
else:
    from io import StringIO

__author__ = 'Nigel Small'
__package__ = "jsonstream"

_ESCAPES = {
    '"': u'"',
    '\\': u'\\',
    '/': u'/',
    'b': u'\b',
    'f': u'\f',
    'n': u'\n',
    'r': u'\r',
    't': u'\t',
}

if __PY2:
    _chr = lambda x: unichr(x)
else:
    _chr = lambda x: chr(x)


class Pending(BaseException):

    def __init__(self, *args, **kwargs):
        super(Pending, self).__init__(*args, **kwargs)


class EndOfStream(BaseException):

    def __init__(self, *args, **kwargs):
        super(EndOfStream, self).__init__(*args, **kwargs)


class UnexpectedCharacter(ValueError):

    def __init__(self, *args, **kwargs):
        super(UnexpectedCharacter, self).__init__(*args, **kwargs)


class Tokeniser(object):

    def __init__(self):
        self.data = StringIO()
        self._write_pos = 0
        self._writable = True

    def _assert_writable(self):
        if not self._writable:
            raise IOError("Stream is not writable")

    def write(self, data):
        """Write raw JSON data to the decoder stream.
        """
        self._assert_writable()
        read_pos = self.data.tell()
        self.data.seek(self._write_pos)
        self.data.write(data)
        self._write_pos = self.data.tell()
        self.data.seek(read_pos)

    def end(self):
        self._writable = False

    def _peek(self):
        """Return next available character without
        advancing pointer.
        """
        pos = self.data.tell()
        ch = self.data.read(1)
        self.data.seek(pos)
        if ch:
            return ch
        elif self._writable:
            raise Pending()
        else:
            raise EndOfStream()

    def _read(self):
        """Read the next character.
        """
        ch = self.data.read(1)
        if ch:
            return ch
        elif self._writable:
            raise Pending()
        else:
            raise EndOfStream()

    def _skip_whitespace(self):
        while True:
            pos = self.data.tell()
            ch = self.data.read(1)
            if ch == '':
                break
            if ch not in string.whitespace:
                self.data.seek(pos)
                break

    def _read_literal(self, literal):
        pos = self.data.tell()
        try:
            for expected in literal:
                actual = self._read()
                if actual != expected:
                    raise UnexpectedCharacter(actual)
        except Pending:
            self.data.seek(pos)
            raise Pending()
        return literal

    def _read_digit(self):
        pos = self.data.tell()
        try:
            digit = self._read()
            if digit not in "0123456789":
                self.data.seek(pos)
                raise UnexpectedCharacter(digit)
        except Pending:
            self.data.seek(pos)
            raise Pending()
        return digit

    def _read_string(self):
        global _ESCAPES
        pos = self.data.tell()
        src, value = [self._read_literal('"')], []
        try:
            while True:
                ch = self._read()
                src.append(ch)
                if ch == '\\':
                    ch = self._read()
                    src.append(ch)
                    if ch in _ESCAPES:
                        value.append(_ESCAPES[ch])
                    elif ch == 'u':
                        n = 0
                        for i in range(4):
                            ch = self._read()
                            src.append(ch)
                            n = 16 * n + int(ch, 16)
                        value.append(_chr(n))
                    else:
                        raise UnexpectedCharacter(ch)
                elif ch == '"':
                    break
                else:
                    value.append(ch)
        except Pending:
            self.data.seek(pos)
            raise Pending()
        return "".join(src), u"".join(value)

    def _read_number(self):
        pos = self.data.tell()
        src = []
        has_fractional_part = False
        try:
            # check for sign
            ch = self._peek()
            if ch == '-':
                src.append(self._read())
            # read integer part
            ch = self._read_digit()
            src.append(ch)
            if ch != '0':
                while True:
                    try:
                        src.append(self._read_digit())
                    except (UnexpectedCharacter, EndOfStream):
                        break
            try:
                ch = self._peek()
            except EndOfStream:
                pass
            # read fractional part
            if ch == '.':
                has_fractional_part = True
                src.append(self._read())
                while True:
                    try:
                        src.append(self._read_digit())
                    except (UnexpectedCharacter, EndOfStream):
                        break
        except Pending:
            # number potentially incomplete: need to wait for
            # further data or end of stream
            self.data.seek(pos)
            raise Pending()
        src = "".join(src)
        if has_fractional_part:
            return src, float(src)
        else:
            return src, int(src)

    def read(self):
        global _CONSTANTS
        try:
            self._skip_whitespace()
            ch = self._peek()
            if ch in ',:[]{}':
                return self._read(), None
            elif ch == 'n':
                return self._read_literal('null'), None
            elif ch == 't':
                return self._read_literal('true'), True
            elif ch == 'f':
                return self._read_literal('false'), False
            elif ch == '"':
                return self._read_string()
            elif ch in '-0123456789':
                return self._read_number()
            else:
                raise UnexpectedCharacter(ch)
        except EndOfStream:
            self._writable = True
            raise EndOfStream


VALUE         = 1
ARRAY_OPENER  = 2
ARRAY_CLOSER  = 4
OBJECT_OPENER = 8
OBJECT_CLOSER = 16
COMMA         = 32
COLON         = 64

class Decoder(object):

    def __init__(self):
        self.tokeniser = Tokeniser()
        self.path = []
        self.expecting = VALUE | ARRAY_OPENER | OBJECT_OPENER

    def write(self, data):
        self.tokeniser.write(data)

    def end(self):
        self.tokeniser.end()

    def _assert_expecting(self, token, src):
        if not self.expecting & token:
            raise UnexpectedCharacter(src)

    def _in_array(self):
        return self.path and isinstance(self.path[-1], int)

    def _in_object(self):
        return self.path and not isinstance(self.path[-1], int)

    def _has_key(self):
        if self.path:
            top = self.path[-1]
            if top is None:
                return False
            elif isinstance(self.path[-1], int):
                return None
            else:
                return True
        else:
            return None

    def _callback(self, handler, value):
        if handler:
            handler(self.path, value)

    def _handle_value(self, src, value, handler):
        self._assert_expecting(VALUE, src)
        if self._in_array():
            # array value
            self._callback(handler, value)
            self.path[-1] += 1
            self.expecting = COMMA | ARRAY_CLOSER
        elif self._in_object():
            if self._has_key():
                # object value
                self._callback(handler, value)
                self.path[-1] = None
                self.expecting = COMMA | OBJECT_CLOSER
            else:
                # object key
                self.path[-1] = value
                self.expecting = COLON
        else:
            # simple value
            self._callback(handler, value)

    def _handle_comma(self, src):
        self._assert_expecting(COMMA, src)
        self.expecting = VALUE | ARRAY_OPENER | OBJECT_OPENER

    def _handle_colon(self, src):
        self._assert_expecting(COLON, src)
        self.expecting = VALUE | ARRAY_OPENER | OBJECT_OPENER

    def _open_array(self, src):
        self._assert_expecting(ARRAY_OPENER, src)
        self.path.append(0)
        self.expecting = VALUE | ARRAY_OPENER | ARRAY_CLOSER | OBJECT_OPENER

    def _close_array(self, src):
        self._assert_expecting(ARRAY_CLOSER, src)
        self.path.pop()
        if self._in_array():
            self.path[-1] += 1
            self.expecting = COMMA | ARRAY_CLOSER
        elif self._in_object():
            self.path[-1] = None
            self.expecting = COMMA | OBJECT_CLOSER
        else:
            self.expecting = VALUE | ARRAY_OPENER | OBJECT_OPENER

    def _open_object(self, src):
        self._assert_expecting(OBJECT_OPENER, src)
        self.path.append(None)
        self.expecting = VALUE | OBJECT_CLOSER

    def _close_object(self, src):
        self._assert_expecting(OBJECT_CLOSER, src)
        self.path.pop()
        if self._in_array():
            self.path[-1] += 1
            self.expecting = COMMA | ARRAY_CLOSER
        elif self._in_object():
            self.path[-1] = None
            self.expecting = COMMA | OBJECT_CLOSER
        else:
            self.expecting = VALUE | ARRAY_OPENER | OBJECT_OPENER

    def read(self, handler):
        try:
            while True:
                src, value = self.tokeniser.read()
                if src == ',':
                    self._handle_comma(src)
                elif src == ':':
                    self._handle_colon(src)
                elif src == '[':
                    self._open_array(src)
                elif src == ']':
                    self._close_array(src)
                elif src == '{':
                    self._open_object(src)
                elif src == '}':
                    self._close_object(src)
                else:
                    self._handle_value(src, value, handler)
        except Pending:
            return True
        except EndOfStream:
            return False
