"""A pull parser for parsing JSON streams"""

# The MIT License (MIT)
#
# Copyright (c) 2015 by Teradata
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys
import decimal
import re
import json
import logging
from . import util  # @UnusedImport # noqa
if sys.version_info[0] == 2:
    from StringIO import StringIO  # @UnresolvedImport #@UnusedImport
else:
    from io import StringIO  # @UnresolvedImport @UnusedImport @Reimport # noqa

logger = logging.getLogger(__name__)

# JSONNode and value types.
OBJECT = "OBJECT"
ARRAY = "ARRAY"
FIELD = "FIELD"
STRING = "STRING"
NUMBER = "NUMBER"
BOOLEAN = "BOOLEAN"
NULL = "null"
TRUE = "true"
FALSE = "false"

# JSONEvent types.
START_OBJECT = "START_OBJECT"
START_ARRAY = "START_ARRAY"
FIELD_NAME = "FIELD_NAME"
FIELD_VALUE = "FIELD_VALUE"
ARRAY_VALUE = "ARRAY_VALUE"
END_OBJECT = "END_OBJECT"
END_ARRAY = "END_ARRAY"

# JSONParseError codes
JSON_SYNTAX_ERROR = "JSON_SYNTAX_ERROR"
JSON_INCOMPLETE_ERROR = "JSON_INCOMPLETE_ERROR"
JSON_UNEXPECTED_ELEMENT_ERROR = "JSON_UNEXPECTED_ELEMENT_ERROR"


class JSONPullParser (object):

    def __init__(self, stream, size=2 ** 16):
        """Initialize pull parser with a JSON stream."""
        self.stream = stream
        self.size = size
        self.node = None
        self.value = ""
        self.valueType = None
        self.tokens = []
        self.tokenIndex = 0
        self.halfToken = ""
        self.pattern = re.compile('([\[\]{}:\\\\",])')

    def expectObject(self):
        """Raise JSONParseError if next event is not the start of an object."""
        event = self.nextEvent()
        if event.type != START_OBJECT:
            raise JSONParseError(
                JSON_UNEXPECTED_ELEMENT_ERROR,
                "Expected START_OBJECT but got: " + str(event))

    def expectArray(self):
        """Raise JSONParseError if next event is not the start of an array."""
        event = self.nextEvent()
        if event.type != START_ARRAY:
            raise JSONParseError(
                JSON_UNEXPECTED_ELEMENT_ERROR,
                "Expected START_ARRAY but got: " + str(event))
        return JSONArrayIterator(self)

    def expectField(self, expectedName, expectedType=None, allowNull=False,
                    readAll=False):
        """Raise JSONParseError if next event is not the expected field with
           expected type else return the field value. If the next field is
           an OBJECT or ARRAY, only return whole object or array if
           readAll=True."""
        event = self.nextEvent()
        if event.type != FIELD_NAME:
            raise JSONParseError(
                JSON_UNEXPECTED_ELEMENT_ERROR,
                "Expected FIELD_NAME but got: " + str(event))
        if event.value != expectedName:
            raise JSONParseError(JSON_UNEXPECTED_ELEMENT_ERROR, "Expected " +
                                 expectedName + " field but got " +
                                 event.value + " instead.")
        return self._expectValue(FIELD_VALUE, expectedType, allowNull, readAll)

    def expectArrayValue(self, expectedType=None, allowNull=False,
                         readAll=False):
        """Raise JSONParseError if next event is not an array element with
           the expected type else return the field value. If the next value
           is an OBJECT or ARRAY, only return whole object or array if
           readAll=True."""
        return self._expectValue(ARRAY_VALUE, expectedType, allowNull, readAll)

    def _expectValue(self, eventType, expectedType, allowNull, readAll):
        event = self.nextEvent()
        if event.type == eventType:
            if allowNull and event.valueType == NULL:
                return None
            elif expectedType is not None and event.valueType != expectedType:
                raise JSONParseError(
                    JSON_UNEXPECTED_ELEMENT_ERROR, "Expected " + expectedType +
                    " but got " + event.valueType + " instead.")
            else:
                return event.value
        else:
            if eventType == ARRAY_VALUE:
                if event.node.parent is None or event.node.parent != ARRAY:
                    raise JSONParseError(
                        JSON_UNEXPECTED_ELEMENT_ERROR,
                        "Expected array element but not in an array.")
            if event.type == START_OBJECT:
                if expectedType is not None and expectedType != OBJECT:
                    raise JSONParseError(
                        JSON_UNEXPECTED_ELEMENT_ERROR, "Expected " +
                        expectedType + " but got an object instead.")
                elif expectedType is None or readAll:
                    return self.readObject(event)
            elif event.type == START_ARRAY:
                if expectedType is not None and expectedType != ARRAY:
                    raise JSONParseError(
                        JSON_UNEXPECTED_ELEMENT_ERROR, "Expected " +
                        expectedType + " but got array instead.")
                if expectedType is None or readAll:
                    return self.readArray(event)
                else:
                    return JSONArrayIterator(self)
            else:
                raise JSONParseError(
                    JSON_UNEXPECTED_ELEMENT_ERROR,
                    "Unexpected event: " + str(event))

    def readObject(self, event=None):
        """Read and return a JSON object."""
        if event is None:
            event = self.nextEvent()
            popRequired = False
        else:
            popRequired = True
        if event is None:
            return None
        if event.type != START_OBJECT:
            raise JSONParseError(
                JSON_UNEXPECTED_ELEMENT_ERROR,
                "Expected START_OBJECT but got " + event.type + " instead.")
        obj = self._load(event)
        if popRequired:
            self._pop()
        return obj

    def readArray(self, event=None):
        """Read and return a JSON array."""
        if event is None:
            event = self.nextEvent()
            popRequired = False
        else:
            popRequired = True
        if event is None:
            return None
        if event.type != START_ARRAY:
            raise JSONParseError(
                JSON_UNEXPECTED_ELEMENT_ERROR,
                "Expected START_ARRAY but got " + event.type + " instead.")
        arr = self._load(event)
        if popRequired:
            self._pop()
        return arr

    def nextEvent(self):
        """Iterator method, return next JSON event from the stream, raises
        StopIteration() when complete."""
        try:
            return self.__next__()
        except StopIteration:
            return None

    def next(self):
        """Iterator method, return next JSON event from the stream, raises
        StopIteration() when complete."""
        return self.__next__()

    def __next__(self):
        """Iterator method, return next JSON event from the stream, raises
        StopIteration() when complete."""
        while True:
            try:
                token = self.tokens[self.tokenIndex]
                self.tokenIndex += 1
                if token == "" or token.isspace():
                    pass
                elif token == '{':
                    return self._push(OBJECT)
                elif token == '}':
                    if self.node.type == FIELD:
                        self.tokenIndex -= 1
                        event = self._pop()
                        if event is not None:
                            return event
                    elif self.node.type == OBJECT:
                        return self._pop()
                    else:
                        raise JSONParseError(
                            JSON_SYNTAX_ERROR,
                            "A closing curly brace ('}') is only expected "
                            "at the end of an object.")
                elif token == '[':
                    if self.node is not None and self.node.type == OBJECT:
                        raise JSONParseError(
                            JSON_SYNTAX_ERROR, "An array in an object must "
                            "be preceded by a field name.")
                    return self._push(ARRAY)
                elif token == ']':
                    if self.valueType is not None:
                        self.tokenIndex -= 1
                        event = self._arrayValue()
                        if event is not None:
                            return event
                    elif self.node.type == ARRAY:
                        if self.node.lastIndex == self.node.arrayLength:
                            self.node.arrayLength += 1
                        return self._pop()
                    else:
                        raise JSONParseError(
                            JSON_SYNTAX_ERROR, "A closing bracket (']') "
                            "is only expected at the end of an array.")
                elif token == ':':
                    if self.node.type == OBJECT:
                        if self.value != "" and self.valueType == STRING:
                            event = self._push(FIELD, self.value)
                            self.value = ""
                            self.valueType = None
                            return event
                        else:
                            raise JSONParseError(
                                JSON_SYNTAX_ERROR,
                                "Name for name/value pairs cannot be empty.")
                    else:
                        raise JSONParseError(
                            JSON_SYNTAX_ERROR,
                            "A colon (':') can only following a field "
                            "name within an object.")
                elif token == ',':
                    if self.node.type == ARRAY:
                        event = self._arrayValue()
                        self.node.arrayLength += 1
                    elif self.node.type == FIELD:
                        event = self._pop()
                    else:
                        raise JSONParseError(
                            JSON_SYNTAX_ERROR,
                            "A comma (',') is only expected between fields "
                            "in objects or elements of an array.")
                    if event is not None:
                        return event
                else:
                    if self.valueType is not None:
                        raise JSONParseError(
                            JSON_SYNTAX_ERROR, "Extra name or value found "
                            "following: " + str(self.value))
                    elif self.node is None:
                        raise JSONParseError(
                            JSON_SYNTAX_ERROR,
                            "Input must start with either an "
                            "OBJECT ('{') or ARRAY ('['), got '" + token +
                            "' instead.")
                    elif token == '"':
                        escape = False
                        while True:
                            try:
                                token = self.tokens[self.tokenIndex]
                                self.tokenIndex += 1
                                if token == "":
                                    pass
                                elif escape:
                                    escape = False
                                    self.value += token
                                elif token == '"':
                                    break
                                elif token == '\\':
                                    escape = True
                                else:
                                    self.value += token
                            except IndexError:
                                data = self.stream.read(self.size)
                                if data == "":
                                    raise JSONParseError(
                                        JSON_INCOMPLETE_ERROR,
                                        "Reached end of input before " +
                                        "reaching end of string.")
                                self.tokens = self.pattern.split(data)
                                self.tokenIndex = 0
                        self.valueType = STRING
                    else:
                        token = token.strip()
                        if self.tokenIndex == len(self.tokens):
                            self.halfToken = token
                            raise IndexError
                        elif token[0].isdigit() or token[0] == '-':
                            self.value = decimal.Decimal(token)
                            self.valueType = NUMBER
                        elif token == "null":
                            self.value = None
                            self.valueType = NULL
                        elif token == "true":
                            self.value = True
                            self.valueType = BOOLEAN
                        elif token == "false":
                            self.value = False
                            self.valueType = BOOLEAN
                        else:
                            raise JSONParseError(
                                JSON_SYNTAX_ERROR,
                                "Unexpected token: " + token)
            except IndexError:
                data = self.stream.read(self.size)
                if data == "":
                    if self.node is not None:
                        raise JSONParseError(
                            JSON_INCOMPLETE_ERROR, "Reached end of input "
                            "before reaching end of JSON structures.")
                    else:
                        raise StopIteration()
                    return None
                logger.trace(data)
                self.tokens = self.pattern.split(data)
                self.tokenIndex = 0
                if self.halfToken is not None:
                    self.tokens[0] = self.halfToken + self.tokens[0]
                    self.halfToken = None

    def _load(self, event):
        if event.type == START_OBJECT:
            value = start = "{"
            end = "}"
        elif event.type == START_ARRAY:
            value = start = "["
            end = "]"
        else:
            raise JSONParseError(
                JSON_UNEXPECTED_ELEMENT_ERROR,
                "Unexpected event: " + event.type)
        count = 1
        tokens = self.tokens
        tokenIndex = self.tokenIndex
        inString = False
        inEscape = False
        try:
            while True:
                startIndex = tokenIndex
                for token in tokens[startIndex:]:
                    tokenIndex += 1
                    if token == "":
                        pass
                    elif inString:
                        if inEscape:
                            inEscape = False
                        elif token == '"':
                            inString = False
                        elif token == '\\':
                            inEscape = True
                    elif token == '"':
                        inString = True
                    elif token == start:
                        count += 1
                    elif token == end:
                        count -= 1
                        if count == 0:
                            value += "".join(tokens[startIndex:tokenIndex])
                            raise StopIteration()
                value += "".join(tokens[startIndex:])
                data = self.stream.read(self.size)
                if data == "":
                    raise JSONParseError(
                        JSON_INCOMPLETE_ERROR, "Reached end of input before "
                        "reaching end of JSON structures.")
                tokens = self.pattern.split(data)
                tokenIndex = 0
        except StopIteration:
            pass
        self.tokens = tokens
        self.tokenIndex = tokenIndex
        try:
            return json.loads(value, parse_float=decimal.Decimal,
                              parse_int=decimal.Decimal)
        except ValueError as e:
            raise JSONParseError(JSON_SYNTAX_ERROR, "".join(e.args))

    def _push(self, nodeType, value=None):
        if self.node is not None and self.node.type == FIELD:
            self.node.valueType = nodeType
        self.node = JSONNode(self.node, nodeType, value)
        if self.node.parent is not None and self.node.parent.type == ARRAY:
            self.node.arrayIndex = self.node.parent.arrayLength
            if self.node.parent.lastIndex == self.node.parent.arrayLength:
                raise JSONParseError(
                    JSON_SYNTAX_ERROR,
                    "Missing comma separating array elements.")
            self.node.parent.lastIndex = self.node.parent.arrayLength
        return self.node.startEvent()

    def _pop(self):
        # Pop the current node from the stack.
        node = self.node
        self.node = self.node.parent
        # Set the value and value type on the node.
        if node.valueType is None:
            node.valueType = self.valueType
            node.value = self.value
        # Reset value and valueType
        self.value = ""
        self.valueType = None
        if node.type == FIELD and node.valueType is None:
            raise JSONParseError(
                JSON_SYNTAX_ERROR, "Expected value for field: " + node.name)
        # Return the end event for the node.
        return node.endEvent()

    def _arrayValue(self):
        endOfArray = self.node.lastIndex == self.node.arrayLength
        if self.valueType is None and endOfArray:
            pass
        elif self.valueType is None:
            raise JSONParseError(
                JSON_SYNTAX_ERROR,
                "Expected value for array element at index: " +
                str(self.node.arrayLength))
        else:
            event = JSONEvent(
                self.node, ARRAY_VALUE, self.value, self.valueType,
                self.node.arrayLength)
            self.node.lastIndex = self.node.arrayLength
            # Reset value and valueType
            self.value = ""
            self.valueType = None
            # Return the end event for the node.
            return event

    def __iter__(self):
        return self

# Define exceptions


class JSONParseError(Exception):

    def __init__(self, code, msg):
        self.args = (code, msg)
        self.code = code
        self.msg = msg


class JSONNode (object):

    def __init__(self, parent, nodeType, name=None, value=None,
                 valueType=None):
        self.parent = parent
        self.type = nodeType
        self.name = name
        self.value = value
        self.valueType = valueType
        self.arrayIndex = None
        self.arrayLength = None
        self.lastIndex = -1
        if nodeType == ARRAY:
            self.arrayLength = 0

    def startEvent(self):
        if self.type == ARRAY:
            return JSONEvent(self, START_ARRAY, arrayIndex=self.arrayIndex)
        elif self.type == OBJECT:
            return JSONEvent(self, START_OBJECT, arrayIndex=self.arrayIndex)
        elif self.type == FIELD:
            return JSONEvent(self, FIELD_NAME, self.name)

    def endEvent(self):
        if self.type == ARRAY:
            return JSONEvent(self, END_ARRAY, arrayIndex=self.arrayIndex,
                             arrayLength=self.arrayLength)
        elif self.type == OBJECT:
            return JSONEvent(self, END_OBJECT, arrayIndex=self.arrayIndex)
        elif self.type == FIELD and self.valueType not in (OBJECT, ARRAY):
            return JSONEvent(self, FIELD_VALUE, self.value, self.valueType)


class JSONEvent (object):

    def __init__(self, node, eventType, value=None, valueType=None,
                 arrayIndex=None, arrayLength=None):
        self.node = node
        self.type = eventType
        self.value = value
        self.valueType = valueType
        self.arrayIndex = arrayIndex
        self.arrayLength = arrayLength

    def __repr__(self):
        text = "JSONEvent (type=" + self.type
        if self.value is not None:
            text += ", value=" + str(self.value)
        if self.valueType is not None:
            text += ", valueType=" + str(self.valueType)
        if self.arrayIndex is not None:
            text += ", arrayIndex=" + str(self.arrayIndex)
        if self.arrayLength is not None:
            text += ", arrayLength=" + str(self.arrayLength)
        text += ")"
        return text


class JSONArrayIterator (object):

    def __init__(self, parser):
        self.parser = parser
        self.complete = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.complete:
            raise StopIteration()
        else:
            event = self.parser.nextEvent()
            if event.type == START_OBJECT:
                return self.parser.readObject(event)
            elif event.type == START_ARRAY:
                return self.parser.readArray(event)
            elif event.type == ARRAY_VALUE:
                return event.value
            elif event.type == END_ARRAY:
                self.complete = True
                raise StopIteration()
            else:
                raise JSONParseError(
                    JSON_UNEXPECTED_ELEMENT_ERROR,
                    "Unexpected event: " + str(event))

    def next(self):
        return self.__next__()
