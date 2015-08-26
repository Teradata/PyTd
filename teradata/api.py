"""Defines global variables, helper classes, and exceptions classes
 for implementations of the Python Database API Specification v2.0"""

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
from .version import *  # noqa
import datetime
import decimal

# DB API 2.0 globals
apilevel = "2.0"
threadsafety = 1
paramstyle = 'qmark'

CONFIG_ERROR = "CONFIG_ERROR"


class OutParam ():

    """Represents an output parameter from a Stored Procedure"""

    def __init__(self, name=None, dataType=None, size=None):
        self.name = name
        self.dataType = dataType
        self.size = size
        self.valueFunc = None

    def setValueFunc(self, valueFunc):
        self.valueFunc = valueFunc

    def value(self):
        return None if self.valueFunc is None else self.valueFunc()

    def __repr__(self):
        return "OutParam(name={}, size={})".format(self.name, self.size)


class InOutParam (OutParam):

    """Represents an input and output parameter from a Stored Procedure"""

    def __init__(self, value, name=None, dataType=None, size=None):
        OutParam.__init__(self, name, dataType, size)
        self.inValue = value

    def __repr__(self):
        return "InOutParam(value={}, name={}, dataType={}, size={})".format(
            self.inValue, self.name, self.dataType, self.size)

# Define exceptions


class Error(Exception):

    def __init__(self, msg):
        self.args = (msg,)
        self.msg = msg


class InterfaceError(Error):

    """Represents an error in using Teradata's implementation of the Python
     Database API Specification v2.0"""

    def __init__(self, code, msg):
        self.args = (code, msg)
        self.code = code
        self.msg = msg


class DatabaseError(Error):

    """Represents an error returned by the Database"""

    def __init__(self, code, msg, sqlState=None):
        self.args = (code, msg)
        self.code = code
        self.msg = msg
        self.sqlState = sqlState

# Definitions for types
BINARY = bytearray
Binary = bytearray
DATETIME = datetime.datetime
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
STRING = str
NUMBER = decimal.Decimal
ROWID = int
DateFromTicks = datetime.date.fromtimestamp
TimeFromTicks = lambda x: datetime.datetime.fromtimestamp(x).time()
TimestampFromTicks = datetime.datetime.fromtimestamp
