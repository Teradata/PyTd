""" Helper classes and interfaces for Teradata Python module. """

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
import re
import argparse
import inspect
import copy
import getpass
from .api import *  # @UnusedWildImport # noqa

INVALID_ARGUMENT = "INVALID_ARGUMENT"

# Create new trace log level
TRACE = 15
logging.addLevelName(TRACE, 'TRACE')


def trace(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kws)
logging.TRACE = TRACE
logging.Logger.trace = trace

logger = logging.getLogger (__name__)

def raiseIfNone(name, value):
    if not value:
        raise InterfaceError(
            INVALID_ARGUMENT, "Missing value for \"{}\".".format(name))


def booleanValue(value):
    retval = value
    if isinstance (value, str):
        retval = value.lower() in ["1", "on", "true", "yes"]
    return retval

class OutParams (object):

    """ Represents a set of Output parameters. """

    def __init__(self, params, dataTypeConverter, outparams=None):
        names = {}
        copy = []
        for p in params:
            if isinstance(p, OutParam):
                if outparams:
                    value = outparams.pop(0)
                else:
                    value = p.value()
                if p.dataType is not None:
                    typeCode = dataTypeConverter.convertType(p.dataType)
                    value = dataTypeConverter.convertValue(
                        p.dataType, typeCode, value)
                if isinstance (p, OutParam) and value is not None and p.size is not None and isinstance (value, (str, bytes, bytearray)):
                    value = value [:p.size]
                copy.append(value)
                if p.name is not None:
                    names[p.name] = value
            else:
                copy.append(p)
        super(OutParams, self).__setattr__("config", copy)
        super(OutParams, self).__setattr__("names", names)

    def __getattr__(self, name):
        try:
            return self.names[name]
        except KeyError:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        raise AttributeError("Output parameters are read only.")

    def __setitem__(self, key, value):
        raise AttributeError("Output parameters are read only.")

    def __getitem__(self, key):
        try:
            return self.config[key]
        except TypeError:
            return self.names[key]

    def __len__(self):
        return len(self.config)

    def __str__(self):
        return str(self.config)

    def __iter__(self):
        return self.config.__iter__()


class SqlScript:

    """An iterator for iterating through the queries in a SQL script."""

    def __init__(self, filename, delimiter=";", encoding=None):
        self.delimiter = delimiter
        with open(filename, mode='r', encoding=encoding) as f:
            self.sql = f.read()

    def __iter__(self):
        return sqlsplit(self.sql, self.delimiter)


class BteqScript:

    """An iterator for iterating through the queries in a BTEQ script."""

    def __init__(self, filename, encoding=None):
        self.file = filename
        with open(self.file, mode='r', encoding=encoding) as f:
            self.lines = f.readlines()

    def __iter__(self):
        return bteqsplit(self.lines)


def sqlsplit(sql, delimiter=";"):
    """A generator function for splitting out SQL statements according to the
     specified delimiter. Ignores delimiter when in strings or comments."""
    tokens = re.split("(--|'|\n|" + re.escape(delimiter) + "|\"|/\*|\*/)",
                      sql if isinstance(sql, str) else delimiter.join(sql))
    statement = []
    inComment = False
    inLineComment = False
    inString = False
    inQuote = False
    for t in tokens:
        if not t:
            continue
        if inComment:
            if t == "*/":
                inComment = False
        elif inLineComment:
            if t == "\n":
                inLineComment = False
        elif inString:
            if t == '"':
                inString = False
        elif inQuote:
            if t == "'":
                inQuote = False
        elif t == delimiter:
            sql = "".join(statement).strip()
            if sql:
                yield sql
            statement = []
            continue
        elif t == "'":
            inQuote = True
        elif t == '"':
            inString = True
        elif t == "/*":
            inComment = True
        elif t == "--":
            inLineComment = True
        statement.append(t)
    sql = "".join(statement).strip()
    if sql:
        yield sql


def linesplit(sql, newline="\n"):
    """A generator function for splitting out SQL statements according to the
     specified delimiter. Ignores delimiter when in strings or comments."""
    tokens = re.split("(--|'|" + re.escape(newline) + "|\"|/\*|\*/)",
                      sql if isinstance(sql, str) else newline.join(sql))
    statement = []
    inComment = False
    inLineComment = False
    inString = False
    inQuote = False
    for t in tokens:
        if inComment:
            if t == "*/":
                inComment = False
            if t == newline:
                sql = "".join(statement)
                yield sql
                statement = []
                continue
        elif inLineComment:
            if t == "\n":
                inLineComment = False
            if t == newline:
                sql = "".join(statement)
                yield sql
                statement = []
                continue
        elif inString:
            if t == '"':
                inString = False
        elif inQuote:
            if t == "'":
                inQuote = False
        elif t == newline:
            sql = "".join(statement)
            yield sql
            statement = []
            continue
        elif t == "'":
            inQuote = True
        elif t == '"':
            inString = True
        elif t == "/*":
            inComment = True
        elif t == "--":
            inLineComment = True
        statement.append(t)
    sql = "".join(statement)
    if sql:
        yield sql


def bteqsplit(lines):
    """A generator function for splitting out SQL statements according
     BTEQ rule."""
    statement = []
    inStatement = False
    inComment = False
    for originalLine in lines:
        line = originalLine.strip()
        if not inStatement:
            if inComment:
                if "*/" in line:
                    inComment = False
                    line = line.split("*/", 1)[1].strip()
                    originalLine = originalLine.split("*/", 1)[1]
                else:
                    continue
            if not line:
                continue
            # Else if BTEQ command.
            elif line.startswith("."):
                continue
            # Else if BTEQ comment.
            elif line.startswith("*"):
                continue
            elif line.startswith("/*"):
                if not line.endswith("*/"):
                    inComment = True
                continue
            else:
                inStatement = True
        statement.append(originalLine)
        if line.endswith(";"):
            sql = "".join(statement).strip()
            if sql:
                yield sql
            statement = []
            inStatement = False
    if inStatement:
        sql = "".join(statement).strip()
        if sql:
            yield sql


def createTestCasePerDSN(testCase, baseCls, dataSourceNames):
    """A method for duplicating test cases, once for each named data source."""
    for dsn in dataSourceNames:
        attr = dict(testCase.__dict__)
        attr['dsn'] = dsn
        newTestCase = type(
            testCase.__name__ + "_" + dsn,  (testCase, baseCls), attr)
        setattr(sys.modules[testCase.__module__],
                newTestCase.__name__, newTestCase)


def setupTestUser(udaExec, dsn, user=None, passwd=None, perm=100000000):
    """A utility method for creating a test user to be used by unittests."""
    if user is None:
        user = "py%s_%std_%s_test" % (
            sys.version_info[0], sys.version_info[1], getpass.getuser())
    if passwd is None:
        passwd = user
    with udaExec.connect(dsn) as conn:
        try:
            conn.execute("DELETE DATABASE " + user)
            conn.execute("MODIFY USER " + user + " AS PERM = %s" % perm)
        except DatabaseError as e:
            if e.code == 3802:
                conn.execute(
                    "CREATE USER " + user +
                    " AS PERM = %s, PASSWORD = %s" % (perm, passwd))
                conn.execute("GRANT UDTTYPE ON SYSUDTLIB to %s" % user)
                conn.execute(
                    "GRANT CREATE PROCEDURE ON %s to %s" % (user, user))
    return user

def cleanupTestUser (udaExec, dsn, user=None, passwd=None):
    """A utility method for dropping a test user used by unittests."""
    if user is None:
        user = "py%s_%std_%s_test" % (
            sys.version_info[0], sys.version_info[1], getpass.getuser())
    if passwd is None:
        passwd = user
    with udaExec.connect(dsn) as conn:
        conn.execute("DELETE DATABASE " + user)
        conn.execute("DROP USER " + user)

class CommandLineArgumentParser:

    """ Command Line Argument Parser that matches command line arguments to
     the functions in a module."""

    def __init__(self, moduleName, optionalArgs=None, positionalArgs=None):
        module = sys.modules[moduleName]
        preparser = argparse.ArgumentParser(add_help=False)
        if optionalArgs:
            for argument in optionalArgs:
                preparser.add_argument(*argument.args, **argument.kwargs)
        glob, extra = preparser.parse_known_args()
        parser = argparse.ArgumentParser(
            description=module.__doc__, parents=[preparser])
        targetparser = parser.add_subparsers(
            metavar="targets", help="one or more targets to execute")
        targetparser.required = True
        for name, func in inspect.getmembers(module, inspect.isfunction):
            if not name.startswith("_") and func.__doc__:
                p = targetparser.add_parser(name, help=func.__doc__)
                if optionalArgs:
                    for argument in optionalArgs:
                        if argument.targets is None or func \
                                in argument.targets:
                            p.add_argument(*argument.args, **argument.kwargs)
                if positionalArgs:
                    for argument in positionalArgs:
                        if argument.targets is None or func \
                                in argument.targets:
                            p.add_argument(*argument.args, **argument.kwargs)
                p.set_defaults(func=func, name=name)
        self.arguments = []
        while True:
            args, extra = parser.parse_known_args(
                extra, namespace=copy.copy(glob))
            self.arguments.append(args)
            if sum((0 if arg.startswith("-") else 1 for arg in extra)) == 0:
                break

    def __iter__(self):
        return iter(self.arguments)


class CommandLineArgument:

    """ Represents a command line argument."""

    def __init__(self, *args, **kwargs):
        self.targets = None
        if "targets" in kwargs:
            self.targets = kwargs.pop("targets")
        self.args = args
        self.kwargs = kwargs
