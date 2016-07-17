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
import codecs
import argparse
import inspect
import copy
import getpass
from .api import *  # @UnusedWildImport # noqa

INVALID_ARGUMENT = "INVALID_ARGUMENT"

# Create new trace log level
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


def trace(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kws)
logging.TRACE = TRACE
logging.Logger.trace = trace

logger = logging.getLogger(__name__)

if sys.version_info[0] == 2:
    openfile = codecs.open
else:
    openfile = open


def isString(value):
    # Implement python version specific setup.
    if sys.version_info[0] == 2:
        return isinstance(value, basestring)  # @UndefinedVariable
    else:
        return isinstance(value, str)  # @UndefinedVariable


def toUnicode(string):
    if not isString(string):
        string = str(string)
    if sys.version_info[0] == 2:
        if isinstance(string, str):
            string = string.decode("utf8")
    return string


def raiseIfNone(name, value):
    if not value:
        raise InterfaceError(
            INVALID_ARGUMENT, "Missing value for \"{}\".".format(name))


def booleanValue(value):
    retval = value
    if isString(value):
        retval = value.lower() in ["1", "on", "true", "yes"]
    return retval


class Cursor:

    """An abstract cursor for encapsulating shared functionality of connection
     specific implementations (e.g. ODBC, REST)"""

    def __init__(self, connection, dbType, dataTypeConverter):
        self.connection = connection
        self.converter = dataTypeConverter
        self.dbType = dbType
        self.results = None
        self.arraysize = 1
        self.fetchSize = None
        self.rowcount = -1
        self.description = None
        self.types = None
        self.iterator = None
        self.rownumber = None

    def callproc(self, procname, params):
        # Abstract method, defined by convention only
        raise NotImplementedError("Subclass must implement abstract method")

    def close(self):
        pass

    def execute(self, query, params=None):
        # Abstract method, defined by convention only
        raise NotImplementedError("Subclass must implement abstract method")

    def executemany(self, query, params, batch=False):
        # Abstract method, defined by convention only
        raise NotImplementedError("Subclass must implement abstract method")

    def fetchone(self):
        self.fetchSize = 1
        return next(self, None)

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        self.fetchSize = size
        rows = []
        count = 0
        for row in self:
            rows.append(row)
            count += 1
            if count == size:
                break
        return rows

    def fetchall(self):
        self.fetchSize = self.arraysize
        rows = []
        for row in self:
            rows.append(row)
        return rows

    def nextset(self):
        # Abstract method, defined by convention only
        raise NotImplementedError("Subclass must implement abstract method")

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        self.fetchSize = self.arraysize
        if self.iterator:
            if self.rownumber is None:
                self.rownumber = 0
            else:
                self.rownumber += 1
            values = next(self.iterator)
            for i in range(0, len(values)):
                values[i] = self.converter.convertValue(
                    self.dbType, self.types[i][0], self.types[i][1], values[i])
            row = Row(self.columns, values, self.rownumber + 1)
            # logger.debug("%s", row)
            return row
        raise StopIteration()

    def next(self):
        return self.__next__()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()


class Row (object):

    """Represents a table row."""

    def __init__(self, columns, values, rowNum):
        super(Row, self).__setattr__("columns", columns)
        super(Row, self).__setattr__("values", values)
        super(Row, self).__setattr__("rowNum", rowNum)

    def __getattr__(self, name):
        try:
            index = self.columns[name.lower()]
            return self.values[index]
        except KeyError:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        try:
            self.values[self.columns[name.lower()]] = value
        except KeyError:
            raise AttributeError("No such attribute: " + name)

    def __setitem__(self, key, value):
        try:
            self.values[key] = value
        except TypeError:
            self.values[self.columns[key.lower()]] = value

    def __getitem__(self, key):
        try:
            return self.values[key]
        except TypeError:
            index = self.columns[key.lower()]
            return self.values[index]

    def __len__(self):
        return len(self.values)

    def __str__(self):
        return "Row " + str(self.rowNum) + ": [" + \
            ", ".join(map(str, self.values)) + "]"

    def __iter__(self):
        return self.values.__iter__()


class OutParams (object):

    """ Represents a set of Output parameters. """

    def __init__(self, params, dbType, dataTypeConverter, outparams=None):
        names = {}
        copy = []
        for p in params:
            if isinstance(p, OutParam):
                if outparams:
                    value = outparams.pop(0)
                else:
                    value = p.value()
                if p.dataType is not None:
                    typeCode = dataTypeConverter.convertType(
                        dbType, p.dataType)
                    value = dataTypeConverter.convertValue(
                        dbType, p.dataType, typeCode, value)
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
        with openfile(filename, mode='r', encoding=encoding) as f:
            self.sql = f.read()

    def __iter__(self):
        return sqlsplit(self.sql, self.delimiter)


class BteqScript:

    """An iterator for iterating through the queries in a BTEQ script."""

    def __init__(self, filename, encoding=None):
        self.file = filename
        with openfile(self.file, mode='r', encoding=encoding) as f:
            self.lines = f.readlines()

    def __iter__(self):
        return bteqsplit(self.lines)


def sqlsplit(sql, delimiter=";"):
    """A generator function for splitting out SQL statements according to the
     specified delimiter. Ignores delimiter when in strings or comments."""
    tokens = re.split("(--|'|\n|" + re.escape(delimiter) + "|\"|/\*|\*/)",
                      sql if isString(sql) else delimiter.join(sql))
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
                      sql if isString(sql) else newline.join(sql))
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
    """A utility method for creating a test user to be use by unittests."""
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
                    " FROM DBC AS PERM = %s, PASSWORD = %s" % (perm, passwd))
                conn.execute("GRANT UDTTYPE ON SYSUDTLIB to %s" % user)
                conn.execute(
                    "GRANT CREATE PROCEDURE ON %s to %s" % (user, user))
    return user


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
