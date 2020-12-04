"""An implementation of the Python Database API Specification v2.0
 using Teradata Python Driver."""

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

import atexit
import collections
import ctypes
import inspect
import json
import platform
import re
import sys
import threading

import logging

import teradatasql

from . import util, datatypes
from .api import *  # @UnusedWildImport # noqa


logger = logging.getLogger(__name__)

NATIVE_SQL_AUTOCOMMIT_ON  = "{fn teradata_nativesql}{fn teradata_autocommit_on}"
NATIVE_SQL_AUTOCOMMIT_OFF = "{fn teradata_nativesql}{fn teradata_autocommit_off}"
NATIVE_SQL_SESSION_NUMBER = "{fn teradata_nativesql}{fn teradata_session_number}"
TERADATA_FAKE_RESULT_SETS = "{fn teradata_fake_result_sets}"

#FAKE RESULTS
NATIVE_SQL      = 0
WARNING_CODE    = 5
WARNING_MSG     = 6
COLUMN_METADATA = 7

lock = threading.Lock()

# The amount of seconds to wait when submitting non-user defined SQL (e.g.
# set query bands, etc). Currently not being used.
QUERY_TIMEOUT = 120

connections = []

def cleanupConnections():
    """Cleanup open connections."""
    if connections:
        logger.warning(
            "%s open connections found on exit, attempting to close...",
            len(connections))
        for conn in list(connections):
            conn.close()


class TeradataSqlConnection:

    """Represents a Connection to Teradata using Teradata Python Driver."""

    def __init__(self, system=None,
                 username=None, autoCommit=True,
                 transactionMode=None, queryBands=None,
                 dataTypeConverter=datatypes.DefaultDataTypeConverter(),
                 charset=None, **kwargs):
        """Creates a teradatasql connection."""

        if charset is not None and charset != 'UTF8':
            raise InterfaceError(util.INVALID_ARGUMENT,
                "Connection charset {} is not valid only UTF8 is supported".format(charset))

        if "host" not in kwargs and system is not None:
            kwargs ["host"] = system

        if "user" not in kwargs and username is not None:
            kwargs ["user"] = username

        if "tmode" not in kwargs and transactionMode is not None:
            kwargs ["tmode"] = transactionMode

        sConParams = json.dumps(kwargs)
        logger.trace ('> enter __init__ {} sConParams={} kwargs={}'.format (self.__class__.__name__, sConParams, kwargs))
        try:
            self.cursorCount = 0
            self.sessionno = 0
            self.cursors = []
            self.converter = dataTypeConverter
            bAutoCommit = util.booleanValue(autoCommit)

            # Create connection
            logger.debug("Creating connection using teradatasql parameters are: %s",
                        re.sub("password=.*?(;|$)", "password=XXX;", sConParams))

            try:
                lock.acquire()
                try:
                    self.conn = teradatasql.connect (sConParams)
                except Exception as e:
                    raise (_convertError (e))
            finally:
                lock.release()
            connections.append(self)

            # Setup autocommit, query bands, etc.
            try:
                with self.conn.cursor () as c:
                    if not bAutoCommit:
                        logger.debug("Turning off AUTOCOMMIT")
                        c.execute (NATIVE_SQL_AUTOCOMMIT_OFF)
                    self.sessionno = c.execute (NATIVE_SQL_SESSION_NUMBER).fetchone () [0]
                    logger.debug("SELECT SESSION returned %s", self.sessionno)
                    if queryBands:
                        c.execute(u"SET QUERY_BAND = '{};' FOR SESSION".format(
                            u";".join(u"{}={}".format(k,v)
                                    for k, v in queryBands.items())))

                    if not bAutoCommit:
                        self.commit()
                    logger.debug("Created session %s.", self.sessionno)
            except Exception:
                self.close()
                raise
        finally:
            logger.trace ('> leave __init__ {}'.format (self))
        # end __init__

    def close(self):
        """Closes a teradatasql Connection."""
        logger.trace ('> enter close {}'.format (self))
        try:
            if self.sessionno:
                logger.debug("Closing session %s...", self.sessionno)
                for cursor in list(self.cursors):
                    cursor.close()
                self.conn.close ()
                connections.remove(self)
                if self.sessionno:
                    logger.debug("Session %s closed.", self.sessionno)
                self.sessionno = 0
        finally:
            logger.trace ('< leave close {}'.format (self))
        #end close

    def commit (self):
        """Commits a transaction."""
        logger.debug ("Committing transaction...")
        self.conn.commit ()

    def rollback(self):
        """Rollsback a transaction."""
        logger.debug("Rolling back transaction...")
        self.conn.rollback ()

    def cursor(self):
        """Returns a cursor."""
        cursor = TeradataSqlCursor (self, self.conn, self.converter, self.cursorCount)
        self.cursorCount += 1
        return cursor

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()

    def __repr__(self):
        return "{} sessionno={}".format(self.__class__.__name__, self.sessionno)

    # end class TeradataSqlConnection

connect = TeradataSqlConnection


class TeradataSqlCursor:

    """Represents a teradatasql cursor."""

    def __init__(self, connection, tdConn, converter, num):
        self.connection = connection
        self.converter = converter
        self.results = None
        self.arraysize = 1
        self.fetchSize = None
        self.rowcount = -1
        self.description = None
        self.columns = None
        self.types = None
        self.aLobLocators = {}
        self.iterator = None
        self.rownumber = None

        self.num = num
        self.moreResults = None
        if num > 0:
            logger.debug(
                "Creating cursor %s for session %s.", self.num,
                self.connection.sessionno)
        self.tdConn = tdConn
        self.cur = tdConn.cursor ()
        connection.cursors.append(self)
        self.bClosed = False
        # end __init__

    def callproc(self, procname, params, queryTimeout=0):
        logger.trace ('> enter callproc ({}) {}'.format (procname, self))
        try:
            if params is None:
                sCall = "{call " + procname + "()}"
            else:
                asPars = ["?" if type (params [i]) is not OutParam else "p%s" % i for i in range (len(params))]

                sCall = "{call " + procname + "(" + ', '.join (asPars) + ")}"
                sEscParamTypes = ""
                nIndex = 1
                aoParams = []
                for nParam in range (0, len (params)):
                    if isinstance(params [nParam], InOutParam) or isinstance (params [nParam], InParam):
                        if params [nParam].escapeParamType is not None:
                            sCall = "{fn teradata_parameter(%s, %s)} " % (nIndex, params [nParam].escapeParamType) + sCall
                        if params [nParam].dataType is not None and params [nParam].dataType.startswith("PERIOD"):
                            params [nParam].setValueFunc (lambda: datatypes.removeTrailingZerosFromPeriod(params [nParam].inValue))
                        elif params [nParam].dataType is not None and params [nParam].dataType.startswith("TIME"):
                            params [nParam].setValueFunc (lambda: datatypes.removeTrailingZerosFromTimeAndTimestamp(params [nParam].inValue))
                        else:
                            params [nParam].setValueFunc (lambda: params [nParam].inValue)
                        logger.debug ("appending values {}".format (params [nParam].value ()))
                        aoParams.append (params [nParam].value ())

                    elif not isinstance(params [nParam], OutParam):
                        aoParams.append (params [nParam])
                    if not isinstance (params [nParam], OutParam) or isinstance(params [nParam], InOutParam):
                        nIndex += 1

            logger.debug("Executing Procedure: %s", sCall)
            self.executemany(sCall, params=aoParams, queryTimeout=queryTimeout)
            return util.OutParams(params, self.converter, outparams=self._getRow())
        finally:
            logger.trace ('< leave callproc {}'.format (self))
        # end callproc

    def close(self):
        logger.trace ('> enter close {}'.format (self))
        try:
            if not self.bClosed:
                self.bClosed = True
                if self.num > 0:
                    logger.debug(
                        "Closing cursor %s for session %s.", self.num,
                        self.connection.sessionno)
                self.cur.close ()
                self.connection.cursors.remove(self)
        finally:
            logger.trace ('< leave close {}'.format (self))
        # end close

    def _setQueryTimeout(self, queryTimeout):
        pass

    def execute(self, query, params=None, queryTimeout=0):
        logger.trace ('> enter execute {}'.format (self))
        try:
            return self.executemany (query, params, queryTimeout)
        finally:
            logger.trace ('> leave execute {}'.format (self))
        # end execute

    def executemany (self, query, params, batch=False, ignoreErrors = None, queryTimeout=0):
        logger.trace ('> enter executemany {} : {}'.format (self, query))
        try:
            self._setQueryTimeout(queryTimeout)

            self.bFakeResult = True
            if (params):

                if isinstance (params, tuple):
                    params = list (params)

                # Need to convert interval and period types to their string values
                for i in range (0, len (params)):
                    if isinstance (params [i], list):
                        for j in range (0, len (params [i])):
                            if isinstance (params [i][j], (datatypes.Interval, datatypes.Period)):
                                params [i][j] = str (params [i][j])
                    elif isinstance (params [i], (datatypes.Interval, datatypes.Period)):
                        params [i] = str (params [i])

                # If batch is false and a batch request was submitted, process each request one by one
                if not batch and len (params) > 0 and type (params [0]) in [list, tuple]:
                    logger.debug("Executing each query in the batch one by one")
                    try:
                        for p in params:
                            self.cur.execute (TERADATA_FAKE_RESULT_SETS + query, p)
                            self._handleResults()
                    except Exception as e:
                        raise (_convertError (e))
                    return self

            try:
                if self.connection.sessionno:
                    logger.debug(
                        "Executing query on session %s using execute: %s",
                        self.connection.sessionno, query)
                self.cur.execute (TERADATA_FAKE_RESULT_SETS + query, params, ignoreErrors)
            except Exception as e:
                raise (_convertError (e))
            self._handleResults()
            return self
        finally:
            logger.trace ('> leave executeMany {}'.format (self))
        # end executeMany

    def _handleResults(self):
        logger.trace ('> enter _handleResults {}'.format (self))
        try:
            self.columnCount = 0
            self.moreResults = False

            self._obtainResultMetaData ()
            # After obtaining result set metadata, check if a resuslt set was returned
            if not self.moreResults:
                return
            self.columnCount = len (self.cur.description)
            self.rowcount = self.cur.rowcount
            logger.debug ("Row count {}, column count {}".format (self.rowcount, self.columnCount))
            self.iterator = self.rowIterator()
            # Processing current result, set more results to false to force self.nextset to call self.cur.nextset
            self.moreResults = False
        finally:
            logger.trace ('< leave _handleResults {}'.format (self))
        # end _handleResults

    def _obtainResultMetaData (self):
        logger.trace ('> enter _obtainResultMetaData {}'.format (self))
        try:
            # It is possible to use cur.description returned from teradata.sql but we
            # need to identify lobs

            row = self.cur.fetchone ()

            if not self.bFakeResult:
                self.description = self.cur.description
                # Don't move past current result
                self.moreResults = True
                return

            if logger.isEnabledFor (logging.DEBUG):
                [ logger.debug (" Column {} {:15} = {}".format (i + 1, self.cur.description [i][0], row [i])) for i in range (0, len (row)) ]

            if int (row [WARNING_CODE]) > 0:
                logger.warning ("{} succeeded with warning: [code ]{} message {}".format (row [NATIVE_SQL], row [WARNING_CODE], row [WARNING_MSG]))

            aJsonColMetadata = json.loads (row [COLUMN_METADATA])
            if aJsonColMetadata is None:
                # No column metadata returned get empty result set
                self.moreResults = self.cur.nextset ()
                return

            columnCount = len (aJsonColMetadata)
            # Get column meta data
            if columnCount > 0:
                self.description = []
                self.columns = {}
                self.types = []
                self.aLobLocators = {}
                for col in range(0, columnCount):
                    columnName = aJsonColMetadata [col] ['Title'] if aJsonColMetadata [col] ['Title'] is not None else aJsonColMetadata [col] ['Name']
                    sTypeName = aJsonColMetadata [col] ['TypeName']
                    pythonType = self.converter.convertType(sTypeName)
                    columnSize = aJsonColMetadata [col] ['ByteCount']
                    decimalDigits = aJsonColMetadata [col] ['Precision']
                    nullable = aJsonColMetadata [col] ['Nullable']
                    nCookedType = aJsonColMetadata [col] ['CookedDataType']
                    if nCookedType in datatypes.LOB_LOCATOR_TYPES:
                        self.aLobLocators [col + 1] = datatypes.LOB_LOCATOR_TYPES [nCookedType]
                    self.columns[columnName.lower()] = col
                    self.types.append((sTypeName, pythonType))
                    self.description.append((
                        columnName, pythonType, None, columnSize,
                        decimalDigits, None, nullable))
            # Move past metadata result set
            self.moreResults = self.cur.nextset ()
        finally:
            logger.trace ('< leave _obtainResultMetaData {}'.format (self))
        # end _obtainResultMetaData

    def fetchone(self):
        self.fetchSize = 1
        return next(self, None)

    def fetchmany(self, size=None):
        logger.trace ('> enter fetchmany {}'.format (self))
        try:
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
        finally:
            logger.trace ('< leave fetchmany {}'.format (self))

    def fetchall(self):
        logger.trace ('> enter fetchall {}'.format (self))
        try:
            self.fetchSize = self.arraysize
            rows = []
            for row in self:
                rows.append(row)
            return rows
        finally:
            logger.trace ('< leave fetchall {}'.format (self))
        #end fetchall

    def nextset(self):
        logger.trace ('> enter nextset {}'.format (self))
        try:
            if not self.moreResults:
                self.moreResults = self.cur.nextset ()

            if self.moreResults:
                self._handleResults()
                return True
        finally:
            logger.trace ('< leave nextset {}'.format (self))
        # end nextset

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        logger.trace ("> enter __next__ {}".format (self))
        try:
            self.fetchSize = self.arraysize
            if self.iterator:
                if self.rownumber is None:
                    self.rownumber = 0
                else:
                    self.rownumber += 1
                values = next(self.iterator)
                for i in range(0, len(values)):
                    values[i] = self.converter.convertValue(
                        self.types[i][0], self.types[i][1], values[i])
                row = Row(self.columns, values, self.rownumber + 1)
                if logger.isEnabledFor (logging.DEBUG):
                    [ logger.debug (" Column {} {:15} = {}".format (i + 1, self.cur.description [i][0], row [i])) for i in range (0, len (row)) ]

                return row
            raise StopIteration()
        finally:
            logger.trace ('< leave __next__ {}'.format (self))
        #end __next__

    def next(self):
        return self.__next__()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()

    def __repr__(self):
        return "{} sessionno={} cursor={} bClosed={}".format (self.__class__.__name__,
                self.connection.sessionno, self.num, self.bClosed)

    def rowIterator (self):
        for rowIndex in range(0, self.rowcount):
            yield self._getRow()

    def _getRow(self):
        """Reads a row of data.  If the column type is a LOB,
        then that data is obtained via a call to _readLobValue."""
        logger.trace ('> enter _getRow {}'.format (self))
        try:
            aPyRow = self.cur.fetchone ()
            row = []
            oColVal = None
            for col in range(1, len(self.cur.description) + 1):

                if aPyRow [col - 1] is not None and col in self.aLobLocators:
                    logger.debug ("retrieving lob value col = {}".format (col))
                    row.append (self._readLobValue (aPyRow [col - 1], self.aLobLocators [col]))
                else:
                    row.append (aPyRow [col - 1])

            return row
        finally:
            logger.trace ('< leave _getRow {}'.format (self))
        # end _getRow

    def _readLobValue (self, abyInputLocator, sDataType):
        logger.trace ("> enter _readLobValue {}".format (self))
        try:
            if type (abyInputLocator) is not bytes:
                raise Error ("abyInputLocator must be bytes not {}".format (type (abyInputLocator)))

            with self.tdConn.cursor () as c:
                olobValue = c.execute ("{fn teradata_parameter(1," +  sDataType + ")}select ?", [abyInputLocator]).fetchone () [0]
                self.connection.commit()

            return (olobValue)
        finally:
            logger.trace ('< leave _readLobValue {}'.format (self))
        # end _readLobValue

    # end class TeradataSqlCursor


def _convertError (e):
    nErrCode = 0
    sErrMsg = "{}".format (e)

    mat = re.compile ("\\[Error (\\d+)\\]").search (sErrMsg)
    if mat and mat.lastindex == 1:
        nErrCode = int (mat.group (1))

    if "[Teradata Database]" in sErrMsg:
        return DatabaseError (nErrCode, sErrMsg)

    if e.__class__.__name__ == "ProgrammingError":
        return ProgrammingError (nErrCode, sErrMsg)

    return OperationalError (nErrCode, sErrMsg)
    #end _convertError


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
            if isinstance (key, int) and key < 0 or key >= len (self.values):
                raise ProgrammingError (0, "Invalid key index {} cannot be less than 0 or greater than or equal to size of results {} ".format (key, len (self.values)))
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


