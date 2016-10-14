"""An implementation of the Python Database API Specification v2.0
 using Teradata ODBC."""

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
import ctypes
import threading
import atexit
import platform
import re
import collections

from . import util, datatypes
from .api import *  # @UnusedWildImport # noqa

logger = logging.getLogger(__name__)

# ODBC Constants
SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC2, SQL_OV_ODBC3 = 200, 2, 3
SQL_ATTR_QUERY_TIMEOUT, SQL_ATTR_AUTOCOMMIT = 0, 102
SQL_NULL_HANDLE, SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT = 0, 1, 2, 3
SQL_SUCCESS, SQL_SUCCESS_WITH_INFO = 0, 1,
SQL_ERROR, SQL_INVALID_HANDLE = -1, -2
SQL_NEED_DATA, SQL_NO_DATA = 99, 100
SQL_CLOSE, SQL_UNBIND, SQL_RESET_PARAMS = 0, 2, 3
SQL_PARAM_TYPE_UNKNOWN = 0
SQL_PARAM_INPUT, SQL_PARAM_INPUT_OUTPUT, SQL_PARAM_OUTPUT = 1, 2, 4
SQL_ATTR_PARAM_BIND_TYPE = 18
SQL_ATTR_ROWS_FETCHED_PTR, SQL_ATTR_ROW_STATUS_PTR = 26, 25
SQL_ATTR_ROW_ARRAY_SIZE = 27
SQL_ATTR_PARAMS_PROCESSED_PTR, SQL_ATTR_PARAM_STATUS_PTR = 21, 20
SQL_ATTR_PARAMSET_SIZE = 22
SQL_PARAM_BIND_BY_COLUMN = 0
SQL_NULL_DATA, SQL_NTS = -1, -3
SQL_IS_POINTER, SQL_IS_UINTEGER, SQL_IS_INTEGER = -4, -5, -6
SQL_FETCH_NEXT, SQL_FETCH_FIRST, SQL_FETCH_LAST = 1, 2, 4

SQL_SIGNED_OFFSET = -20
SQL_C_BINARY, SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY = -2, -2, -3, -4
SQL_C_WCHAR, SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR = -8, -8, -9, -10
SQL_C_SBIGINT = -5 + SQL_SIGNED_OFFSET
SQL_FLOAT = 6
SQL_C_FLOAT = SQL_REAL = 7
SQL_C_DOUBLE = SQL_DOUBLE = 8
SQL_DESC_TYPE_NAME = 14
SQL_COMMIT, SQL_ROLLBACK = 0, 1

SQL_STATE_DATA_TRUNCATED = '01004'
SQL_STATE_CONNECTION_NOT_OPEN = '08003'
SQL_STATE_INVALID_TRANSACTION_STATE = '25000'

SQLLEN = ctypes.c_ssize_t
SQLULEN = ctypes.c_size_t
SQLUSMALLINT = ctypes.c_ushort
SQLSMALLINT = ctypes.c_short
SQLINTEGER = ctypes.c_int
SQLFLOAT = ctypes.c_float
SQLDOUBLE = ctypes.c_double
SQLBYTE = ctypes.c_ubyte
SQLCHAR = ctypes.c_char
SQLWCHAR = ctypes.c_wchar
SQLRETURN = SQLSMALLINT
SQLPOINTER = ctypes.c_void_p
SQLHANDLE = ctypes.c_void_p

ADDR = ctypes.byref
PTR = ctypes.POINTER
ERROR_BUFFER_SIZE = 2 ** 10
SMALL_BUFFER_SIZE = 2 ** 12
LARGE_BUFFER_SIZE = 2 ** 20
TRUE = 1
FALSE = 0

odbc = None
hEnv = None
drivers = None
lock = threading.Lock()
pyVer = sys.version_info[0]
osType = platform.system()

# The amount of seconds to wait when submitting non-user defined SQL (e.g.
# set query bands, etc).
QUERY_TIMEOUT = 120

if pyVer > 2:
    unicode = str  # @ReservedAssignment

if osType == "Darwin" or osType == "Windows" or osType.startswith('CYGWIN'):
    # Mac OSx and Windows
    _createBuffer = lambda l: ctypes.create_unicode_buffer(l)
    _inputStr = lambda s, l = None: None if s is None else \
        ctypes.create_unicode_buffer((s if util.isString(s) else str(s)), l)
    _outputStr = lambda s: s.value
    _convertParam = lambda s: None if s is None else (
        s if util.isString(s) else str(s))
else:
    # Unix/Linux
    # Multiply by 3 as one UTF-16 character can require 3 UTF-8 bytes.
    _createBuffer = lambda l: ctypes.create_string_buffer(l * 3)
    _inputStr = lambda s, l = None: None if s is None else \
        ctypes.create_string_buffer((s if util.isString(s) else str(s)).encode(
            'utf8'), l)
    _outputStr = lambda s: unicode(s.raw.partition(b'\00')[0], 'utf8')
    _convertParam = lambda s: None if s is None else (
        (s if util.isString(s) else str(s)).encode('utf8'))
    SQLWCHAR = ctypes.c_char

connections = []


def cleanupConnections():
    """Cleanup open connections."""
    if connections:
        logger.warn(
            "%s open connections found on exit, attempting to close...",
            len(connections))
        for conn in list(connections):
            conn.close()


def getDiagnosticInfo(handle, handleType=SQL_HANDLE_STMT):
    """Gets diagnostic information associated with ODBC calls, particularly
     when errors occur."""
    info = []
    infoNumber = 1
    while True:
        sqlState = _createBuffer(6)
        nativeError = SQLINTEGER()
        messageBuffer = _createBuffer(ERROR_BUFFER_SIZE)
        messageLength = SQLSMALLINT()
        rc = odbc.SQLGetDiagRecW(handleType, handle, infoNumber, sqlState,
                                 ADDR(nativeError), messageBuffer,
                                 len(messageBuffer), ADDR(messageLength))
        if rc == SQL_SUCCESS_WITH_INFO and \
                messageLength.value > ctypes.sizeof(messageBuffer):
            # Resize buffer to fit entire message.
            messageBuffer = _createBuffer(messageLength.value)
            continue
        if rc == SQL_SUCCESS or rc == SQL_SUCCESS_WITH_INFO:
            info.append(
                (_outputStr(sqlState), _outputStr(messageBuffer),
                 abs(nativeError.value)))
            infoNumber += 1
        elif rc == SQL_NO_DATA:
            return info
        elif rc == SQL_INVALID_HANDLE:
            raise InterfaceError(
                'SQL_INVALID_HANDLE',
                "Invalid handle passed to SQLGetDiagRecW.")
        elif rc == SQL_ERROR:
            if infoNumber > 1:
                return info
            raise InterfaceError(
                "SQL_ERROR", "SQL_ERROR returned from SQLGetDiagRecW.")
        else:
            raise InterfaceError(
                "UNKNOWN_RETURN_CODE",
                "SQLGetDiagRecW returned an unknown return code: %s", rc)


def checkStatus(rc, hEnv=SQL_NULL_HANDLE, hDbc=SQL_NULL_HANDLE,
                hStmt=SQL_NULL_HANDLE, method="Method", ignore=None):
    """ Check return status code and log any information or error messages.
     If error is returned, raise exception."""
    sqlState = []
    logger.trace("%s returned status code %s", method, rc)
    if rc not in (SQL_SUCCESS, SQL_NO_DATA):
        if hStmt != SQL_NULL_HANDLE:
            info = getDiagnosticInfo(hStmt, SQL_HANDLE_STMT)
        elif hDbc != SQL_NULL_HANDLE:
            info = getDiagnosticInfo(hDbc, SQL_HANDLE_DBC)
        else:
            info = getDiagnosticInfo(hEnv, SQL_HANDLE_ENV)
        for i in info:
            sqlState.append(i[0])
            if rc == SQL_SUCCESS_WITH_INFO:
                logger.debug(
                    u"{} succeeded with info:  [{}] {}".format(method,
                                                               i[0], i[1]))
            elif not ignore or i[0] not in ignore:
                logger.debug((u"{} returned non-successful error code "
                              u"{}: [{}] {}").format(method, rc, i[0], i[1]))
                msg = ", ".join(map(lambda m: m[1], info))
                if re.search(r'[^0-9\s]', msg) is None or i[0] == 'I':
                    msg = msg + (". Check that the ODBC driver is installed "
                                 "and the ODBCINI or ODBCINST environment "
                                 "variables are correctly set.")
                raise DatabaseError(i[2], u"[{}] {}".format(i[0], msg), i[0])
            else:
                logger.debug(
                    u"Ignoring return of {} from {}:  [{}] {}".format(rc,
                                                                      method,
                                                                      i[0],
                                                                      i[1]))
                # Breaking here because this error is ignored and info could
                # contain older error messages.
                # E.g. if error was SQL_STATE_CONNECTION_NOT_OPEN, the next
                # error would be the original connection error.
                break
        if not info:
            logger.info(
                "No information associated with return code %s from %s",
                rc, method)
    return sqlState


def prototype(func, *args):
    """Setup function prototype"""
    func.restype = SQLRETURN
    func.argtypes = args


def initFunctionPrototypes():
    """Initialize function prototypes for ODBC calls."""
    prototype(odbc.SQLAllocHandle, SQLSMALLINT, SQLHANDLE, PTR(SQLHANDLE))
    prototype(odbc.SQLGetDiagRecW, SQLSMALLINT, SQLHANDLE, SQLSMALLINT,
              PTR(SQLWCHAR), PTR(SQLINTEGER), PTR(SQLWCHAR), SQLSMALLINT,
              PTR(SQLSMALLINT))
    prototype(odbc.SQLSetEnvAttr, SQLHANDLE,
              SQLINTEGER, SQLPOINTER, SQLINTEGER)
    prototype(odbc.SQLDriverConnectW, SQLHANDLE, SQLHANDLE,
              PTR(SQLWCHAR), SQLSMALLINT, PTR(SQLWCHAR), SQLSMALLINT,
              PTR(SQLSMALLINT), SQLUSMALLINT)
    prototype(odbc.SQLFreeHandle, SQLSMALLINT, SQLHANDLE)
    prototype(odbc.SQLExecDirectW, SQLHANDLE, PTR(SQLWCHAR), SQLINTEGER)
    prototype(odbc.SQLNumResultCols, SQLHANDLE, PTR(SQLSMALLINT))
    prototype(odbc.SQLDescribeColW, SQLHANDLE, SQLUSMALLINT, PTR(SQLWCHAR),
              SQLSMALLINT, PTR(SQLSMALLINT), PTR(SQLSMALLINT), PTR(SQLULEN),
              PTR(SQLSMALLINT), PTR(SQLSMALLINT))
    prototype(odbc.SQLColAttributeW, SQLHANDLE, SQLUSMALLINT,
              SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, PTR(SQLSMALLINT),
              PTR(SQLLEN))
    prototype(odbc.SQLFetch, SQLHANDLE)
    prototype(odbc.SQLGetData, SQLHANDLE, SQLUSMALLINT,
              SQLSMALLINT, SQLPOINTER, SQLLEN, PTR(SQLLEN))
    prototype(odbc.SQLFreeStmt, SQLHANDLE, SQLUSMALLINT)
    prototype(odbc.SQLPrepareW, SQLHANDLE, PTR(SQLWCHAR), SQLINTEGER)
    prototype(odbc.SQLNumParams, SQLHANDLE, PTR(SQLSMALLINT))
    prototype(odbc.SQLDescribeParam, SQLHANDLE, SQLUSMALLINT, PTR(
        SQLSMALLINT), PTR(SQLULEN), PTR(SQLSMALLINT), PTR(SQLSMALLINT))
    prototype(odbc.SQLBindParameter, SQLHANDLE, SQLUSMALLINT, SQLSMALLINT,
              SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
              SQLLEN, PTR(SQLLEN))
    prototype(odbc.SQLExecute, SQLHANDLE)
    prototype(odbc.SQLSetStmtAttr, SQLHANDLE,
              SQLINTEGER, SQLPOINTER, SQLINTEGER)
    prototype(odbc.SQLMoreResults, SQLHANDLE)
    prototype(odbc.SQLDisconnect, SQLHANDLE)
    prototype(odbc.SQLSetConnectAttr, SQLHANDLE,
              SQLINTEGER, SQLPOINTER, SQLINTEGER)
    prototype(odbc.SQLEndTran, SQLSMALLINT, SQLHANDLE, SQLSMALLINT)
    prototype(odbc.SQLRowCount, SQLHANDLE, PTR(SQLLEN))
    prototype(odbc.SQLBindCol, SQLHANDLE, SQLUSMALLINT, SQLSMALLINT,
              SQLPOINTER, SQLLEN, PTR(SQLLEN))
    prototype(odbc.SQLDrivers, SQLHANDLE, SQLUSMALLINT, PTR(SQLCHAR),
              SQLSMALLINT, PTR(SQLSMALLINT), PTR(SQLCHAR), SQLSMALLINT,
              PTR(SQLSMALLINT))


def initOdbcLibrary(odbcLibPath=None):
    """Initialize the ODBC Library."""
    global odbc
    if odbc is None:
        if osType == "Windows":
            odbc = ctypes.windll.odbc32
        else:
            if not odbcLibPath:
                # If MAC OSx
                if osType == "Darwin":
                    odbcLibPath = "libiodbc.dylib"
                elif osType.startswith("CYGWIN"):
                    odbcLibPath = "odbc32.dll"
                else:
                    odbcLibPath = 'libodbc.so'
            logger.info("Loading ODBC Library: %s", odbcLibPath)
            odbc = ctypes.cdll.LoadLibrary(odbcLibPath)


def initDriverList():
    global drivers
    if drivers is None:
        drivers = []
        description = ctypes.create_string_buffer(SMALL_BUFFER_SIZE)
        descriptionLength = SQLSMALLINT()
        attributesLength = SQLSMALLINT()
        rc = SQL_SUCCESS
        direction = SQL_FETCH_FIRST
        while True:
            rc = odbc.SQLDrivers(hEnv, direction, description,
                                 len(description), ADDR(descriptionLength),
                                 None, 0, attributesLength)
            checkStatus(rc, hEnv=hEnv)
            if rc == SQL_NO_DATA:
                break
            drivers.append(description.value.decode("utf-8"))
            direction = SQL_FETCH_NEXT
        logger.info("Available drivers: {}".format(", ".join(drivers)))


def initOdbcEnv():
    """Initialize ODBC environment handle."""
    global hEnv
    if hEnv is None:
        hEnv = SQLPOINTER()
        rc = odbc.SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, ADDR(hEnv))
        checkStatus(rc, hEnv=hEnv)
        atexit.register(cleanupOdbcEnv)
        atexit.register(cleanupConnections)
        # Set the ODBC environment's compatibility level to ODBC 3.0
        rc = odbc.SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, 0)
        checkStatus(rc, hEnv=hEnv)


def cleanupOdbcEnv():
    """Cleanup ODBC environment handle."""
    if hEnv:
        odbc.SQLFreeHandle(SQL_HANDLE_ENV, hEnv)


def init(odbcLibPath=None):
    try:
        lock.acquire()
        initOdbcLibrary(odbcLibPath)
        initFunctionPrototypes()
        initOdbcEnv()
        initDriverList()
    finally:
        lock.release()


def determineDriver(dbType, driver):
    retval = driver
    if driver is not None:
        if driver not in drivers:
            raise InterfaceError(
                "DRIVER_NOT_FOUND",
                "No driver found with name '{}'. "
                " Available drivers: {}".format(driver, ",".join(drivers)))
    else:
        matches = []
        for driver in drivers:
            if dbType in driver:
                matches.append(driver)
        if not matches:
            raise InterfaceError(
                "DRIVER_NOT_FOUND",
                "No driver found for '{}'.  "
                "Available drivers: {}".format(dbType, ",".join(drivers)))
        else:
            retval = matches[len(matches) - 1]
            if len(matches) > 1:
                logger.warning(
                    "More than one driver found "
                    "for '{}'.  Using '{}'."
                    "  Specify the 'driver' option to "
                    "select a specific driver.".format(dbType, retval))
    return retval


class OdbcConnection:

    """Represents a Connection to Teradata using ODBC."""

    def __init__(self, dbType="Teradata", system=None,
                 username=None, password=None, autoCommit=False,
                 transactionMode=None, queryBands=None, odbcLibPath=None,
                 dataTypeConverter=datatypes.DefaultDataTypeConverter(),
                 driver=None, **kwargs):
        """Creates an ODBC connection."""
        self.hDbc = SQLPOINTER()
        self.cursorCount = 0
        self.sessionno = 0
        self.cursors = []
        self.dbType = dbType
        self.converter = dataTypeConverter

        # Initialize connection handle
        init(odbcLibPath)

        # Build connect string
        extraParams = set(k.lower() for k in kwargs)
        connectParams = collections.OrderedDict()
        if "dsn" not in extraParams:
            connectParams["DRIVER"] = determineDriver(dbType, driver)
        if system:
            connectParams["DBCNAME"] = system
        if username:
            connectParams["UID"] = username
        if password:
            connectParams["PWD"] = password
        if transactionMode:
            connectParams["SESSIONMODE"] = "Teradata" \
                if transactionMode == "TERA" else transactionMode
        connectParams.update(kwargs)
        connectString = u";".join(u"{}={}".format(key, value)
                                  for key, value in connectParams.items())

        rc = odbc.SQLAllocHandle(SQL_HANDLE_DBC, hEnv, ADDR(self.hDbc))
        checkStatus(rc, hEnv=hEnv, method="SQLAllocHandle")

        # Create connection
        logger.debug("Creating connection using ODBC ConnectString: %s",
                     re.sub("PWD=.*?(;|$)", "PWD=XXX;", connectString))
        try:
            lock.acquire()
            rc = odbc.SQLDriverConnectW(self.hDbc, 0, _inputStr(connectString),
                                        SQL_NTS, None, 0, None, 0)
        finally:
            lock.release()
        try:
            checkStatus(rc, hDbc=self.hDbc, method="SQLDriverConnectW")
        except:
            rc = odbc.SQLFreeHandle(SQL_HANDLE_DBC, self.hDbc)
            self.hDbc = None
            raise
        connections.append(self)

        # Setup autocommit, query bands, etc.
        try:
            logger.debug("Setting AUTOCOMMIT to %s",
                         "True" if util.booleanValue(autoCommit) else "False")
            rc = odbc.SQLSetConnectAttr(
                self.hDbc, SQL_ATTR_AUTOCOMMIT,
                TRUE if util.booleanValue(autoCommit) else FALSE, 0)
            checkStatus(
                rc, hDbc=self.hDbc,
                method="SQLSetConnectAttr - SQL_ATTR_AUTOCOMMIT")
            if dbType == "Teradata":
                with self.cursor() as c:
                    self.sessionno = c.execute(
                        "SELECT SESSION",
                        queryTimeout=QUERY_TIMEOUT).fetchone()[0]
                    logger.debug("SELECT SESSION returned %s", self.sessionno)
                    if queryBands:
                        c.execute(u"SET QUERY_BAND = '{};' FOR SESSION".format(
                            u";".join(u"{}={}".format(util.toUnicode(k),
                                                      util.toUnicode(v))
                                      for k, v in queryBands.items())),
                                  queryTimeout=QUERY_TIMEOUT)
                self.commit()
                logger.debug("Created session %s.", self.sessionno)
        except Exception:
            self.close()
            raise

    def close(self):
        """CLoses an ODBC Connection."""
        if self.hDbc:
            if self.sessionno:
                logger.debug("Closing session %s...", self.sessionno)
            for cursor in list(self.cursors):
                cursor.close()
            rc = odbc.SQLDisconnect(self.hDbc)
            sqlState = checkStatus(
                rc, hDbc=self.hDbc, method="SQLDisconnect",
                ignore=[SQL_STATE_CONNECTION_NOT_OPEN,
                        SQL_STATE_INVALID_TRANSACTION_STATE])
            if SQL_STATE_INVALID_TRANSACTION_STATE in sqlState:
                logger.warning("Rolling back open transaction for session %s "
                               "so it can be closed.", self.sessionno)
                rc = odbc.SQLEndTran(SQL_HANDLE_DBC, self.hDbc, SQL_ROLLBACK)
                checkStatus(
                    rc, hDbc=self.hDbc,
                    method="SQLEndTran - SQL_ROLLBACK - Disconnect")
                rc = odbc.SQLDisconnect(self.hDbc)
                checkStatus(rc, hDbc=self.hDbc, method="SQLDisconnect")
            rc = odbc.SQLFreeHandle(SQL_HANDLE_DBC, self.hDbc)
            if rc != SQL_INVALID_HANDLE:
                checkStatus(rc, hDbc=self.hDbc, method="SQLFreeHandle")
            connections.remove(self)
            self.hDbc = None
            if self.sessionno:
                logger.debug("Session %s closed.", self.sessionno)

    def commit(self):
        """Commits a transaction."""
        logger.debug("Committing transaction...")
        rc = odbc.SQLEndTran(SQL_HANDLE_DBC, self.hDbc, SQL_COMMIT)
        checkStatus(rc, hDbc=self.hDbc, method="SQLEndTran - SQL_COMMIT")

    def rollback(self):
        """Rollsback a transaction."""
        logger.debug("Rolling back transaction...")
        rc = odbc.SQLEndTran(SQL_HANDLE_DBC, self.hDbc, SQL_ROLLBACK)
        checkStatus(rc, hDbc=self.hDbc, method="SQLEndTran - SQL_ROLLBACK")

    def cursor(self):
        """Returns a cursor."""
        cursor = OdbcCursor(
            self, self.dbType, self.converter, self.cursorCount)
        self.cursorCount += 1
        return cursor

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()

    def __repr__(self):
        return "OdbcConnection(sessionno={})".format(self.sessionno)

connect = OdbcConnection


class OdbcCursor (util.Cursor):

    """Represents an ODBC Cursor."""

    def __init__(self, connection, dbType, converter, num):
        util.Cursor.__init__(self, connection, dbType, converter)
        self.num = num
        self.moreResults = None
        if num > 0:
            logger.debug(
                "Creating cursor %s for session %s.", self.num,
                self.connection.sessionno)
        self.hStmt = SQLPOINTER()
        rc = odbc.SQLAllocHandle(
            SQL_HANDLE_STMT, connection.hDbc, ADDR(self.hStmt))
        checkStatus(rc, hStmt=self.hStmt)
        connection.cursors.append(self)

    def callproc(self, procname, params, queryTimeout=0):
        self._checkClosed()
        query = "CALL {} (".format(procname)
        for i in range(0, len(params)):
            if i > 0:
                query += ", "
            query += "?"
        query += ")"
        logger.debug("Executing Procedure: %s", query)
        self.execute(query, params, queryTimeout=queryTimeout)
        return util.OutParams(params, self.dbType, self.converter)

    def close(self):
        if self.hStmt:
            if self.num > 0:
                logger.debug(
                    "Closing cursor %s for session %s.", self.num,
                    self.connection.sessionno)
            rc = odbc.SQLFreeHandle(SQL_HANDLE_STMT, self.hStmt)
            checkStatus(rc, hStmt=self.hStmt)
            self.connection.cursors.remove(self)
            self.hStmt = None

    def _setQueryTimeout(self, queryTimeout):
        rc = odbc.SQLSetStmtAttr(
            self.hStmt, SQL_ATTR_QUERY_TIMEOUT, SQLPOINTER(queryTimeout),
            SQL_IS_UINTEGER)
        checkStatus(
            rc, hStmt=self.hStmt,
            method="SQLSetStmtStmtAttr - SQL_ATTR_QUERY_TIMEOUT")

    def execute(self, query, params=None, queryTimeout=0):
        self._checkClosed()
        if params:
            self.executemany(query, [params, ], queryTimeout)
        else:
            if self.connection.sessionno:
                logger.debug(
                    "Executing query on session %s using SQLExecDirectW: %s",
                    self.connection.sessionno, query)
            self._free()
            self._setQueryTimeout(queryTimeout)
            rc = odbc.SQLExecDirectW(
                self.hStmt, _inputStr(_convertLineFeeds(query)), SQL_NTS)
            checkStatus(rc, hStmt=self.hStmt, method="SQLExecDirectW")
        self._handleResults()
        return self

    def executemany(self, query, params, batch=False, queryTimeout=0):
        self._checkClosed()
        self._free()
        # Prepare the query
        rc = odbc.SQLPrepareW(
            self.hStmt, _inputStr(_convertLineFeeds(query)), SQL_NTS)
        checkStatus(rc, hStmt=self.hStmt, method="SQLPrepare")
        self._setQueryTimeout(queryTimeout)
        # Get the number of parameters in the SQL statement.
        numParams = SQLSMALLINT()
        rc = odbc.SQLNumParams(self.hStmt, ADDR(numParams))
        checkStatus(rc, hStmt=self.hStmt, method="SQLNumParams")
        numParams = numParams.value
        # The argument types.
        dataTypes = []
        for paramNum in range(0, numParams):
            dataType = SQLSMALLINT()
            parameterSize = SQLULEN()
            decimalDigits = SQLSMALLINT()
            nullable = SQLSMALLINT()
            rc = odbc.SQLDescribeParam(
                self.hStmt, paramNum + 1, ADDR(dataType), ADDR(parameterSize),
                ADDR(decimalDigits), ADDR(nullable))
            checkStatus(rc, hStmt=self.hStmt, method="SQLDescribeParams")
            dataTypes.append(dataType.value)
        if batch:
            logger.debug(
                "Executing query on session %s using batched SQLExecute: %s",
                self.connection.sessionno, query)
            self._executeManyBatch(params, numParams, dataTypes)
        else:
            logger.debug(
                "Executing query on session %s using SQLExecute: %s",
                self.connection.sessionno, query)
            rc = odbc.SQLSetStmtAttr(self.hStmt, SQL_ATTR_PARAMSET_SIZE, 1, 0)
            checkStatus(rc, hStmt=self.hStmt, method="SQLSetStmtAttr")
            paramSetNum = 0
            for p in params:
                paramSetNum += 1
                logger.trace("ParamSet %s: %s", paramSetNum, p)
                if len(p) != numParams:
                    raise InterfaceError(
                        "PARAMS_MISMATCH", "The number of supplied parameters "
                        "({}) does not match the expected number of "
                        "parameters ({}).".format(len(p), numParams))
                paramArray = []
                lengthArray = []
                for paramNum in range(0, numParams):
                    val = p[paramNum]
                    inputOutputType = _getInputOutputType(val)
                    valueType, paramType = _getParamValueType(
                        dataTypes[paramNum])
                    param, length, null = _getParamValue(val, valueType, False)
                    paramArray.append(param)
                    if param is not None:
                        if valueType == SQL_C_BINARY:
                            bufSize = SQLLEN(length)
                            lengthArray.append(SQLLEN(length))
                            columnSize = SQLULEN(length)
                        elif valueType == SQL_C_DOUBLE:
                            bufSize = SQLLEN(length)
                            lengthArray.append(SQLLEN(length))
                            columnSize = SQLULEN(length)
                            param = ADDR(param)
                        else:
                            bufSize = SQLLEN(ctypes.sizeof(param))
                            lengthArray.append(SQLLEN(SQL_NTS))
                            columnSize = SQLULEN(length)
                        if null:
                            # Handle INOUT parameter with NULL input value.
                            lengthArray.pop(-1)
                            lengthArray.append(SQLLEN(SQL_NULL_DATA))
                    else:
                        bufSize = SQLLEN(0)
                        columnSize = SQLULEN(0)
                        lengthArray.append(SQLLEN(SQL_NULL_DATA))
                    logger.trace("Binding parameter %s...", paramNum + 1)
                    rc = odbc.SQLBindParameter(
                        self.hStmt, paramNum + 1, inputOutputType, valueType,
                        paramType, columnSize, 0, param, bufSize,
                        ADDR(lengthArray[paramNum]))
                    checkStatus(
                        rc, hStmt=self.hStmt, method="SQLBindParameter")
                logger.debug("Executing prepared statement.")
                rc = odbc.SQLExecute(self.hStmt)
                for paramNum in range(0, numParams):
                    val = p[paramNum]
                    if isinstance(val, OutParam):
                        val.size = lengthArray[paramNum].value
                checkStatus(rc, hStmt=self.hStmt, method="SQLExecute")
        self._handleResults()
        return self

    def _executeManyBatch(self, params, numParams, dataTypes):
        # Get the number of parameter sets.
        paramSetSize = len(params)
        # Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
        # column-wise binding.
        rc = odbc.SQLSetStmtAttr(
            self.hStmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0)
        checkStatus(rc, hStmt=self.hStmt, method="SQLSetStmtAttr")
        # Specify the number of elements in each parameter array.
        rc = odbc.SQLSetStmtAttr(
            self.hStmt, SQL_ATTR_PARAMSET_SIZE, paramSetSize, 0)
        checkStatus(rc, hStmt=self.hStmt, method="SQLSetStmtAttr")
        # Specify a PTR to get the number of parameters processed.
        # paramsProcessed = SQLULEN()
        # rc = odbc.SQLSetStmtAttr(self.hStmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
        #                          ADDR(paramsProcessed), SQL_IS_POINTER)
        # checkStatus(rc, hStmt=self.hStmt, method="SQLSetStmtAttr")
        # Specify a PTR to get the status of the parameters processed.
        # paramsStatus = (SQLUSMALLINT * paramSetSize)()
        # rc = odbc.SQLSetStmtAttr(self.hStmt, SQL_ATTR_PARAM_STATUS_PTR,
        #                          ADDR(paramsStatus), SQL_IS_POINTER)
        # checkStatus(rc, hStmt=self.hStmt, method="SQLSetStmtAttr")
        # Bind the parameters.
        paramArrays = []
        lengthArrays = []
        paramSetSize = len(params)
        paramSetNum = 0
        debugEnabled = logger.isEnabledFor(logging.DEBUG)
        for p in params:
            paramSetNum += 1
            if debugEnabled:
                logger.debug("ParamSet %s: %s", paramSetNum, p)
            if len(p) != numParams:
                raise InterfaceError(
                    "PARAMS_MISMATCH", "The number of supplied parameters "
                    "({}) does not match the expected number of parameters "
                    "({}).".format(len(p), numParams))
        for paramNum in range(0, numParams):
            p = []
            valueType, paramType = _getParamValueType(dataTypes[paramNum])
            maxLen = 0
            for paramSetNum in range(0, paramSetSize):
                param, length, null = _getParamValue(  # @UnusedVariable
                    params[paramSetNum][paramNum], valueType, True)
                if length > maxLen:
                    maxLen = length
                p.append(param)
            if debugEnabled:
                logger.debug("Max length for parameter %s is %s.",
                             paramNum + 1, maxLen)
            if valueType == SQL_C_BINARY:
                valueSize = SQLLEN(maxLen)
                paramArrays.append((SQLBYTE * (paramSetSize * maxLen))())
            elif valueType == SQL_C_DOUBLE:
                valueSize = SQLLEN(maxLen)
                paramArrays.append((SQLDOUBLE * paramSetSize)())
            else:
                maxLen += 1
                valueSize = SQLLEN(ctypes.sizeof(SQLWCHAR) * maxLen)
                paramArrays.append(_createBuffer(paramSetSize * maxLen))
            lengthArrays.append((SQLLEN * paramSetSize)())
            for paramSetNum in range(0, paramSetSize):
                index = paramSetNum * maxLen
                if p[paramSetNum] is not None:
                    if valueType == SQL_C_DOUBLE:
                        paramArrays[paramNum][paramSetNum] = p[paramSetNum]
                    else:
                        for c in p[paramSetNum]:
                            paramArrays[paramNum][index] = c
                            index += 1
                        if valueType == SQL_C_BINARY:
                            lengthArrays[paramNum][
                                paramSetNum] = len(p[paramSetNum])
                        else:
                            lengthArrays[paramNum][
                                paramSetNum] = SQLLEN(SQL_NTS)
                            paramArrays[paramNum][
                                index] = _convertParam("\x00")[0]
                else:
                    lengthArrays[paramNum][paramSetNum] = SQLLEN(SQL_NULL_DATA)
                    if valueType == SQL_C_WCHAR:
                        paramArrays[paramNum][index] = _convertParam("\x00")[0]
            if debugEnabled:
                logger.debug("Binding parameter %s...", paramNum + 1)
            rc = odbc.SQLBindParameter(self.hStmt, paramNum + 1,
                                       SQL_PARAM_INPUT, valueType, paramType,
                                       SQLULEN(maxLen), 0,
                                       paramArrays[paramNum], valueSize,
                                       lengthArrays[paramNum])
            checkStatus(rc, hStmt=self.hStmt, method="SQLBindParameter")
        # Execute the SQL statement.
        if debugEnabled:
            logger.debug("Executing prepared statement.")
        rc = odbc.SQLExecute(self.hStmt)
        checkStatus(rc, hStmt=self.hStmt, method="SQLExecute")

    def _handleResults(self):
        # Rest cursor attributes.
        self.description = None
        self.rowcount = -1
        self.rownumber = None
        self.columns = {}
        self.types = []
        self.moreResults = None
        # Get column count in result set.
        columnCount = SQLSMALLINT()
        rc = odbc.SQLNumResultCols(self.hStmt, ADDR(columnCount))
        checkStatus(rc, hStmt=self.hStmt, method="SQLNumResultCols")
        rowCount = SQLLEN()
        rc = odbc.SQLRowCount(self.hStmt, ADDR(rowCount))
        checkStatus(rc, hStmt=self.hStmt, method="SQLRowCount")
        self.rowcount = rowCount.value
        # Get column meta data and create row iterator.
        if columnCount.value > 0:
            self.description = []
            nameBuf = _createBuffer(SMALL_BUFFER_SIZE)
            nameLength = SQLSMALLINT()
            dataType = SQLSMALLINT()
            columnSize = SQLULEN()
            decimalDigits = SQLSMALLINT()
            nullable = SQLSMALLINT()
            for col in range(0, columnCount.value):
                rc = odbc.SQLDescribeColW(
                    self.hStmt, col + 1, nameBuf, len(nameBuf),
                    ADDR(nameLength), ADDR(dataType), ADDR(columnSize),
                    ADDR(decimalDigits), ADDR(nullable))
                checkStatus(rc, hStmt=self.hStmt, method="SQLDescribeColW")
                columnName = _outputStr(nameBuf)
                odbc.SQLColAttributeW(
                    self.hStmt, col + 1, SQL_DESC_TYPE_NAME, ADDR(nameBuf),
                    len(nameBuf), None, None)
                checkStatus(rc, hStmt=self.hStmt, method="SQLColAttributeW")
                typeName = _outputStr(nameBuf)
                typeCode = self.converter.convertType(self.dbType, typeName)
                self.columns[columnName.lower()] = col
                self.types.append((typeName, typeCode, dataType.value))
                self.description.append((
                    columnName, typeCode, None, columnSize.value,
                    decimalDigits.value, None, nullable.value))
        self.iterator = rowIterator(self)

    def nextset(self):
        self._checkClosed()
        if self.moreResults is None:
            self._checkForMoreResults()
        if self.moreResults:
            self._handleResults()
            return True

    def _checkForMoreResults(self):
        rc = odbc.SQLMoreResults(self.hStmt)
        checkStatus(rc, hStmt=self.hStmt, method="SQLMoreResults")
        self.moreResults = rc == SQL_SUCCESS or rc == SQL_SUCCESS_WITH_INFO
        return self.moreResults

    def _free(self):
        rc = odbc.SQLFreeStmt(self.hStmt, SQL_CLOSE)
        checkStatus(rc, hStmt=self.hStmt, method="SQLFreeStmt - SQL_CLOSE")
        rc = odbc.SQLFreeStmt(self.hStmt, SQL_RESET_PARAMS)
        checkStatus(
            rc, hStmt=self.hStmt, method="SQLFreeStmt - SQL_RESET_PARAMS")

    def _checkClosed(self):
        if not self.hStmt:
            raise InterfaceError("CURSOR_CLOSED",
                                 "Operations cannot be performed on a "
                                 "closed cursor.")


def _convertLineFeeds(query):
    return "\r".join(util.linesplit(query))


def _getInputOutputType(val):
    inputOutputType = SQL_PARAM_INPUT
    if isinstance(val, InOutParam):
        inputOutputType = SQL_PARAM_INPUT_OUTPUT
    elif isinstance(val, OutParam):
        inputOutputType = SQL_PARAM_OUTPUT
    return inputOutputType


def _getParamValueType(dataType):
    valueType = SQL_C_WCHAR
    paramType = SQL_WVARCHAR
    if dataType in (SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY):
        valueType = SQL_C_BINARY
        paramType = dataType
    elif dataType == SQL_WLONGVARCHAR:
        paramType = SQL_WLONGVARCHAR
    elif dataType in (SQL_FLOAT, SQL_DOUBLE, SQL_REAL):
        valueType = SQL_C_DOUBLE
        paramType = SQL_DOUBLE
    return valueType, paramType


def _getParamBufferSize(val):
    return SMALL_BUFFER_SIZE if val.size is None else val.size


def _getParamValue(val, valueType, batch):
    length = 0
    null = False
    if val is None:
        param = None
    elif valueType == SQL_C_BINARY:
        ba = val
        if isinstance(val, InOutParam):
            ba = val.inValue
            if val.inValue is None:
                null = True
                ba = bytearray(_getParamBufferSize(val))
        elif isinstance(val, OutParam):
            ba = bytearray(_getParamBufferSize(val))
        if ba is not None and not isinstance(ba, bytearray):
            raise InterfaceError("Expected bytearray for BINARY parameter.")
        length = len(ba)
        if batch:
            param = ba
        else:
            byteArr = SQLBYTE * length
            param = byteArr.from_buffer(ba)
            if isinstance(val, OutParam):
                val.setValueFunc(lambda: ba[:val.size])
    elif valueType == SQL_C_DOUBLE:
        f = val
        if isinstance(val, InOutParam):
            f = val.inValue
            if f is None:
                null = True
                f = float(0)
        elif isinstance(val, OutParam):
            f = float(0)
        param = SQLDOUBLE(f if not util.isString(f) else float(f))
        length = ctypes.sizeof(param)
        if isinstance(val, OutParam):
            val.setValueFunc(lambda: param.value)
    else:
        if batch:
            param = _convertParam(val)
            length = len(param)
        elif isinstance(val, InOutParam):
            length = _getParamBufferSize(val)
            if val.inValue is not None:
                param = _inputStr(val.inValue, length)
            else:
                param = _createBuffer(length)
                null = True
            val.setValueFunc(lambda: _outputStr(param))
        elif isinstance(val, OutParam):
            length = _getParamBufferSize(val)
            param = _createBuffer(length)
            val.setValueFunc(lambda: _outputStr(param))
        else:
            param = _inputStr(val)
            length = len(param)
    return param, length, null


def _getFetchSize(cursor):
    """Gets the fetch size associated with the cursor."""
    fetchSize = cursor.fetchSize
    for dataType in cursor.types:
        if dataType[2] in (SQL_LONGVARBINARY, SQL_WLONGVARCHAR):
            fetchSize = 1
            break
    return fetchSize


def _getBufSize(cursor, colIndex):
    bufSize = cursor.description[colIndex - 1][3] + 1
    dataType = cursor.types[colIndex - 1][0]
    if dataType in datatypes.BINARY_TYPES:
        pass
    elif dataType in datatypes.FLOAT_TYPES:
        bufSize = ctypes.sizeof(ctypes.c_double)
    elif dataType in datatypes.INT_TYPES:
        bufSize = 30
    elif cursor.types[colIndex - 1][2] in (SQL_WCHAR, SQL_WVARCHAR,
                                           SQL_WLONGVARCHAR):
        pass
    elif dataType.startswith("DATE"):
        bufSize = 20
    elif dataType.startswith("TIMESTAMP"):
        bufSize = 40
    elif dataType.startswith("TIME"):
        bufSize = 30
    elif dataType.startswith("INTERVAL"):
        bufSize = 80
    elif dataType.startswith("PERIOD"):
        bufSize = 80
    elif dataType.startswith("DECIMAL"):
        bufSize = 42
    else:
        bufSize = 2 ** 16 + 1
    return bufSize


def _setupColumnBuffers(cursor, buffers, bufSizes, dataTypes, indicators,
                        lastFetchSize):
    """Sets up the column buffers for retrieving multiple rows of a result set
    at a time"""
    fetchSize = _getFetchSize(cursor)
    # If the fetchSize hasn't changed since the last time setupBuffers
    # was called, then we can reuse the previous buffers.
    if fetchSize != lastFetchSize:
        logger.debug("FETCH_SIZE: %s" % fetchSize)
        rc = odbc.SQLSetStmtAttr(
            cursor.hStmt, SQL_ATTR_ROW_ARRAY_SIZE, fetchSize, 0)
        checkStatus(rc, hStmt=cursor.hStmt,
                    method="SQLSetStmtAttr - SQL_ATTR_ROW_ARRAY_SIZE")
        for col in range(1, len(cursor.description) + 1):
            dataType = SQL_C_WCHAR
            buffer = None
            bufSize = _getBufSize(cursor, col)
            lob = False
            if cursor.types[col - 1][2] == SQL_LONGVARBINARY:
                lob = True
                bufSize = LARGE_BUFFER_SIZE
                buffer = (ctypes.c_byte * bufSize)()
                dataType = SQL_LONGVARBINARY
            elif cursor.types[col - 1][2] == SQL_WLONGVARCHAR:
                lob = True
                buffer = _createBuffer(LARGE_BUFFER_SIZE)
                bufSize = ctypes.sizeof(buffer)
                dataType = SQL_WLONGVARCHAR
            elif cursor.description[col - 1][1] == BINARY:
                dataType = SQL_C_BINARY
                buffer = (ctypes.c_byte * bufSize * fetchSize)()
            elif cursor.types[col - 1][0] in datatypes.FLOAT_TYPES:
                dataType = SQL_C_DOUBLE
                buffer = (ctypes.c_double * fetchSize)()
            else:
                buffer = _createBuffer(bufSize * fetchSize)
                bufSize = int(ctypes.sizeof(buffer) / fetchSize)
            dataTypes.append(dataType)
            buffers.append(buffer)
            bufSizes.append(bufSize)
            logger.debug("Buffer size for column %s: %s", col, bufSize)
            indicators.append((SQLLEN * fetchSize)())
            if not lob:
                rc = odbc.SQLBindCol(cursor.hStmt, col, dataType, buffer,
                                     bufSize, indicators[col - 1])
                checkStatus(rc, hStmt=cursor.hStmt, method="SQLBindCol")
    return fetchSize


def _getLobData(cursor, colIndex, buf):
    """ Get LOB Data """
    length = SQLLEN()
    dataType = SQL_C_WCHAR
    bufSize = ctypes.sizeof(buf)
    if cursor.description[colIndex - 1][1] == BINARY:
        dataType = SQL_C_BINARY
    rc = odbc.SQLGetData(
        cursor.hStmt, colIndex, dataType, buf, bufSize, ADDR(length))
    sqlState = checkStatus(rc, hStmt=cursor.hStmt, method="SQLGetData")
    val = None
    if length.value != SQL_NULL_DATA:
        if SQL_STATE_DATA_TRUNCATED in sqlState:
            logger.debug(
                "Data truncated. Calling SQLGetData to get next part "
                "of data for column %s of size %s.",
                colIndex, length.value)
            if dataType == SQL_C_BINARY:
                val = bytearray(length.value)
                val[0:bufSize] = buf
                newBufSize = len(val) - bufSize
                newBuffer = (ctypes.c_byte * newBufSize).from_buffer(
                    val, bufSize)
                rc = odbc.SQLGetData(
                    cursor.hStmt, colIndex, dataType, newBuffer,
                    newBufSize, ADDR(length))
                checkStatus(
                    rc, hStmt=cursor.hStmt, method="SQLGetData2")
            else:
                val = [_outputStr(buf), ]
                while SQL_STATE_DATA_TRUNCATED in sqlState:
                    rc = odbc.SQLGetData(
                        cursor.hStmt, colIndex, dataType, buf, bufSize,
                        ADDR(length))
                    sqlState = checkStatus(
                        rc, hStmt=cursor.hStmt, method="SQLGetData2")
                    val.append(_outputStr(buf))
                val = "".join(val)
        else:
            if dataType == SQL_C_BINARY:
                val = bytearray(
                    (ctypes.c_byte * length.value).from_buffer(buf))
            else:
                val = _outputStr(buf)
    return val


def _getRow(cursor, buffers, bufSizes, dataTypes, indicators, rowIndex):
    """Reads a row of data from the fetched input buffers.  If the column
       type is a BLOB or CLOB, then that data is obtained via calls to
       SQLGetData."""
    row = []
    for col in range(1, len(cursor.description) + 1):
        val = None
        buf = buffers[col - 1]
        bufSize = bufSizes[col - 1]
        dataType = dataTypes[col - 1]
        length = indicators[col - 1][rowIndex]
        if length != SQL_NULL_DATA:
            if dataType == SQL_C_BINARY:
                val = bytearray((ctypes.c_byte * length).from_buffer(
                    buf, bufSize * rowIndex))
            elif dataType == SQL_C_DOUBLE:
                val = ctypes.c_double.from_buffer(buf,
                                                  bufSize * rowIndex).value
            elif dataType == SQL_WLONGVARCHAR:
                val = _getLobData(cursor, col, buf)
            elif dataType == SQL_LONGVARBINARY:
                val = _getLobData(cursor, col, buf)
            else:
                chLen = (int)(bufSize / ctypes.sizeof(SQLWCHAR))
                chBuf = (SQLWCHAR * chLen)
                val = _outputStr(chBuf.from_buffer(buf,
                                                   bufSize * rowIndex))
        row.append(val)
    return row


def rowIterator(cursor):
    buffers = []
    bufSizes = []
    dataTypes = []
    indicators = []
    rowCount = SQLULEN()
    lastFetchSize = None
    rc = odbc.SQLSetStmtAttr(
        cursor.hStmt, SQL_ATTR_ROWS_FETCHED_PTR, ADDR(rowCount), 0)
    checkStatus(rc, hStmt=cursor.hStmt,
                method="SQLSetStmtAttr - SQL_ATTR_ROWS_FETCHED_PTR")
    while cursor.description is not None:
        lastFetchSize = _setupColumnBuffers(cursor, buffers, bufSizes,
                                            dataTypes, indicators,
                                            lastFetchSize)
        rc = odbc.SQLFetch(cursor.hStmt)
        checkStatus(rc, hStmt=cursor.hStmt, method="SQLFetch")
        if rc == SQL_NO_DATA:
            break
        for rowIndex in range(0, rowCount.value):
            yield _getRow(cursor, buffers, bufSizes, dataTypes,
                          indicators, rowIndex)
    if not cursor._checkForMoreResults():
        cursor._free()
