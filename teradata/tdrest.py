"""An implementation of the Python Database API Specification v2.0 using
Teradata REST."""

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
import json
import base64
import io
import logging
import atexit
import time
import ssl

from . import pulljson, util, datatypes
from .api import *  # @UnusedWildImport # noqa

if sys.version_info[0] == 2:
    import httplib as httplib  # @UnresolvedImport #@UnusedImport
else:
    import http.client as httplib  # @UnresolvedImport @UnusedImport @Reimport
    unicode = str

logger = logging.getLogger(__name__)

REST_ERROR = "REST_ERROR"
HTTP_STATUS_DATABASE_ERROR = 420
ERROR_USER_GENERATED_TRANSACTION_ABORT = 3514
MAX_CONNECT_RETRIES = 5

connections = []


def cleanup():
    for conn in connections:
        conn.close()
atexit.register(cleanup)


class RestConnection:

    """ Represents a Connection to Teradata using the REST API for
     Teradata Database """

    def __init__(self, dbType="Teradata", host=None, system=None,
                 username=None, password=None, protocol='http', port=None,
                 webContext='/tdrest', autoCommit=False, implicit=False,
                 transactionMode='TERA', queryBands=None, charset=None,
                 verifyCerts=True, sslContext=None,
                 dataTypeConverter=datatypes.DefaultDataTypeConverter()):
        self.dbType = dbType
        self.system = system
        self.sessionId = None
        self.implicit = implicit
        self.transactionMode = transactionMode
        self.dataTypeConverter = dataTypeConverter
        # Support TERA and Teradata as transaction mode to be consistent with
        # ODBC.
        if transactionMode == "Teradata":
            self.transactionMode = "TERA"
        self.autoCommit = False
        if port is None:
            if protocol == 'http':
                port = 1080
            elif protocol == 'https':
                port = 1443
            else:
                raise InterfaceError(
                    CONFIG_ERROR, "Unsupported protocol: {}".format(protocol))
        self.template = RestTemplate(
            protocol, host, port, webContext, username, password,
            accept='application/vnd.com.teradata.rest-v2.0+json',
            verifyCerts=util.booleanValue(verifyCerts), sslContext=sslContext)
        with self.template.connect() as conn:
            if not self.implicit:
                options = {}
                options['autoCommit'] = autoCommit
                options['transactionMode'] = transactionMode
                if queryBands:
                    options['queryBands'] = queryBands
                if charset:
                    options['charSet'] = charset
                try:
                    session = conn.post(
                        '/systems/{0}/sessions'.format(self.system),
                        options).readObject()
                    self.sessionId = session['sessionId']
                    connections.append(self)
                    logger.info("Created explicit session: %s",  session)
                except (pulljson.JSONParseError) as e:
                    raise InterfaceError(
                        e.code, "Error reading JSON response: " + e.msg)

    def close(self):
        """ Closes an Explicit Session using the REST API for Teradata
         Database """
        if hasattr(self, 'sessionId') and self.sessionId is not None:
            with self.template.connect() as conn:
                try:
                    conn.delete(
                        '/systems/{0}/sessions/{1}'.format(
                            self.system, self.sessionId))
                except InterfaceError as e:
                    # Ignore if the session is already closed.
                    if e.code != 404:
                        raise e
            logger.info("Closing session: %s", self.sessionId)
            self.sessionId = None
            connections.remove(self)

    def commit(self):
        with self.cursor() as cursor:
            if self.transactionMode == 'ANSI':
                cursor.execute("COMMIT")
            else:
                cursor.execute("ET")

    def rollback(self):
        with self.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK")
            except DatabaseError as e:
                if e.code != ERROR_USER_GENERATED_TRANSACTION_ABORT:
                    raise e

    def cursor(self):
        return RestCursor(self)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()

connect = RestConnection


class RestCursor (util.Cursor):

    def __init__(self, connection):
        self.conn = None
        util.Cursor.__init__(
            self, connection, connection.dbType, connection.dataTypeConverter)
        self.conn = connection.template.connect()

    def callproc(self, procname, params, queryTimeout=None):
        inparams = None
        outparams = None
        count = 0
        query = "CALL {} (".format(procname)
        if params is not None:
            inparams = [[]]
            outparams = []
            for p in params:
                if count > 0:
                    query += ", "
                if isinstance(p, InOutParam):
                    inparams[0].append(p.inValue)
                    # outparams.append(p.inValue)
                elif isinstance(p, OutParam):
                    outparams.append(None)
                else:
                    inparams[0].append(p)
                count += 1
                query += "?"
        query += ")"
        outparams = self._handleResults(self._execute(
            query, inparams, outparams, queryTimeout=queryTimeout),
            len(outparams) > 0)
        return util.OutParams(params,  self.dbType, self.converter, outparams)

    def close(self):
        if self.conn:
            self.conn.close()

    def execute(self, query, params=None, queryTimeout=None):
        if params is not None:
            params = [params]
        self._handleResults(
            self._execute(query, params, queryTimeout=queryTimeout))
        return self

    def executemany(self, query, params, batch=False, queryTimeout=None):
        self._handleResults(
            self._execute(query, params, batch=batch,
                          queryTimeout=queryTimeout))
        return self

    def _handleResults(self, results, hasOutParams=False):
        self.results = results
        try:
            results.expectObject()
            self.queueDuration = results.expectField(
                "queueDuration", pulljson.NUMBER)
            self.queryDuration = results.expectField(
                "queryDuration", pulljson.NUMBER)
            logger.debug("Durations reported by REST service: Queue Duration: "
                         "%s, Query Duration: %s", self.queueDuration,
                         self.queryDuration)
            results.expectField("results", pulljson.ARRAY)
            results.expectObject()
            return self._handleResultSet(results, hasOutParams)
        except (pulljson.JSONParseError) as e:
            raise InterfaceError(
                e.code, "Error reading JSON response: " + e.msg)

    def _execute(self, query, params=None, outParams=None, batch=False,
                 queryTimeout=None):
        options = {}
        options['query'] = query
        options['format'] = 'array'
        options['includeColumns'] = 'true'
        options['rowLimit'] = 0
        if params is not None:
            options['params'] = list(
                list(_convertParam(p) for p in paramSet)
                for paramSet in params)
            options['batch'] = batch
        if outParams is not None:
            options['outParams'] = outParams
        if not self.connection.implicit:
            options['session'] = int(self.connection.sessionId)
        if queryTimeout is not None:
            options['queryTimeout'] = queryTimeout
            options['queueTimeout'] = queryTimeout
        return self.conn.post('/systems/{0}/queries'.format(
            self.connection.system), options)

    def _handleResultSet(self, results, hasOutParams=False):
        outParams = None
        if hasOutParams:
            outParams = results.expectField(
                "outParams", pulljson.ARRAY, readAll=True)
            self.resultSet = None
        else:
            try:
                self.resultSet = results.expectField(
                    "resultSet", pulljson.BOOLEAN)
            except pulljson.JSONParseError:
                # Workaround for Batch mode and Stored procedures which doens't
                # include a resultSet.
                self.resultSet = None
        if self.resultSet:
            index = 0
            self.columns = {}
            self.description = []
            self.types = []
            self.rowcount = -1
            self.rownumber = None
            for column in results.expectField("columns", pulljson.ARRAY):
                self.columns[column["name"].lower()] = index
                type_code = self.converter.convertType(
                    self.dbType, column["type"])
                self.types.append((column["type"], type_code))
                self.description.append(
                    (column["name"], type_code, None, None, None, None, None))
                index += 1
            self.iterator = results.expectField("data", pulljson.ARRAY)
        else:
            self.columns = None
            self.description = None
            self.rownumber = None
            self.rowcount = -1
            if self.resultSet is not None:
                self.rowcount = results.expectField("count")
        return outParams

    def nextset(self):
        for row in self:  # @UnusedVariable
            pass
        for event in self.results:
            if event.type == pulljson.START_OBJECT:
                self._handleResultSet(self.results)
                return True


def _convertParam(p):
    if util.isString(p) or p is None:
        return p
    elif isinstance(p, bytearray):
        return ''.join('{:02x}'.format(x) for x in p)
    else:
        return unicode(p)


class RestTemplate:

    def __init__(self, protocol, host, port, webContext, username, password,
                 sslContext=None, verifyCerts=True, accept=None):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.webContext = webContext
        self.headers = {}
        self.headers['Content-Type'] = 'application/json'
        if accept is not None:
            self.headers['Accept'] = accept
        self.headers['Authorization'] = 'Basic ' + \
            base64.b64encode(
                (username + ":" + password).encode('utf_8')).decode('ascii')
        self.sslContext = sslContext
        if sslContext is None and not verifyCerts:
            self.sslContext = ssl.create_default_context()
            self.sslContext.check_hostname = False
            self.sslContext.verify_mode = ssl.CERT_NONE

    def connect(self):
        return HttpConnection(self)


class HttpConnection:

    def __init__(self, template):
        self.template = template
        if template.protocol.lower() == "http":
            self.conn = httplib.HTTPConnection(template.host, template.port)
        elif template.protocol.lower() == "https":
            self.conn = httplib.HTTPSConnection(
                template.host, template.port, context=template.sslContext)
        else:
            raise InterfaceError(
                REST_ERROR, "Unknown protocol: %s" % template.protocol)
        failureCount = 0
        while True:
            try:
                self.conn.connect()
                break
            except Exception as e:
                eofError = "EOF occurred in violation of protocol" in str(e)
                failureCount += 1
                if not eofError or failureCount > MAX_CONNECT_RETRIES:
                    raise InterfaceError(
                        REST_ERROR,
                        "Error accessing {}:{}. ERROR:  {}".format(
                            template.host, template.port, e))
                else:
                    logger.debug(
                        "Received an \"EOF occurred in violation of "
                        "protocol\" error, retrying connection.")

    def close(self):
        if self.conn:
            self.conn.close()

    def post(self, uri, data={}):
        return self.send(uri, 'POST', data)

    def delete(self, uri):
        self.send(uri, 'DELETE', None)

    def get(self, uri):
        return self.send(uri, 'GET', None)

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()

    def send(self, uri, method, data):
        response = None
        url = self.template.webContext + uri
        try:
            start = time.time()
            payload = json.dumps(data).encode('utf8') if data else None
            logger.trace("%s: %s, %s", method, url, payload)
            self.conn.request(method, url, payload, self.template.headers)
            response = self.conn.getresponse()
            duration = time.time() - start
            logger.debug("Roundtrip Duration: %.3f seconds", duration)
        except Exception as e:
            raise InterfaceError(
                REST_ERROR, 'Error accessing {}.  ERROR:  {}'.format(url, e))
        if response.status < 300:
            if sys.version_info[0] == 2:
                return pulljson.JSONPullParser(response)
            else:
                return pulljson.JSONPullParser(
                    io.TextIOWrapper(response, encoding="utf8"))
        if response.status < 400:
            raise InterfaceError(
                response.status,
                "HTTP Status: {}.   ERROR:  Redirection not supported.")
        else:
            msg = response.read().decode("utf8")
            try:
                errorDetails = json.loads(msg)
            except Exception:
                raise InterfaceError(
                    response.status, "HTTP Status: " + str(response.status) +
                    ", URL: " + url + ", Details:  " + str(msg))
            if response.status == HTTP_STATUS_DATABASE_ERROR:
                raise DatabaseError(
                    int(errorDetails['error']), errorDetails['message'])
            else:
                raise InterfaceError(response.status, "HTTP Status: " + str(
                    response.status) + ", URL: " + url +
                    ", Details:  " + str(errorDetails))
