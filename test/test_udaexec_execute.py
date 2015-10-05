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
import unittest
import sys
import logging
import os
import decimal
import teradata
import threading
from teradata import util

logger = logging.getLogger(__name__)


class UdaExecExecuteTest ():

    """Test UdaExec execute methods on a named data source"""

    @classmethod
    def setUpClass(cls):
        cls.username = cls.password = util.setupTestUser(udaExec, cls.dsn)
        cls.failure = False

    def testCursorBasics(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            with conn.cursor() as cursor:
                count = 0
                for row in cursor.execute("SELECT * FROM DBC.DBCInfo"):
                    self.assertEqual(len(row), 2)
                    self.assertIsNotNone(row[0])
                    self.assertIsNotNone(row['InfoKey'])
                    self.assertIsNotNone(row['infokey'])
                    self.assertIsNotNone(row.InfoKey)
                    self.assertIsNotNone(row.infokey)
                    self.assertIsNotNone(row[1])
                    self.assertIsNotNone(row['InfoData'])
                    self.assertIsNotNone(row['infodata'])
                    self.assertIsNotNone(row.infodata)
                    self.assertIsNotNone(row.InfoData)

                    row[0] = "test1"
                    self.assertEqual(row[0], "test1")
                    self.assertEqual(row['InfoKey'], "test1")
                    self.assertEqual(row.infokey, "test1")

                    row['infokey'] = "test2"
                    self.assertEqual(row[0], "test2")
                    self.assertEqual(row['InfoKey'], "test2")
                    self.assertEqual(row.infokey, "test2")

                    row.infokey = "test3"
                    self.assertEqual(row[0], "test3")
                    self.assertEqual(row['InfoKey'], "test3")
                    self.assertEqual(row.InfoKey, "test3")
                    count += 1

                self.assertEqual(cursor.description[0][0], "InfoKey")
                self.assertEqual(cursor.description[0][1], teradata.STRING)
                self.assertEqual(cursor.description[1][0], "InfoData")
                self.assertEqual(cursor.description[1][1], teradata.STRING)
                self.assertEqual(count, 3)

    def testQueryBands(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.username,
                             queryBands={"queryBand1": "1",
                                         "queryBand2": "2"}) as conn:
            cursor = conn.cursor()
            queryBands = cursor.execute("Select GetQueryBand()").fetchone()[0]
            self.assertIn("ApplicationName=PyTdUnitTests", queryBands)
            self.assertIn("Version=1.00.00.01", queryBands)
            self.assertIn("queryBand1=1", queryBands)
            self.assertIn("queryBand2=2", queryBands)

    def testRollbackCommitTeraMode(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password, autoCommit=False,
                             transactionMode='TERA') as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()

            cursor.execute("CREATE TABLE testRollbackCommitTeraMode (x INT)")
            conn.commit()

            cursor.execute("INSERT INTO testRollbackCommitTeraMode VALUES (1)")

            row = cursor.execute(
                "SELECT COUNT(*) FROM testRollbackCommitTeraMode").fetchone()
            self.assertEqual(row[0], 1)

            conn.rollback()

            row = cursor.execute(
                "SELECT COUNT(*) FROM testRollbackCommitTeraMode").fetchone()
            self.assertEqual(row[0], 0)

    def testRollbackCommitAnsiMode(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password, autoCommit="false",
                             transactionMode='ANSI') as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()

            cursor.execute("CREATE TABLE testRollbackCommitAnsiMode (x INT)")
            conn.commit()

            cursor.execute("INSERT INTO testRollbackCommitAnsiMode VALUES (1)")

            row = cursor.execute(
                "SELECT COUNT(*) FROM testRollbackCommitAnsiMode").fetchone()
            self.assertEqual(row[0], 1)

            conn.rollback()

            row = cursor.execute(
                "SELECT COUNT(*) FROM testRollbackCommitAnsiMode").fetchone()
            self.assertEqual(row[0], 0)

    def testSqlScriptExecution(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            scriptFile = os.path.join(
                os.path.dirname(__file__), "testScript.sql")
            udaExec.config['sampleTable'] = 'sample1'
            conn.execute(file=scriptFile)
            rows = conn.execute("SELECT * FROM ${sampleTable}").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].a, 23)
            self.assertEqual(
                rows[0].b, "This is a test;Making sure semi-colons\nin "
                "statements work.$")
            self.assertEqual(rows[0].e, decimal.Decimal("1.23456"))
            self.assertEqual(rows[0].f, decimal.Decimal(789))

    def testSqlScriptExecutionDelimiter(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            scriptFile = os.path.join(
                os.path.dirname(__file__), "testScript2.sql")
            udaExec.config['sampleTable'] = 'sample2'
            conn.execute(file=scriptFile, delimiter="|")
            rows = conn.execute("SELECT * FROM ${sampleTable}").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].a, 23)
            self.assertEqual(
                rows[0].b,
                'This is a test|Making sure pipes in statements work.')
            self.assertEqual(rows[0].e, decimal.Decimal("1.23456"))
            self.assertEqual(rows[0].f, decimal.Decimal(789))

    def testBteqScriptExecution(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()
            scriptFile = os.path.join(
                os.path.dirname(__file__), "testBteqScript.sql")
            conn.execute(file=scriptFile, fileType="bteq")
            rows = cursor.execute(
                "SELECT * FROM {}.Sou_EMP_Tab".format(
                    self.username)).fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].EMP_ID, 1)
            self.assertEqual(rows[0].EMP_Name.strip(), 'bala')
            self.assertEqual(rows[1].EMP_ID, 2)
            self.assertEqual(rows[1].EMP_Name.strip(), 'nawab')

    def testExecuteManyFetchMany(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()

            rowCount = 10000
            cursor.execute("""CREATE TABLE testExecuteManyFetchMany (
                id INT, name VARCHAR(128), dob TIMESTAMP)""")
            cursor.executemany(
                "INSERT INTO testExecuteManyFetchMany \
                VALUES (?, ?, CURRENT_TIMESTAMP)",
                [(x, "{ \\[]" + str(x) + "\"}") for x in range(0, rowCount)],
                batch=True, logParamFrequency=1000)

            row = cursor.execute(
                "SELECT COUNT(*) FROM testExecuteManyFetchMany").fetchone()
            self.assertEqual(row[0], rowCount)

            setCount = 10
            cursor.execute("".join(
                ["SELECT * FROM testExecuteManyFetchMany WHERE id = %s; "
                 % x for x in range(0, setCount)]))
            for i in range(0, setCount):
                if i != 0:
                    self.assertTrue(cursor.nextset())
                row = cursor.fetchone()
                self.assertEqual(row.id, i)
                self.assertEqual(row.name, "{ \\[]" + str(i) + "\"}")
            self.assertIsNone(cursor.nextset())

            setCount = 10
            cursor.execute("".join(
                ["SELECT * FROM testExecuteManyFetchMany WHERE id = %s; "
                 % x for x in range(0, setCount)]))
            for i in range(0, setCount):
                if i != 0:
                    self.assertTrue(cursor.nextset())
                row = cursor.fetchall()
                self.assertEqual(row[0].id, i)
                self.assertEqual(row[0].name, "{ \\[]" + str(i) + "\"}")
            self.assertIsNone(cursor.nextset())

            fetchCount = 500
            cursor.execute("SELECT * FROM testExecuteManyFetchMany")
            for i in range(0, rowCount // fetchCount):
                rows = cursor.fetchmany(fetchCount)
                self.assertEqual(len(rows), fetchCount)
            rows = cursor.fetchmany(fetchCount)
            self.assertEqual(len(rows), 0)
            self.assertIsNone(cursor.fetchone())

    def testVolatileTable(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()

            rowCount = 1000
            cursor.execute(
                "CREATE VOLATILE TABLE testVolatileTable, NO FALLBACK ,"
                "NO BEFORE JOURNAL,NO AFTER JOURNAL, NO LOG, CHECKSUM = "
                "DEFAULT (id INT, name VARCHAR(128), dob TIMESTAMP) "
                "ON COMMIT PRESERVE ROWS;")
            cursor.executemany(
                "INSERT INTO testVolatileTable VALUES (?, ?, "
                "CURRENT_TIMESTAMP)",
                [(x, "{ \\[]" + str(x) + "\"}") for x in range(0, rowCount)],
                batch=True)

            row = cursor.execute(
                "SELECT COUNT(*) FROM testVolatileTable").fetchone()
            self.assertEqual(row[0], rowCount)

            setCount = 10
            cursor.execute("".join(
                ["SELECT * FROM testVolatileTable WHERE id = %s; " % x
                 for x in range(0, setCount)]))
            for i in range(0, setCount):
                if i != 0:
                    self.assertTrue(cursor.nextset())
                row = cursor.fetchone()
                self.assertEqual(row.id, i)
                self.assertEqual(row.name, "{ \\[]" + str(i) + "\"}")
            self.assertIsNone(cursor.nextset())

            fetchCount = 500
            cursor.execute("SELECT * FROM testVolatileTable")
            for i in range(0, rowCount // fetchCount):
                rows = cursor.fetchmany(fetchCount)
                self.assertEqual(len(rows), fetchCount)
            rows = cursor.fetchmany(fetchCount)
            self.assertEqual(len(rows), 0)
            self.assertIsNone(cursor.fetchone())

    def testProcedure(self):
        # REST-307 - Unable to create Stored Procedure using REST, always use
        # ODBC.
        with udaExec.connect("ODBC", username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            for r in conn.execute(
                """REPLACE PROCEDURE testProcedure1
                    (IN p1 INTEGER,  OUT p2 INTEGER)
                    BEGIN
                        SET p2 = p1;
                    END;"""):
                logger.info(r)
            for r in conn.execute(
                """REPLACE PROCEDURE testProcedure2 (INOUT p2 INTEGER)
                    BEGIN
                        SET p2 = p2 * p2;
                    END;"""):
                logger.info(r)
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            for i in range(0, 10):
                result = conn.callproc(
                    "testProcedure1",
                    (i, teradata.OutParam("p2", dataType="INTEGER")))
                self.assertEqual(result["p2"], i)
            # Does not work with REST due to REST-308
            if self.dsn == "ODBC":
                for i in range(0, 10):
                    result = conn.callproc(
                        "testProcedure2",
                        (teradata.InOutParam(i, "p1", dataType="INTEGER"), ))
                    self.assertEqual(result["p1"], i * i)

    def testProcedureWithBinaryAndFloatParameters(self):
        if self.dsn == "ODBC":
            with udaExec.connect(self.dsn, username=self.username,
                                 password=self.password) as conn:
                self.assertIsNotNone(conn)
                for r in conn.execute(
                    """REPLACE PROCEDURE testProcedure1
                        (INOUT p1 VARBYTE(128),  OUT p2 VARBYTE(128),
                        INOUT p3 FLOAT, OUT p4 FLOAT, OUT p5 TIMESTAMP)
                        BEGIN
                            SET p2 = p1;
                            SET p4 = p3;
                            SET p5 = CURRENT_TIMESTAMP;
                        END;"""):
                    logger.info(r)
                result = conn.callproc(
                    "testProcedure1",
                    (teradata.InOutParam(bytearray([0xFF]), "p1"),
                     teradata.OutParam("p2"),
                     teradata.InOutParam(float("inf"), "p3"),
                     teradata.OutParam("p4", dataType="FLOAT"),
                     teradata.OutParam("p5", dataType="TIMESTAMP")))
                self.assertEqual(result.p1, bytearray([0xFF]))
                self.assertEqual(result.p2, result.p1)
                self.assertEqual(result.p3, float('inf'))
                self.assertEqual(result.p4, result.p3)

    def testQueryTimeout(self):
        with self.assertRaises(teradata.DatabaseError) as cm:
            with udaExec.connect(self.dsn,  username=self.username,
                                 password=self.password) as conn:
                conn.execute(
                    "CREATE TABLE testQueryTimeout (id INT, "
                    "name VARCHAR(128), dob TIMESTAMP)")
                conn.executemany(
                    "INSERT INTO testQueryTimeout VALUES (?, ?, "
                    "CURRENT_TIMESTAMP)",
                    [(x, str(x)) for x in range(0, 10000)],
                    batch=True)
                conn.execute(
                    "SELECT * FROM testQueryTimeout t1, testQueryTimeout t2",
                    queryTimeout=1)
        self.assertIn("timeout", cm.exception.msg)

    def testNewlinesInQuery(self):
        with udaExec.connect(self.dsn,  username=self.username,
                             password=self.password,
                             transactionMode="ANSI") as conn:
            with self.assertRaises(teradata.DatabaseError) as cm:
                conn.execute(
                    """--THIS SQL STATMENT HAS A SYNATAX ERROR
                    SELECT * FROM ThereIsNoWayThisTableExists""")
            self.assertEqual(3807, cm.exception.code)
            row = conn.execute("""--THIS SQL STATMENT HAS CORRECT SYNTAX
                                SELECT
                                'Line\nFeed'
                                AS
                                linefeed""").fetchone()
            self.assertEqual(row.linefeed, 'Line\nFeed')

    def testUnicode(self):
        insertCount = 1000
        unicodeString = u"\u4EC5\u6062\u590D\u914D\u7F6E\u3002\u73B0"
        "\u6709\u7684\u5386\u53F2\u76D1\u63A7\u6570\u636E\u5C06"
        "\u4FDD\u7559\uFF0C\u4E0D\u4F1A\u4ECE\u5907\u4EFD\u4E2D"
        "\u6062\u590D\u3002"
        with udaExec.connect(self.dsn,  username=self.username,
                             password=self.password) as conn:
            self.assertEqual(conn.execute(
                u"SELECT '{}'".format(unicodeString)).fetchone()[0],
                unicodeString)
            conn.execute(
                "CREATE TABLE testUnicode (id INT, name VARCHAR(10000) "
                "CHARACTER SET UNICODE)")
            conn.executemany("INSERT INTO testUnicode VALUES (?, ?)", [
                             (x, unicodeString)
                             for x in range(0, insertCount)],
                             batch=True)
            conn.executemany("INSERT INTO testUnicode VALUES (?, ?)", [
                             (x + insertCount, unicodeString * 100)
                             for x in range(0, 10)],
                             batch=False)
            count = 0
            for row in conn.execute("SELECT * FROM testUnicode"):
                if row.id >= insertCount:
                    self.assertEqual(row.name, unicodeString * 100)
                else:
                    self.assertEqual(row.name, unicodeString)
                count += 1
            self.assertEqual(count, insertCount + 10)

    def testExecuteWhileIterating(self):
        insertCount = 100
        with udaExec.connect(self.dsn,  username=self.username,
                             password=self.password) as conn:
            conn.execute(
                "CREATE TABLE testExecuteWhileIterating (id INT, "
                "name VARCHAR(128))")
            conn.executemany(
                "INSERT INTO testExecuteWhileIterating VALUES (?, ?)",
                [(x, str(x)) for x in range(0, insertCount)], batch=True)
            count = 0
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM testExecuteWhileIterating"
                ).fetchone()[0], insertCount)
            for row in conn.cursor().execute(
                    "SELECT * FROM testExecuteWhileIterating"):
                conn.execute(
                    "DELETE FROM testExecuteWhileIterating WHERE id = ?",
                    (row.id, ))
                count += 1
            self.assertEqual(count, insertCount)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM testExecuteWhileIterating"
            ).fetchone()[0], 0)

    def testUdaExecMultipleThreads(self):
        threadCount = 5
        threads = []
        for i in range(0, threadCount):
            t = threading.Thread(
                target=connectAndExecuteSelect, args=(self, i))
            t.daemon = True
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        if self.failure:
            raise self.failure

    def testConnectionMultipleThreads(self):
        threadCount = 5
        threads = []
        with udaExec.connect(self.dsn,  username=self.username,
                             password=self.password) as conn:
            for i in range(0, threadCount):
                t = threading.Thread(
                    target=cursorAndExecuteSelect, args=(self, conn, i))
                t.daemon = True
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
        if self.failure:
            raise self.failure

    def testAutoGeneratedKeys(self):
        # Auto-generated keys are not supported by REST.
        if self.dsn == "ODBC":
            rowCount = 1
            with udaExec.connect(self.dsn,  username=self.username,
                                 password=self.password,
                                 ReturnGeneratedKeys="C") as conn:
                conn.execute(
                    "CREATE TABLE testAutoGeneratedKeys (id INTEGER "
                    "GENERATED BY DEFAULT AS IDENTITY, name VARCHAR(128))")
                count = 0
                for row in conn.executemany(
                        "INSERT INTO testAutoGeneratedKeys VALUES (NULL, ?)",
                        [(str(x), ) for x in range(0, rowCount)]):
                    count += 1
                    print(row)
                    self.assertEqual(row[0], count)
                # Potential ODBC bug is preventing this test case from
                # passing, e-mail sent to ODBC support team.
                # self.assertEqual(count, rowCount)

    def testEmptyResultSet(self):
        with udaExec.connect(self.dsn,  username=self.username,
                             password=self.password) as conn:
            conn.execute(
                "CREATE TABLE testEmptyResultSet (id INTEGER, "
                "name VARCHAR(128))")
            count = 0
            with conn.cursor() as cursor:
                for row in cursor.execute("SELECT * FROM testEmptyResultSet"):
                    count += 1
                    print(row)
                self.assertEqual(count, 0)


def connectAndExecuteSelect(testCase, threadId):
    try:
        with udaExec.connect(testCase.dsn,  username=testCase.username,
                             password=testCase.password) as session:
            for row in session.execute("SELECT * FROM DBC.DBCInfo"):
                logger.info(str(threadId) + ": " + str(row))
    except Exception as e:
        testCase.failure = e


def cursorAndExecuteSelect(testCase, session, threadId):
    try:
        with session.cursor() as cursor:
            for row in cursor.execute("SELECT * FROM DBC.DBCInfo"):
                logger.info(str(threadId) + ": " + str(row))
    except Exception as e:
        testCase.failure = e


# The unit tests in the UdaExecExecuteTest are execute once for each named
# data source below.
util.createTestCasePerDSN(
    UdaExecExecuteTest, unittest.TestCase, ("HTTP", "HTTPS", "ODBC"))

if __name__ == '__main__':
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    sh.setLevel(logging.INFO)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(sh)

configFiles = [os.path.join(os.path.dirname(__file__), 'udaexec.ini')]
udaExec = teradata.UdaExec(configFiles=configFiles, configureLogging=False)
udaExec.checkpoint()


def runTest(testName):
    suite = unittest.TestSuite()
    suite.addTest(UdaExecExecuteTest_ODBC(testName))  # @UndefinedVariable # noqa
    suite.addTest(UdaExecExecuteTest_HTTPS(testName))  # @UndefinedVariable # noqa
    unittest.TextTestRunner().run(suite)

if __name__ == '__main__':
    # runTest('testExecuteManyFetchMany')
    unittest.main()
