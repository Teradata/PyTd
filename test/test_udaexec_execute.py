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
import random
import time
from teradata import util

logger = logging.getLogger(__name__)


class UdaExecExecuteTest ():

    """Test UdaExec execute methods on a named data source"""

    @classmethod
    def setUpClass(cls):
        cls.username = cls.password = util.setupTestUser(udaExec, cls.dsn)
        cls.failure = False

    @classmethod
    def tearDownClass(cls):
        util.cleanupTestUser(udaExec, cls.dsn)

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

    def testDefaultDatabase(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password, database="DBC") as conn:
            self.assertIsNotNone(conn)
            conn.execute("SELECT * FROM DBCInfo")

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

    def testRollbackCreateAnsiMode(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password, autoCommit="false",
                             transactionMode='ANSI') as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()

            cursor.execute("CREATE TABLE testRollbackCreateAnsiMode (x INT)")
            # instead of using cursor.rollback, use escape syntax so warnings will
            # be printed in log file.
            cursor.execute("{fn teradata_fake_result_sets}{fn teradata_rollback}")

            with self.assertRaises (teradata.DatabaseError) as cm:
                cursor.execute("INSERT INTO testRollbackCreateAnsiMode VALUES (1)")
            self.assertEqual (cm.exception.code, 3807)
        # end testRollbackWarningAnsiMode

    def testRollbackCreateTeraMode(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password, autoCommit="false",
                             transactionMode='TERA') as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()

            cursor.execute("CREATE TABLE testRollbackCreateTeraMode (x INT)")
            # instead of using cursor.rollback, use escape syntax so warnings will
            # be printed in log file.
            cursor.execute("{fn teradata_fake_result_sets}{fn teradata_rollback}")

            with self.assertRaises (teradata.DatabaseError) as cm:
                cursor.execute("INSERT INTO testRollbackCreateTeraMode VALUES (1)")
            self.assertEqual (cm.exception.code, 3807)
        # end testRollbackWarningAnsiMode

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
                rows[0].b, "This -----  is a test;Making sure semi-colons\nin "
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

    def testProcedureInOutParamNull(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            for r in conn.execute(
                """REPLACE PROCEDURE testProcedureIONull
                    (IN p1 INTEGER,  INOUT p2 INTEGER,
                        INOUT p3 VARCHAR(200), INOUT p4 FLOAT,
                        INOUT p5 VARBYTE(128))
                    BEGIN
                        IF p2 IS NULL THEN
                            SET p2 = p1;
                        END IF;
                        IF p3 IS NULL THEN
                            SET p3 = 'PASSING TEST';
                        END IF;
                        IF p4 IS NULL THEN
                            SET p4 = p1;
                        END IF;
                        IF p5 IS NULL THEN
                            SET p5 = 'AABBCCDDEEFFAABBCCDDEEFF'XBV;
                        END IF;
                    END;"""):
                logger.info(r)

        sP3Value = 'PASSING TEST'
        byP5Value = bytearray ([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            for i in range(0, 10):
                result = conn.callproc(
                    "testProcedureIONull",
                    (i,
                        teradata.InOutParam(None, "p2", dataType='INTEGER'),
                        teradata.InOutParam(None, "p3", dataType='VARCHAR(200)', size = i + 1),
                        teradata.InOutParam(None, "p4", dataType='FLOAT'),
                        teradata.InOutParam(None, "p5", dataType='VARBYTE(50)', size = 14 - i)))
                self.assertEqual(result["p2"], i)
                self.assertEqual(result["p3"], sP3Value [:i + 1])
                self.assertEqual(result["p4"], i)
                self.assertEqual(result["p5"], byP5Value [:14 - i])

    def testProcedure(self):
        with udaExec.connect(self.dsn, username=self.username,
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
            for i in range(0, 10):
                result = conn.callproc(
                    "testProcedure2",
                    (teradata.InOutParam(i, "p1", dataType="INTEGER"), ))
                self.assertEqual(result["p1"], i * i)

    def testProcedureWithLargeLobInput(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            scriptFile = os.path.join(
                os.path.dirname(__file__), "testClobSp.sql")
            conn.execute(file=scriptFile, delimiter=";;")

            SQLText = "CDR_2011-07-25_090000.000000.txt\n"
            SQLText = SQLText * 5000

            conn.callproc('GCFR_BB_ExecutionLog_Set',
                          ('TestProc', 127, 12, 96, 2, 2, 'MyText',
                           'Test.py', 0, 0, SQLText))

            count = 0
            for row in conn.execute("SELECT * FROM GCFR_Execution_Log"):
                self.assertEqual(row.Sql_Text, SQLText)
                count = count + 1
            self.assertEqual(count, 1)

    def testProcedureWithBinaryAndFloatParameters(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            for r in conn.execute(
                """REPLACE PROCEDURE testProcedureBF
                    (INOUT p1 VARBYTE(128),  OUT p2 VARBYTE(128),
                    INOUT p3 FLOAT, OUT p4 FLOAT, OUT p5 TIMESTAMP,
                    INOUT p6 VARCHAR(10), OUT p7 VARCHAR (10))
                    BEGIN
                        SET p2 = p1;
                        SET p4 = p3;
                        SET p5 = CURRENT_TIMESTAMP;
                        SET p7 = p6;
                    END;"""):
                logger.info(r)
            result = conn.callproc(
                "testProcedureBF",
                (teradata.InOutParam(bytearray([0xFF, 0xFE, 0xFF]), "p1", size=2),
                    teradata.OutParam("p2", size=1),
                    teradata.InOutParam(float("inf"), "p3"),
                    teradata.OutParam("p4", dataType="FLOAT"),
                    teradata.OutParam("p5", dataType="TIMESTAMP"),
                    teradata.InOutParam("abcdefghij", "p6", size = 40),
                    teradata.OutParam ("p7", size = 4)))
            self.assertEqual(result.p1, bytearray([0xFF, 0xFE]))
            self.assertEqual(result.p2, result.p1[:1])
            self.assertEqual(result.p3, float('inf'))
            self.assertEqual(result.p4, result.p3)
            self.assertEqual(result.p6, "abcdefghij")
            self.assertEqual(result.p7,result.p6[:4])

    def testProcedureWithResultSet(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            for r in conn.execute(
                """REPLACE PROCEDURE testProcedureWithResultSet()
                    DYNAMIC RESULT SETS 1
                    BEGIN
                        DECLARE QUERY1 VARCHAR(22000);
                        DECLARE dyna_set1 CURSOR WITH RETURN TO CALLER FOR STMT1;
                        SET QUERY1 = 'select * from dbc.dbcinfo';
                        PREPARE STMT1 FROM QUERY1;
                        OPEN dyna_set1;
                        DEALLOCATE PREPARE STMT1;
                    END;"""):
                logger.info(r)
            with conn.cursor() as cursor:
                cursor.callproc("testProcedureWithResultSet", ())
                self.assertTrue ("cursor.nextset failed to retrieve dynamic result set", cursor.nextset ())
                self.assertEqual(len(cursor.fetchall()), 3)

    def testProcedureWithParamsAndResultSet(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            for r in conn.execute(
                """REPLACE PROCEDURE testProcedureWithParamsAndResultSet
                    (IN p1 VARBYTE (128), INOUT p2 VARBYTE(128), OUT p3 VARBYTE(128),
                     IN p4 VARCHAR (100) , INOUT p5 VARCHAR(100) , OUT p6 VARCHAR (100))
                    DYNAMIC RESULT SETS 2
                    BEGIN
                        declare cur1 cursor with return for select :p1 as c1, bytes (:p1) as c2     , :p2 as c3, bytes (:p2) as c4 ;
                        declare cur2 cursor with return for select :p4 as c1, characters (:p4) as c2, :p5 as c3, characters (:p5) as c4 ;
                        open cur1 ;
                        open cur2 ;

                        SET p3 = p2;
                        SET p2 = p1;
                        SET p6 = p5;
                        SET p5 = p4;
                    END;"""):
                logger.info(r)
            with conn.cursor() as cursor:
                result = cursor.callproc("testProcedureWithParamsAndResultSet",
                    (teradata.InParam   (bytearray ([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])),
                    teradata.InOutParam (bytearray ([0xFF, 0xFE, 0xFF, 0xEE]), "p2", dataType = 'VARBYTE(20)', size=9),
                    teradata.OutParam   ("p3", size=3),
                    teradata.InParam    ("abcdefghijklmnop"),
                    teradata.InOutParam ("123456789012345678901", "p5", dataType = 'VARCHAR(128)', size = 15),
                    teradata.OutParam   ("p6", size = 4)))
                self.assertEqual(result.p2, bytearray ([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0xAA, 0xBB, 0xCC]))
                self.assertEqual(result.p3, bytearray ([0xFF, 0xFE, 0xFF]))
                self.assertEqual(result.p5, "abcdefghijklmno")
                self.assertEqual(result.p6, "1234")

                self.assertTrue ("cursor.nextset failed to retrieve dynamic result set one", cursor.nextset ())
                compareLists (self, cursor.description, [
                    ['c1', bytearray      , None, 128,    0, None, False],
                    ['c2', decimal.Decimal, None,   4,   10, None, False],
                    ['c3', bytearray      , None, 128,    0, None, False],
                    ['c4', decimal.Decimal, None,   4,   10, None, False]
                ])
                compareLists (self, cursor.types, [
                    ['VARBYTE', bytearray],
                    ['INTEGER', decimal.Decimal],
                    ['VARBYTE', bytearray],
                    ['INTEGER', decimal.Decimal]
                ])
                for row in cursor.fetchall () :
                    self.assertEqual(row [0], bytearray ([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]))
                    self.assertEqual(row [1], len (row [0]))
                    self.assertEqual(row [2], bytearray ([0xFF, 0xFE, 0xFF, 0xEE]))
                    self.assertEqual(row [3], len (row [2]))

                self.assertTrue ("cursor.nextset failed to retrieve dynamic result set two", cursor.nextset ())
                compareLists (self, cursor.description, [
                    ['c1', str            , None, 200,  0, None, False],
                    ['c2', decimal.Decimal, None,   4, 10, None, False],
                    ['c3', str            , None, 200,  0, None, False],
                    ['c4', decimal.Decimal, None,   4, 10, None, False]
                ])
                compareLists (self, cursor.types, [
                    ['VARCHAR', str],
                    ['INTEGER', decimal.Decimal],
                    ['VARCHAR', str],
                    ['INTEGER', decimal.Decimal]
                ])
                for row in cursor.fetchall () :
                    self.assertEqual(row [0], "abcdefghijklmnop")
                    self.assertEqual(row [1], len (row [0]))
                    self.assertEqual(row [2], "123456789012345678901")
                    self.assertEqual(row [3], len (row [2]))
        # end testProcedureWithParamsAndResultSet

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
                self.assertEqual(count, 0)

    def testFetchArraySize1000(self):
        rows = 5000
        randomset = []
        for j in range(rows):
            rowset = [j, ]
            rowset.append(int(random.random() * 100000))
            rowset.append(int(random.random() * 100000))
            rowset.append(int(random.random() * 100000))
            rowset.append(str(random.random() * 100000))
            rowset.append(str(random.random() * 100000))
            rowset.append(str(random.random() * 100000))
            randomset.append(rowset)

        createtablestatement = """
        CREATE MULTISET TABLE testFetchArraySize1000
            (
               id INTEGER,
               randint1 INTEGER,
               randint2 INTEGER,
               randint3 INTEGER,
               randchar1 VARCHAR(20),
               randchar2 VARCHAR(20),
               randchar3 VARCHAR(20)
            )
         NO PRIMARY INDEX;
         """
        with udaExec.connect(self.dsn,  username=self.username,
                             password=self.password) as session:
            cursor = session.execute(createtablestatement)
            cursor.arraysize = 1000
            index = 0
            while index < 20:
                session.executemany("""INSERT INTO testFetchArraySize1000
                                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                    randomset,
                                    batch=True)
                for y in randomset:
                    y[0] = y[0] + len(randomset)
                index += 1
            fetchRows(self, 100, randomset, session)
            fetchRows(self, 1000, randomset, session)
            fetchRows(self, 10000, randomset, session)
            fetchRows(self, 100000, randomset, session)

    def testDollarSignInPassword(self):
        with udaExec.connect(self.dsn) as session:
            session.execute("DROP USER testDollarSignInPassword",
                            ignoreErrors=[3802, 3524])
        util.setupTestUser(udaExec, self.dsn, user='testDollarSignInPassword',
                           passwd='pa$$$$word')
        with udaExec.connect(self.dsn, username='testDollarSignInPassword',
                             password='pa$$$$word') as session:
            session.execute("SELECT * FROM DBC.DBCINFO")

    def testOperationsOnClosedCursor(self):
        with udaExec.connect(self.dsn) as session:
            cursor = session.cursor()
            cursor.close()
            error = None
            try:
                cursor.execute("SELECT * FROM DBC.DBCINFO")
            except teradata.ProgrammingError as e:
                error = e
            self.assertIsNotNone(error)

    def testIgnoreError(self):
        with udaExec.connect(self.dsn) as session:
            cursor = session.execute("DROP DATABASE ThisDatabaseDoesNotExist",
                                     ignoreErrors=(3802,))
            self.assertIsNotNone(cursor.error)

    def testMultipleResultSets(self):
        with udaExec.connect(self.dsn) as session:
            cursor = session.execute("""SELECT 'string' as \"string\";
                SELECT 1 as \"integer\"""")
            self.assertEqual(cursor.description[0][0], 'string')
            self.assertTrue(cursor.nextset())
            self.assertEqual(cursor.description[0][0], 'integer')


def fetchRows(test, count, randomset, session):
    result = session.execute(
        """select * from testFetchArraySize1000 WHERE id < %s
        ORDER BY id""" % count)
    t0 = time.time()
    rowIndex = 0
    for r in result:
        colIndex = 0
        for col in r:
            if colIndex != 0:
                test.assertEqual(
                    col, randomset[rowIndex % len(randomset)][colIndex])
            colIndex += 1
        rowIndex += 1
    print("fetch over sample %s records: %s seconds " %
          (count, time.time() - t0))


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

def compareLists (test, aaoList1, aaoList2):
    nRowIndex = 0
    for aoList in aaoList1:
        for nCol in range (len (aoList)):
            test.assertEqual (aoList [nCol], aaoList2 [nRowIndex][nCol])
        nRowIndex += 1

util.createTestCasePerDSN(
    UdaExecExecuteTest, unittest.TestCase,  ("TERADATASQL",))

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
    suite.addTest(UdaExecExecuteTest_TERADATASQL(testName))  # @UndefinedVariable # noqa
    unittest.TextTestRunner().run(suite)
    unittest.addCleanup (util.cleanupTestUser(udaExec, 'TERADATASQL'))

if __name__ == '__main__':
    unittest.main()
