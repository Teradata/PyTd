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
import os
import teradata
from teradata import tdsql, util


class TdSqlTest (unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.username = cls.password = util.setupTestUser(udaExec, dsn)

    @classmethod
    def tearDownClass(cls):
        util.cleanupTestUser(udaExec, dsn)

    def testGlobals(self):
        self.assertEqual(tdsql.apilevel, "2.0")
        self.assertEqual(tdsql.threadsafety, 1)
        self.assertEqual(tdsql.paramstyle, "qmark")

    def testSystemNotFound(self):
        with self.assertRaises(tdsql.OperationalError) as cm:
            tdsql.connect(system="hostNotFound.td.teradata.com",
                           username=self.username, password=self.password)
        self.assertTrue("Hostname lookup failed" in str(cm.exception), cm.exception)

    def testBadCredentials(self):
        with self.assertRaises(tdsql.DatabaseError) as cm:
            tdsql.connect(system=system, username="bad", password="bad")
        self.assertEqual(cm.exception.code, 8017, cm.exception.msg)

        with self.assertRaises(tdsql.InterfaceError) as cm:
            tdsql.connect(system=system,
                           username=self.username, password=self.password, charset="UTF16")
        self.assertTrue("Connection charset" in str(cm.exception), cm.exception)

    def testConnect(self):
        conn1 = tdsql.connect(
            system=system, username=self.username, password=self.password)
        self.assertIsNotNone(conn1)
        conn2 = tdsql.connect(
            system=system, username=self.username, password=self.password)
        self.assertIsNotNone(conn2)
        tdsql.cleanupConnections ()

    def testCursorBasics(self):
        with tdsql.connect(system=system, username=self.username,
                            password=self.password, autoCommit=True) as conn:
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
                self.assertEqual(cursor.description[0][1], tdsql.STRING)
                self.assertEqual(cursor.description[1][0], "InfoData")
                self.assertEqual(cursor.description[1][1], tdsql.STRING)
                self.assertEqual(count, 3)

    def testExecuteWithParamsMismatch(self):
        with self.assertRaises(teradata.DatabaseError) as cm:
            with tdsql.connect(system=system, username=self.username,
                                password=self.password,
                                autoCommit=True) as conn:
                self.assertIsNotNone(conn)
                with conn.cursor() as cursor:
                    cursor.execute(
                        "CREATE TABLE testExecuteWithParamsMismatch (id INT, "
                        "name VARCHAR(128), dob TIMESTAMP)")
                    cursor.execute(
                        "INSERT INTO testExecuteWithParamsMismatch "
                        "VALUES (?, ?, ?)", (1, "TEST", ))
        self.assertEqual(
            cm.exception.code, 3939, cm.exception.msg)

configFiles = [os.path.join(os.path.dirname(__file__), 'udaexec.ini')]
udaExec = teradata.UdaExec(configFiles=configFiles, configureLogging=False)
dsn = 'TERADATASQL'
tdsqlConfig = udaExec.config.section(dsn)
system = tdsqlConfig['system']

if __name__ == '__main__':
    unittest.main()
