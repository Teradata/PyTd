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
from teradata import tdrest, util


class TdRestTest (unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.username = cls.password = util.setupTestUser(udaExec, dsn)

    def testGlobals(self):
        self.assertEqual(tdrest.apilevel, "2.0")
        self.assertEqual(tdrest.threadsafety, 1)
        self.assertEqual(tdrest.paramstyle, "qmark")

    def testBadHost(self):
        badHost = "badhostname"
        with self.assertRaises(tdrest.InterfaceError) as cm:
            tdrest.connect(
                host=badHost, system=system, username=self.username,
                password=self.password)
        self.assertEqual(cm.exception.code, tdrest.REST_ERROR)
        self.assertTrue(badHost in cm.exception.msg,
                        '{} not found in "{}"'.format(
                            badHost, cm.exception.msg))

    def testSystemNotFound(self):
        with self.assertRaises(tdrest.InterfaceError) as cm:
            tdrest.connect(
                host=host, system="unknown", username=self.username,
                password=self.password)
        self.assertEqual(cm.exception.code, 404)
        # print(cm.exception)
        self.assertTrue(
            "404" in cm.exception.msg,
            '404 not found in "{}"'.format(cm.exception.msg))

    def testBadCredentials(self):
        with self.assertRaises(tdrest.DatabaseError) as cm:
            tdrest.connect(
                host=host, system=system, username="bad", password="bad")
        # print(cm.exception)
        self.assertEqual(cm.exception.code, 8017, cm.exception.msg)

    def testConnect(self):
        conn = tdrest.connect(
            host=host, system=system, username=self.username,
            password=self.password)
        self.assertIsNotNone(conn)
        conn.close()

    def testCursorBasics(self):
        with tdrest.connect(host=host, system=system, username=self.username,
                            password=self.password) as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()
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
            self.assertEqual(cursor.description[0][1], tdrest.STRING)
            self.assertEqual(cursor.description[1][0], "InfoData")
            self.assertEqual(cursor.description[1][1], tdrest.STRING)
            self.assertEqual(count, 3)

    def testExecuteWithParamsMismatch(self):
        with self.assertRaises(teradata.InterfaceError) as cm:
            with tdrest.connect(host=host, system=system,
                                username=self.username,
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
        self.assertEqual(cm.exception.code, 400, cm.exception.msg)

    def testSessionAlreadyClosed(self):
        with tdrest.connect(host=host, system=system, username=self.username,
                            password=self.password, autoCommit=True) as conn:
            self.assertIsNotNone(conn)
            with conn.template.connect() as http:
                http.delete(
                    "/systems/{}/sessions/{}".format(conn.system,
                                                     conn.sessionId))

configFiles = [os.path.join(os.path.dirname(__file__), 'udaexec.ini')]
udaExec = teradata.UdaExec(configFiles=configFiles, configureLogging=False)
dsn = 'HTTP'
restConfig = udaExec.config.section(dsn)
host = restConfig['host']
system = restConfig['system']
super_username = restConfig['username']
super_password = restConfig['password']

if __name__ == '__main__':
    unittest.main()
