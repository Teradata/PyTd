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
import codecs
import datetime
import decimal
import json
import locale
import logging
import math
import os
import sys
import unittest

from teradata import util, datatypes
import teradata


class UdaExecDataTypesTest ():

    """Test UdaExec support for data types."""

    @classmethod
    def setUpClass(cls):
        cls.username = cls.password = util.setupTestUser(udaExec, cls.dsn)
        cls.failure = False

    @classmethod
    def tearDownClass(cls):
        util.cleanupTestUser(udaExec, cls.dsn)

    def testCharacterLimits(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            cursor = conn.execute(
                """CREATE TABLE testCharacterLimits (id INTEGER,
                    a CHAR CHARACTER SET UNICODE,
                    b CHAR(4) CHARACTER SET UNICODE,
                    c VARCHAR(100) CHARACTER SET UNICODE,
                    d VARCHAR(16000) CHARACTER SET UNICODE,
                    e CLOB (2000000) CHARACTER SET UNICODE)""")
            cursor.arraysize = 10
            params = [
                (101, u"\u3456", u"\u3456" * 4, u"\u3456" * 100,
                    u"\u3456" * 10666, u"\u3456" * 2000000),
                (102, None, None, None, None, None)]
            for p in params:
                conn.execute(
                    "INSERT INTO testCharacterLimits "
                    "VALUES (?, ?, ?, ?, ?, ?)", p)
            cursor = conn.execute("SELECT * FROM testCharacterLimits")
            aoDesc = [['id', decimal.Decimal, None, 4      , 10, None, True],
                      ['a' , str            , None, 3      ,  0, None, True],
                      ['b' , str            , None, 12     ,  0, None, True],
                      ['c' , str            , None, 300    ,  0, None, True],
                      ['d' , str            , None, 48000  ,  0, None, True],
                      ['e' , str            , None, 6000000,  0, None, True]]

            aoType = [['INTEGER', decimal.Decimal],
                      ['CHAR'   , str],
                      ['CHAR'   , str],
                      ['VARCHAR', str],
                      ['VARCHAR', str],
                      ['CLOB'   , str]]
            nRowIndex = 0
            for desc in cursor.description:
                for nCol in range (len (desc)):
                    self.assertEqual (desc [nCol], aoDesc [nRowIndex][nCol])
                nRowIndex += 1
            nRowIndex = 0
            for oType in cursor.types:
                for nCol in range (len (oType)):
                    self.assertEqual (oType [nCol], aoType [nRowIndex][nCol])
                nRowIndex += 1
            rowIndex = 0
            for row in cursor:
                colIndex = 0
                for col in row:
                    self.assertEqual(col, params[rowIndex][colIndex])
                    colIndex += 1
                rowIndex += 1

    def testStringDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                "CREATE TABLE testStringDataTypes (id INTEGER, a CHAR, "
                "a2 CHAR(4), b VARCHAR(100), c CLOB CHARACTER SET UNICODE, "
                "d VARCHAR(100))")
            conn.execute(
                "INSERT INTO testStringDataTypes VALUES (1, '1', '1', "
                "'1111111111', '11111111111111111111', NULL)")
            conn.execute("INSERT INTO testStringDataTypes "
                         "VALUES (?, ?, ?, ?, ?, ?)",
                         [2, str(2), str(2), str(2) * 10, str(2) * 20, None])
            conn.executemany("INSERT INTO testStringDataTypes " +
                             "VALUES (?, ?, ?, ?, ?, ?)",
                             [(i, str(i % 10), str(i % 100), str(i) * 10,
                               str(i) * 20, None) for i in range(3, 100)],
                             batch=True)
            for row in conn.execute("SELECT * FROM testStringDataTypes "
                                    "ORDER BY id"):
                # The strip is required on the CHAR columns because they are fixed width
                # and therefore have additonal spaces when compared to the data inserted.
                self.assertEqual(row.a.strip(), str(row.id % 10))
                self.assertEqual(row.a2.strip(), str(row.id % 100))
                self.assertEqual(row.b, str(row.id) * 10)
                self.assertEqual(row.c, str(row.id) * 20)
                self.assertIsNone(row.d)

            unicodeString = u"\u4EC5\u6062\u590D\u914D\u7F6E\u3002\u73B0"
            "\u6709\u7684\u5386\u53F2\u76D1\u63A7\u6570\u636E\u5C06"
            "\u4FDD\u7559\uFF0C\u4E0D\u4F1A\u4ECE\u5907\u4EFD\u4E2D"
            "\u6062\u590D\u3002"
            params = (101, None, None,  None, unicodeString * 100000, None)
            conn.execute(
                "INSERT INTO testStringDataTypes "
                "VALUES (?, ?, ?, ?, ?, ?)", params)
            for row in conn.execute("SELECT * FROM testStringDataTypes "
                                    "WHERE id = 101"):
                self.assertEqual(row.c, params[4])
            conn.executemany("INSERT INTO testStringDataTypes "
                                "VALUES (?, ?, ?, ?, ?, ?)",
                                [(i, str(i % 10), str(i % 100), str(i) * 10,
                                str(i % 10) * 64000, None)
                                for i in range(102, 112)],
                                batch=True)
            for row in conn.execute("{fn teradata_lobselect(S)}" + "SELECT * FROM testStringDataTypes "
                                    "WHERE id > 101"):
                self.assertEqual(row.c, (str(row.id % 10) * 64000))

    def testBinaryLimits(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            cursor = conn.execute(
                """CREATE TABLE testBinaryLimits (id INTEGER,
                    a BYTE,
                    c VARBYTE(10000),
                    e BLOB (2000000))""")
            cursor.arraysize = 10
            params = [
                (101, bytearray(os.urandom(1)),
                    bytearray(os.urandom(10000)),
                    bytearray(os.urandom(2000000))),
                (102, None, None, None)]
            for p in params:
                conn.execute(
                    "INSERT INTO testBinaryLimits "
                    "VALUES (?, ?, ?, ?)", p)
            cursor = conn.execute("SELECT * FROM testBinaryLimits")

            aoDesc = [['id', decimal.Decimal, None,       4, 10, None, True],
                      ['a' , bytearray      , None,       1,  0, None, True],
                      ['c' , bytearray      , None,   10000,  0, None, True],
                      ['e' , bytearray      , None, 2000000,  0, None, True]]
            aoType = [['INTEGER', decimal.Decimal],
                      ['BYTE'   , bytearray],
                      ['VARBYTE', bytearray],
                      ['BLOB'   , bytearray]]
            nRowIndex = 0
            for desc in cursor.description:
                for nCol in range (len (desc)):
                    self.assertEqual (desc [nCol], aoDesc [nRowIndex][nCol])
                nRowIndex += 1
            nRowIndex = 0
            for oType in cursor.types:
                for nCol in range (len (oType)):
                    self.assertEqual (oType [nCol], aoType [nRowIndex][nCol])
                nRowIndex += 1
            rowIndex = 0
            for row in cursor:
                colIndex = 0
                for col in row:
                    self.assertEqual(col, params[rowIndex][colIndex])
                    colIndex += 1
                rowIndex += 1

    def testBinaryDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                "CREATE TABLE testByteDataTypes (id INTEGER, a BYTE, "
                "b VARBYTE(6), c BYTE(4), d BLOB, e BLOB)")
            conn.execute(
                "INSERT INTO testByteDataTypes VALUES (1, 'FF'XBF, "
                "'AABBCCDDEEFF'XBV, 'AABBCCDD'XBF, "
                "'010203040506070809AABBCCDDEEFF'XBV, NULL)")
            conn.execute("INSERT INTO testByteDataTypes "
                            "VALUES (2, ?, ?, ?, ?, ?)",
                            (bytearray([0xFF]),
                            bytearray([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]),
                            bytearray([0xAA, 0xBB, 0xCC, 0xDD]),
                            bytearray([0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                                        0x07, 0x08, 0x09, 0xAA, 0xBB, 0xCC,
                                        0xDD, 0xEE, 0xFF]), None))
            for row in conn.execute("SELECT * FROM testByteDataTypes "
                                    "ORDER BY id"):
                self.assertEqual(row.a, bytearray([0xFF]))
                self.assertEqual(
                    row.b, bytearray([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]))
                self.assertEqual(
                    row.c, bytearray([0xAA, 0xBB, 0xCC, 0xDD]))
                self.assertEqual(row.d, bytearray(
                    [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x8, 0x9,
                        0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]))
                self.assertIsNone(row.e)
            params = (3, bytearray(os.urandom(1)),
                        bytearray(os.urandom(6)),
                        bytearray(os.urandom(4)),
                        bytearray(os.urandom(10000000)), None)
            conn.execute(
                "INSERT INTO testByteDataTypes VALUES (?, ?, ?, ?, ?, ?)",
                params)
            for row in conn.execute("SELECT * FROM testByteDataTypes "
                                    "WHERE id > 2 ORDER BY id"):
                self.assertEqual(row.a, params[1])
                self.assertEqual(row.b, params[2])
                self.assertEqual(row.c, params[3])
                self.assertEqual(row.d, params[4])
                self.assertIsNone(row.e)
            conn.execute("DELETE FROM testByteDataTypes WHERE id > 2")
            params = [(i, bytearray(os.urandom(1)),
                        bytearray(os.urandom(6)),
                        bytearray(os.urandom(4)),
                        bytearray(os.urandom(10000)), None)
                        for i in range(3, 100)]
            conn.executemany(
                "INSERT INTO testByteDataTypes VALUES (?, ?, ?, ?, ?, ?)",
                params, batch=True)
            for row in conn.execute("SELECT * FROM testByteDataTypes "
                                    "WHERE id > 3 ORDER BY id"):
                param = params[int(row.id) - 3]
                self.assertEqual(row.a, param[1])
                self.assertEqual(row.b, param[2])
                self.assertEqual(row.c, param[3])
                self.assertEqual(row.d, param[4])
                self.assertIsNone(row.e)

    def testMixedDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                "CREATE TABLE testByteDataType (id INTEGER, b BYTE(4), "
                "c CHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL, "
                "d BYTE(4))")
            conn.execute("INSERT INTO testByteDataType "
                            "VALUES (1, ?, ?, ?)",
                            (bytearray([0xAA, 0xBB, 0xCC, 0xDD]), "test",
                            bytearray([0xDD, 0xCC, 0xBB, 0xAA])))
            for row in conn.execute("SELECT * FROM testByteDataType "
                                    "WHERE id = 1"):
                self.assertEqual(
                    row.b, bytearray([0xAA, 0xBB, 0xCC, 0xDD]))
                self.assertEqual(row.c.strip(), "test")
                self.assertEqual(
                    row.d, bytearray([0xDD, 0xCC, 0xBB, 0xAA]))
            conn.execute("UPDATE testByteDataType SET b = ? WHERE c = ?",
                            (bytearray([0xAA, 0xAA, 0xAA, 0xAA]), "test"))
            for row in conn.execute("SELECT * FROM testByteDataType "
                                    "WHERE id = 1"):
                self.assertEqual(
                    row.b, bytearray([0xAA, 0xAA, 0xAA, 0xAA]))
                self.assertEqual(row.c.strip(), "test")
                self.assertEqual(
                    row.d, bytearray([0xDD, 0xCC, 0xBB, 0xAA]))

    def testNumberLimits(self):
        with udaExec.connect(
            self.dsn, username=self.username,
            password=self.password,
            dataTypeConverter=datatypes.DefaultDataTypeConverter(
                useFloat=True)) as conn:
            self.assertIsNotNone(conn)
            cursor = conn.execute("""CREATE TABLE testNumericLimits (
                id INTEGER,
                a BYTEINT,
                b SMALLINT,
                c INTEGER,
                d BIGINT,
                e FLOAT,
                f DECIMAL(38, 38))""")
            cursor.arraysize = 20
            params = []
            params.append((1, 2 ** 7 - 1, 2 ** 15 - 1, 2 ** 31 - 1,
                           2 ** 63 - 1, (float(2 ** 63 - 1)),
                           decimal.Decimal("-." + "1" * 37)))
            params.append((2, -2 ** 7, -2 ** 15, -2 ** 31, -2 ** 63,
                           float(-2 ** 63), decimal.Decimal("." + "1" * 37)))
            conn.executemany(
                "INSERT INTO testNumericLimits (?, ?, ?, ?, ?, ?, ?)",
                params)
            cursor = conn.execute(
                "SELECT * FROM testNumericLimits ORDER BY id")
            rowIndex = 0
            for r in cursor:
                colIndex = 0
                for col in r:
                    self.assertEqual(
                        col, params[rowIndex][colIndex])
                    colIndex += 1
                rowIndex += 1

    def testNumericDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute("""CREATE TABLE testNumericDataTypes (
                id INTEGER,
                a BYTEINT,
                b SMALLINT,
                c INTEGER,
                d BIGINT,
                e DECIMAL(6,1),
                f NUMERIC(7,2),
                g NUMBER,
                h FLOAT,
                i REAL,
                j DOUBLE PRECISION)""")
            conn.executemany(
                "INSERT INTO testNumericDataTypes (?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?)",
                params=[(i, decimal.Decimal(i), i, decimal.Decimal(i), i,
                         decimal.Decimal(i), i, i, i, i, decimal.Decimal(i))
                        for i in range(-128, 128)],
                batch=True)
            conn.execute(
                "INSERT INTO testNumericDataTypes VALUES (128, 99, 999, "
                "9999, 99999, 99999.9, 99999.99, 99999.999, 99999.9999, "
                "99999.99999, 99999.999999)")
            cursor = conn.execute(
                "SELECT * FROM testNumericDataTypes ORDER BY id")
            for row in cursor:
                if row.id < 128:
                    for col in row:
                        self.assertEqual(col, row.id)
                elif row.id == 128:
                    count = 1
                    for col in row:
                        if count == 1:
                            pass
                        elif count < 6:
                            self.assertEqual(col, 10 ** count - 1)
                        elif count < 9:
                            self.assertEqual(
                                col, decimal.Decimal("99999." + "9" *
                                                     (count - 5)))
                        else:
                            self.assertEqual(
                                col, float("99999." + "9" *
                                           (count - 5)))
                        count += 1

    def testFloatTypes(self):
        for useFloat in (False, True):
            with udaExec.connect(
                self.dsn, username=self.username,
                password=self.password,
                dataTypeConverter=datatypes.DefaultDataTypeConverter(
                    useFloat=useFloat)) as conn:
                self.assertIsNotNone(conn)
                conn.execute("""CREATE TABLE testFloatTypes (
                    id INTEGER,
                    a1 FLOAT,
                    a2 FLOAT,
                    b1 REAL,
                    b2 REAL,
                    c1 DOUBLE PRECISION,
                    c2 DOUBLE PRECISION)""")
                params = []
                paramCount = 5
                for i in range(2, paramCount):
                    f = i / (i - 1)
                    params.append(
                        [i, f, decimal.Decimal(str(f)), f, str(f), f,
                             decimal.Decimal(str(f))])
                params.append([paramCount, None, None, None, None, None, None])
                f = math.sqrt(3)
                self.assertEqual(f, decimal.Decimal(f))
                self.assertEqual(f, float(decimal.Decimal(f)))
                params.append([paramCount + 1, f, decimal.Decimal(str(f)), f, str(f), f, decimal.Decimal(str(f))])
                for batch in (False, True):
                    conn.executemany(
                        "INSERT INTO testFloatTypes (?, ?, ?, ?, ?, ?, ?)",
                        params, batch=batch)
                    count = 0
                    for row in conn.execute("SELECT * FROM testFloatTypes "
                                            "ORDER BY id"):
                        self.assertEqual(row.a1, params[count][1])
                        self.assertEqual(row.b1, params[count][1])
                        self.assertEqual(row.c1, params[count][1])
                        self.assertEqual(row.a1, row.a2)
                        self.assertEqual(row.b1, row.b2)
                        self.assertEqual(row.c1, row.c2)
                        if row.a1 is not None:
                            if useFloat:
                                self.assertTrue(isinstance(row.a1, float))
                            else:
                                self.assertTrue(isinstance(row.a1,
                                                           decimal.Decimal))
                        count += 1
                    conn.execute("DELETE FROM testFloatTypes")
                conn.execute("DROP TABLE testFloatTypes")

    def testDateAndTimeDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            with conn.cursor() as cursor:
                cursor.execute("""CREATE TABLE testDateAndTimeDataTypes (
                    id INT,
                    name VARCHAR(128),
                    "timestamp" TIMESTAMP,
                    timestampWithZone TIMESTAMP WITH TIME ZONE,
                    "time" TIME,
                    "timeWithZone" TIME WITH TIME ZONE,
                    "date" DATE,
                    timestamp3 TIMESTAMP(3))""")

                timestamp = datetime.datetime(2015, 5, 18, 12, 34, 56, 789000)
                timestampWithZone = datetime.datetime(
                    2015, 5, 18, 12, 34, 56, 789000,
                    datatypes.TimeZone("-", 5, 0))
                time = datetime.time(12, 34, 56, 789000)
                timeWithZone = datetime.time(
                    12, 34, 56, 789000, datatypes.TimeZone("+", 10, 30))
                date = datetime.date(2015, 5, 18)
                timestamp3 = datetime.datetime(2015, 5, 18, 12, 34, 56, 789000)

                cursor.execute(
                    "INSERT INTO testDateAndTimeDataTypes "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("1", "TEST1", "2015-05-18 12:34:56.789",
                     "2015-05-18 12:34:56.789-05:00",
                     "12:34:56.789",
                     "12:34:56.789+10:30", "2015-05-18",
                     "2015-05-18 12:34:56.789"))
                cursor.execute(
                    "INSERT INTO testDateAndTimeDataTypes "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (2, "TEST2", timestamp, timestampWithZone, time,
                     timeWithZone, date, str(timestamp3)[:-3]))
                cursor.execute(
                    "INSERT INTO testDateAndTimeDataTypes VALUES "
                    "(3, 'TEST3', '2015-05-18 12:34:56.789', "
                    "'2015-05-18 12:34:56.789-05:00', '12:34:56.789', "
                    "'12:34:56.789+10:30', '2015-05-18', "
                    "'2015-05-18 12:34:56.789')")
                rowId = 0
                for row in cursor.execute("SELECT * FROM "
                                          "testDateAndTimeDataTypes "
                                          "ORDER BY id"):
                    rowId += 1
                    self.assertEqual(row.id, rowId)
                    self.assertEqual(row.name, "TEST" + str(rowId))
                    count = 0
                    for t in (row.timestamp, row.timestampWithZone, row.time,
                              row.timeWithZone, row.date, row.timestamp3):
                        if count not in (2, 3):
                            self.assertEqual(t.year, 2015)
                            self.assertEqual(t.month, 5)
                            self.assertEqual(t.day, 18)
                        if count != 4:
                            if count != 1:
                                self.assertEqual(t.hour, 12,
                                                 "Count is {}".format(count))
                            self.assertEqual(t.minute, 34)
                            self.assertEqual(t.second, 56)
                            self.assertEqual(t.microsecond, 789000)
                        count += 1
                    self.assertEqual(
                        row.timestampWithZone.tzinfo.utcoffset(None),
                        datetime.timedelta(hours=-5))
                    self.assertEqual(row.timeWithZone.tzinfo.utcoffset(
                        None), datetime.timedelta(hours=10, minutes=30))
                    self.assertEqual(
                        row.timestampWithZone, timestampWithZone)
                    self.assertEqual(row.timeWithZone, timeWithZone)
                    self.assertEqual(row.timestamp, timestamp)
                    self.assertEqual(row.time, time)
                    self.assertEqual(row.date, date)
                self.assertEqual(rowId, 3)

    def testIntervalDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute("""CREATE TABLE testIntervalDataTypes (
                id INT,
                "year" INTERVAL YEAR,
                "yearToMonth" INTERVAL YEAR TO MONTH,
                "month" INTERVAL MONTH,
                "day" INTERVAL DAY,
                "dayToHour" INTERVAL DAY TO HOUR,
                "dayToMinute" INTERVAL DAY TO MINUTE,
                "dayToSecond" INTERVAL DAY TO SECOND,
                "hour" INTERVAL HOUR,
                "hourToMinute" INTERVAL HOUR TO MINUTE,
                "hourToSecond" INTERVAL HOUR TO SECOND,
                "minute" INTERVAL MINUTE,
                "minuteToSecond" INTERVAL MINUTE TO SECOND,
                "second" INTERVAL SECOND)""")

            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (1, '1', '1-01', "
                "'1', '1', '1 01', '1 01:01', '1 01:01:01.01', '1', '01:01', "
                "'01:01:01', '1', '01:01', '1.01')")
            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?)",
                ['2', '-2', '-2-02', '-2', '-2', '-2 02', '-2 02:02',
                 '-2 02:02:02.02', '-2', '-02:02', '-02:02:02', '-2',
                 '-02:02', '-2.02'])
            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [3, datatypes.Interval(years=3),
                 datatypes.Interval(years=3, months=3),
                 datatypes.Interval(months=3),
                 datatypes.Interval(days=3),
                 datatypes.Interval(days=3, hours=3),
                 datatypes.Interval(days=3, hours=3, minutes=3),
                 datatypes.Interval(days=3, hours=3, minutes=3, seconds=3.03),
                 datatypes.Interval(hours=3),
                 datatypes.Interval(hours=3, minutes=3),
                 datatypes.Interval(hours=3, minutes=3, seconds=3),
                 datatypes.Interval(minutes=3),
                 datatypes.Interval(minutes=3, seconds=3),
                 datatypes.Interval(seconds=3.03)])
            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?)",
                [4, datatypes.Interval(negative=True, years=4),
                 datatypes.Interval(negative=True, years=4, months=4),
                 datatypes.Interval(negative=True, months=4),
                 datatypes.Interval(negative=True, days=4),
                 datatypes.Interval(negative=True, days=4, hours=4),
                 datatypes.Interval(negative=True, days=4, hours=4, minutes=4),
                 datatypes.Interval(negative=True, days=4, hours=4,
                                    minutes=4, seconds=4.04),
                 datatypes.Interval(negative=True, hours=4),
                 datatypes.Interval(negative=True, hours=4, minutes=4),
                 datatypes.Interval(negative=True, hours=4, minutes=4,
                                    seconds=4),
                 datatypes.Interval(negative=True, minutes=4),
                 datatypes.Interval(negative=True, minutes=4, seconds=4),
                 datatypes.Interval(negative=True, seconds=4.04)])

            cursor = conn.execute(
                "SELECT * FROM testIntervalDataTypes ORDER BY id")
            for row in cursor:
                self.assertEqual(
                    row.year, datatypes.Interval(negative=row.id % 2 == 0,
                                                 years=row.id))
                self.assertEqual(row.yearToMonth, datatypes.Interval(
                    negative=row.id % 2 == 0, years=row.id, months=row.id))
                self.assertEqual(
                    row.month, datatypes.Interval(negative=row.id % 2 == 0,
                                                  months=row.id))
                self.assertEqual(
                    row.day, datatypes.Interval(negative=row.id % 2 == 0,
                                                days=row.id))
                self.assertEqual(row.dayToHour, datatypes.Interval(
                    negative=row.id % 2 == 0, days=row.id, hours=row.id))
                self.assertEqual(row.dayToMinute, datatypes.Interval(
                    negative=row.id % 2 == 0, days=row.id, hours=row.id,
                    minutes=row.id))
                self.assertEqual(row.dayToSecond, datatypes.Interval(
                    negative=row.id % 2 == 0, days=row.id, hours=row.id,
                    minutes=row.id,
                    seconds=float("{}.0{}".format(row.id, row.id))))
                self.assertEqual(
                    row.hour, datatypes.Interval(negative=row.id % 2 == 0,
                                                 hours=row.id))
                self.assertEqual(row.hourToMinute, datatypes.Interval(
                    negative=row.id % 2 == 0, hours=row.id, minutes=row.id))
                self.assertEqual(row.hourToSecond, datatypes.Interval(
                    negative=row.id % 2 == 0, hours=row.id, minutes=row.id,
                    seconds=row.id))
                self.assertEqual(
                    row.minute, datatypes.Interval(negative=row.id % 2 == 0,
                                                   minutes=row.id))
                self.assertEqual(row.minuteToSecond, datatypes.Interval(
                    negative=row.id % 2 == 0, minutes=row.id, seconds=row.id))
                self.assertEqual(row.second, datatypes.Interval(
                    negative=row.id % 2 == 0,
                    seconds=float("{}.0{}".format(row.id, row.id))))
                for col in row:
                    if isinstance(col, datatypes.Interval):
                        try:
                            delta = col.timedelta()
                            if col.years or col.months:
                                self.fail(
                                    "Exception not thrown by timedelta() "
                                    "for years/months interval.")
                            if col.days:
                                self.assertEqual(abs(delta).days, col.days)
                        except teradata.InterfaceError as e:
                            if col.years or col.months:
                                # THis is expected.
                                pass
                            else:
                                raise e

            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (9, '99', '99-11', "
                "'99', '99', '99 23', '99 23:59', '99 23:59:59.999999', '99', "
                "'99:59', '99:59:59.999999', '99', '99:59.999999', "
                "'99.999999')")
            row1 = conn.execute(
                "SELECT * FROM testIntervalDataTypes WHERE id = 9").fetchone()
            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?)",
                [10 if col == 9 else col for col in row1])
            row2 = conn.execute(
                "SELECT * FROM testIntervalDataTypes "
                "WHERE id = 10").fetchone()
            for row in (row1, row2):
                self.assertEqual(
                    row.year, datatypes.Interval(years=99), row.year)
                self.assertEqual(
                    row.yearToMonth, datatypes.Interval(years=99, months=11),
                    row.yearToMonth)
                self.assertEqual(row.month, datatypes.Interval(months=99))
                self.assertEqual(row.day, datatypes.Interval(days=99))
                self.assertEqual(
                    row.dayToHour, datatypes.Interval(days=99, hours=23))
                self.assertEqual(
                    row.dayToMinute, datatypes.Interval(days=99, hours=23,
                                                        minutes=59))
                self.assertEqual(row.dayToSecond, datatypes.Interval(
                    days=99, hours=23, minutes=59, seconds=59.999999))
                self.assertEqual(row.hour, datatypes.Interval(hours=99))
                self.assertEqual(
                    row.hourToMinute, datatypes.Interval(hours=99, minutes=59))
                self.assertEqual(
                    row.hourToSecond, datatypes.Interval(hours=99, minutes=59,
                                                         seconds=59.999999))
                self.assertEqual(row.minute, datatypes.Interval(minutes=99))
                self.assertEqual(
                    row.minuteToSecond, datatypes.Interval(minutes=99,
                                                           seconds=59.999999))
                self.assertEqual(
                    row.second, datatypes.Interval(seconds=99.999999))

            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (11, "
                "INTERVAL '29' MONTH , INTERVAL '13' MONTH, "
                "INTERVAL '99' MONTH, INTERVAL '106' HOUR, "
                "INTERVAL '199' MINUTE, INTERVAL '99' MINUTE, "
                "INTERVAL '9999.999999' SECOND, INTERVAL '800.88' SECOND, "
                "INTERVAL '77.77' SECOND, "
                "INTERVAL '107:59.999999' MINUTE TO SECOND, "
                "INTERVAL '500' SECOND, "
                "INTERVAL '500.55' SECOND, '99.999999')")
            row1 = conn.execute(
                "SELECT * FROM testIntervalDataTypes "
                "WHERE id = 11").fetchone()
            conn.execute(
                "INSERT INTO testIntervalDataTypes VALUES (?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?)",
                [12 if col == 11 else col for col in row1])
            row2 = conn.execute(
                "SELECT * FROM testIntervalDataTypes "
                "WHERE id = 12").fetchone()
            for row in (row1, row2):
                self.assertEqual(
                    row.year, datatypes.Interval(years=2), row.year)
                self.assertEqual(
                    row.yearToMonth, datatypes.Interval(years=1, months=1),
                    row.yearToMonth)
                self.assertEqual(row.month, datatypes.Interval(months=99))
                self.assertEqual(row.day, datatypes.Interval(days=4))
                self.assertEqual(
                    row.dayToHour, datatypes.Interval(days=0, hours=3))
                self.assertEqual(
                    row.dayToMinute, datatypes.Interval(days=0, hours=1,
                                                        minutes=39))
                self.assertEqual(row.dayToSecond, datatypes.Interval(
                    days=0, hours=2, minutes=46, seconds=39.999999))
                self.assertEqual(row.hour, datatypes.Interval(hours=0))
                self.assertEqual(
                    row.hourToMinute, datatypes.Interval(hours=0, minutes=1))
                self.assertEqual(
                    row.hourToSecond,
                    datatypes.Interval(hours=1, minutes=47, seconds=59.999999))
                self.assertEqual(row.minute, datatypes.Interval(minutes=8))
                self.assertEqual(
                    row.minuteToSecond,
                    datatypes.Interval(minutes=8, seconds=20.55))
                self.assertEqual(
                    row.second, datatypes.Interval(seconds=99.999999))

    def testArrayDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            udaExec.config["arrayPrefix"] = self.username
            while True:
                try:
                    conn.execute(
                        "DROP TYPE ${arrayPrefix}_test_int_array",
                        ignoreErrors=[6831])
                    conn.execute(
                        "CREATE TYPE ${arrayPrefix}_test_int_array AS "
                        "INTEGER ARRAY [2][4][3]""")
                    break
                except teradata.DatabaseError as e:
                    if e.code == 3598:
                        continue
                    raise e
            conn.execute("CREATE TABLE testArrayDataTypes (id INT, "
                         "integerArray ${arrayPrefix}_test_int_array)")
            conn.execute(
                "INSERT INTO testArrayDataTypes VALUES (1, "
                "NEW ${arrayPrefix}_test_int_array (11, 12, 13, 21, 22, 23, "
                "31, 32, 33, 41, 42, 43, 51, 52, 53, 61, 62, 63, 71, 72, 73, "
                "81, 82, 83))")
            cursor = conn.execute(
                "SELECT * FROM testArrayDataTypes ORDER BY id")
            for row in cursor:
                self.assertEqual(
                    row.integerArray, "(11,12,13,21,22,23,31,32,33,41,42,"
                    "43,51,52,53,61,62,63,71,72,73,81,82,83)")

    def testJSONDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            self.assertIsNotNone(conn)
            version = conn.execute(
                "SELECT InfoData FROM DBC.DBCInfo "
                "WHERE InfoKey = 'VERSION'").fetchone()[0]
            if version < '15.00.00.00':
                return self.skipTest("JSON Data types are only supported for "
                                     "15.0 and above.")
            conn.execute("CREATE TABLE testJSONDataTypes (id INT, "
                         "data JSON(1024), data2 JSON(1024))")
            data = {}
            data2 = {}
            data['object1'] = data2
            data['field1'] = 'value1'
            data['field2'] = 777
            data2['field1'] = ['value2', 'value3', 'value4']
            data2['field2'] = 1010
            jsonData = json.dumps(data)
            conn.execute(
                "INSERT INTO testJSONDataTypes VALUES (1, '" +
                jsonData + "', NULL)")
            conn.execute(
                "INSERT INTO testJSONDataTypes VALUES (?, ?, ?)",
                (2, jsonData, None))
            conn.execute("INSERT INTO testJSONDataTypes VALUES (?, ?, ?)",
                         (3, json.dumps(list([i for i in range(0, 100)])),
                          None))
            for row in conn.execute("SELECT * FROM testJSONDataTypes "
                                    "where id in (1, 2) ORDER BY id"):
                self.assertEqual(row.data, data)
                self.assertEqual(row.data['object1'], data2)
                self.assertEqual(row.data['field1'], 'value1')
                self.assertEqual(row.data['field2'], 777)
                self.assertEqual(row.data['object1']['field1'][1], 'value3')
                self.assertIsNone(row.data2)
            for row in conn.execute("SELECT * FROM testJSONDataTypes "
                                    "where id = 3 ORDER BY id"):
                self.assertEqual(row.data, list(i for i in range(0, 100)))
                self.assertIsNone(row.data2)

    def testPeriodDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute("""CREATE TABLE testPeriodDataTypes (id INTEGER,
                a PERIOD(DATE),
                b PERIOD(DATE) FORMAT 'YYYY-MM-DD',
                c PERIOD(DATE) FORMAT 'YYYYMMDD',
                d PERIOD(TIMESTAMP),
                e PERIOD(TIMESTAMP WITH TIME ZONE),
                f PERIOD(TIME),
                g PERIOD(TIME WITH TIME ZONE))""")

            period = datatypes.Period(
                datetime.date(1980, 4, 10), datetime.date(2015, 7, 2))
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, a, b, c) VALUES "
                "(1, PERIOD(DATE '1980-04-10', DATE '2015-07-02'), "
                "'(1980-04-10, 2015-07-02)',"
                "'(1980-04-10, 2015-07-02))')")
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, a, b, c) VALUES "
                "(2, ?, ?, ?)",
                ("('1980-04-10', '2015-07-02')",
                    '(1980-04-10, 2015-07-02)',
                    '(1980-04-10, 2015-07-02)'))
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, a, b, c) VALUES "
                "(3, ?, ?, ?)", (period, period, period))
            for row in conn.execute("SELECT * FROM testPeriodDataTypes "
                                    "WHERE id IN (1,2,3) ORDER BY id"):
                self.assertEqual(row.a, period)
                self.assertEqual(row.b, period)
                self.assertEqual(row.c, period)

            periodWithZone = datatypes.Period(
                datetime.datetime(1980, 4, 10, 23, 45, 15, 0,
                                    datatypes.TimeZone("+", 0, 0)),
                datetime.datetime(2015, 7, 2, 17, 36, 33, 0,
                                    datatypes.TimeZone("+", 0, 0)))
            periodWithoutZone = datatypes.Period(
                datetime.datetime(1980, 4, 10, 23, 45, 15),
                datetime.datetime(2015, 7, 2, 17, 36, 33))
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, d, e) VALUES "
                "(4, PERIOD(TIMESTAMP '1980-04-10 23:45:15', "
                "TIMESTAMP '2015-07-02 17:36:33'), "
                "'(1980-04-10 23:45:15+00:00, "
                "2015-07-02 17:36:33+00:00)')")
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, d, e) VALUES "
                "(5, ?, ?)", (periodWithoutZone, periodWithZone))
            for row in conn.execute("SELECT * FROM testPeriodDataTypes "
                                    "WHERE id IN (4,5) ORDER BY id"):
                self.assertEqual(
                    row.d, periodWithoutZone, str(row.d) + "!=" +
                    str(periodWithoutZone))
                self.assertEqual(
                    row.e, periodWithZone, str(row.e) + "!=" +
                    str(periodWithZone))

            timeWithZone = datatypes.Period(
                datetime.time(17, 36, 33, 0,
                                datatypes.TimeZone("+", 0, 0)),
                datetime.time(23, 45, 15, 0,
                                datatypes.TimeZone("+", 0, 0)))
            timeWithoutZone = datatypes.Period(
                datetime.time(17, 36, 33), datetime.time(23, 45, 15))
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, f, g) VALUES "
                "(6, PERIOD(TIME '17:36:33', TIME '23:45:15'), "
                "'(17:36:33+00:00, 23:45:15+00:00)')")
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, f, g) VALUES "
                "(7, ?, ?)", (timeWithoutZone, timeWithZone))
            for row in conn.execute("SELECT * FROM testPeriodDataTypes "
                                    "WHERE id IN (6,7) ORDER BY id"):
                self.assertEqual(
                    row.f, timeWithoutZone, str(row.f) + "!=" +
                    str(timeWithoutZone))
                self.assertEqual(
                    row.g, timeWithZone, str(row.g) + "!=" +
                    str(timeWithZone))

            periodUntilChange = datatypes.Period(
                datetime.date(1980, 4, 10), datetime.date(9999, 12, 31))
            conn.execute(
                "INSERT INTO testPeriodDataTypes (id, a, b, c) VALUES "
                "(8, PERIOD(DATE '1980-04-10', UNTIL_CHANGED), "
                "PERIOD(DATE '1980-04-10', UNTIL_CHANGED), NULL)")
            for row in conn.execute("SELECT * FROM testPeriodDataTypes "
                                    "WHERE id IN (8) ORDER BY id"):
                self.assertEqual(row.a, periodUntilChange)
                self.assertEqual(row.b, periodUntilChange)
                self.assertIsNone(row.c)

    def testLargeTestView(self):
        with udaExec.connect(self.dsn, username=self.username,
                             password=self.password) as conn:
            scriptFile = os.path.join(
                os.path.dirname(__file__), "testlargeview.sql")
            conn.execute(file=scriptFile)
            view = conn.execute("SHOW VIEW LARGE_TEST_VIEW").fetchone()[0]
            self.assertEqual(len(view), 30398)

    def testBatchPeriodIntervalDataTypes(self):
        with udaExec.connect(self.dsn, username=self.username,
                                password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute("""CREATE TABLE testBatchPeriodIntervalDataTypes (
                id INTEGER,
                a  PERIOD(DATE),
                b  PERIOD(TIMESTAMP),
                c  PERIOD(TIMESTAMP WITH TIME ZONE),
                d  PERIOD(TIME),
                e  PERIOD(TIME WITH TIME ZONE),
                f  INTERVAL YEAR TO MONTH,
                g  INTERVAL MONTH,
                h  INTERVAL DAY TO SECOND,
                i  INTERVAL HOUR TO SECOND,
                j  INTERVAL MINUTE TO SECOND)""")

            d = datetime.date
            t = datetime.time
            dt = datetime.datetime
            tz = datatypes.TimeZone
            period = datatypes.Period
            interval = datatypes.Interval

            periodDate = period(d(1995, 4, 15), d(2020, 7, 15))

            periodTS = period(
                dt(1970, 1, 2, 3,  4,  5, 100000),
                dt(1976, 7, 8, 9, 10, 11, 900000))

            periodTSWithTZ = period (
                dt(1970, 1, 2, 3, 4, 5,  123000, tz("+", 5, 30)),
                dt(1976, 7, 8, 9, 10, 11, 123000, tz("+", 5, 30)))

            periodTime = period(
                t(11, 22, 33, 234560),
                t(22, 33, 44, 345600))

            periodTimeWithTZ = period(
                t( 3,  4,  5, 600000, tz("+", 0, 30)),
                t(12, 13, 14, 140000, tz("+", 5, 30)))

            intervalYearToMonth = interval(years=3, months=3)
            intervalMonth = interval(months=3)
            intervalDayToSec = interval(days=3, hours=3, minutes=3, seconds=3.03)
            intervalHourToSec = interval(hours=3, minutes=3, seconds=3)
            intervalMinToSec = interval(minutes=3, seconds=3)


            conn.executemany(
                "INSERT INTO testBatchPeriodIntervalDataTypes "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [[
                    1,
                    periodDate,
                    periodTS,
                    periodTSWithTZ,
                    periodTime,
                    periodTimeWithTZ,
                    intervalYearToMonth,
                    intervalMonth,
                    intervalDayToSec,
                    intervalHourToSec,
                    intervalMinToSec
                ],[
                    2,
                    periodDate,
                    periodTS,
                    periodTSWithTZ,
                    periodTime,
                    periodTimeWithTZ,
                    intervalYearToMonth,
                    intervalMonth,
                    intervalDayToSec,
                    intervalHourToSec,
                    intervalMinToSec

                ],[
                    3,
                    periodDate,
                    periodTS,
                    periodTSWithTZ,
                    periodTime,
                    periodTimeWithTZ,
                    intervalYearToMonth,
                    intervalMonth,
                    intervalDayToSec,
                    intervalHourToSec,
                    intervalMinToSec

                ]], batch=True)

            conn.executemany(
                "INSERT INTO testBatchPeriodIntervalDataTypes "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [[
                    4,
                    periodDate,
                    periodTS,
                    periodTSWithTZ,
                    periodTime,
                    periodTimeWithTZ,
                    intervalYearToMonth,
                    intervalMonth,
                    intervalDayToSec,
                    intervalHourToSec,
                    intervalMinToSec
                ],[
                    5,
                    periodDate,
                    periodTS,
                    periodTSWithTZ,
                    periodTime,
                    periodTimeWithTZ,
                    intervalYearToMonth,
                    intervalMonth,
                    intervalDayToSec,
                    intervalHourToSec,
                    intervalMinToSec

                ],[
                    6,
                    periodDate,
                    periodTS,
                    periodTSWithTZ,
                    periodTime,
                    periodTimeWithTZ,
                    intervalYearToMonth,
                    intervalMonth,
                    intervalDayToSec,
                    intervalHourToSec,
                    intervalMinToSec

                ]], batch=False)

            nRowNum = 1
            for row in conn.execute("SELECT * FROM testBatchPeriodIntervalDataTypes ORDER BY id"):
                self.assertEqual(row.id, nRowNum)
                self.assertEqual(row.a,  periodDate, str(row.a) + "!=" + str(periodDate))
                self.assertEqual(row.b,  periodTS, str(row.b) + "!=" + str(periodTS))
                self.assertEqual(row.c,  periodTSWithTZ, str(row.c) + "!=" + str(periodTSWithTZ))
                self.assertEqual(row.d,  periodTime, str(row.d) + "!=" + str(periodTime))
                self.assertEqual(row.e,  periodTimeWithTZ, str(row.e) + "!=" + str(periodTimeWithTZ))
                self.assertEqual(row.f,  intervalYearToMonth, str(row.f) + "!=" + str(intervalYearToMonth))
                self.assertEqual(row.g,  intervalMonth, str(row.g) + "!=" + str(intervalMonth))
                self.assertEqual(row.h,  intervalDayToSec, str(row.h) + "!=" + str(intervalDayToSec))
                self.assertEqual(row.i,  intervalHourToSec, str(row.i) + "!=" + str(intervalHourToSec))
                self.assertEqual(row.j,  intervalMinToSec, str(row.j) + "!=" + str(intervalMinToSec))
                nRowNum += 1
        #end testBatchPeriodDataTypes

    def testProcedurePeriodNulls(self):
        with udaExec.connect(self.dsn, username=self.username,
                              password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                """REPLACE PROCEDURE testProcedurePeriodNulls
                    (
                        in p1   period (date)                        , inout p2   period (date)                        , out p3   period (date)                        ,
                        in p4   period (time(0))                     , inout p5   period (time(0))                     , out p6   period (time(0))                     ,
                        in p7   period (time)                        , inout p8   period (time)                        , out p9   period (time)                        ,
                        in p10  period (time(0) with time zone)      , inout p11  period (time(0) with time zone)      , out p12  period (time(0) with time zone)      ,
                        in p13  period (time (4) with time zone)     , inout p14  period (time (4) with time zone)     , out p15  period (time (4) with time zone)     ,
                        in p16  period (timestamp (0))               , inout p17  period (timestamp (0))               , out p18  period (timestamp (0))               ,
                        in p19  period (timestamp)                   , inout p20  period (timestamp)                   , out p21  period (timestamp)                   ,
                        in p22  period (timestamp (0) with time zone), inout p23  period (timestamp (0) with time zone), out p24  period (timestamp (0) with time zone),
                        in p25  period (timestamp (3) with time zone), inout p26  period (timestamp (3) with time zone), out p27  period (timestamp (3) with time zone)
                    )  begin
                            set p3   = p2   ; set p2   = p1   ;
                            set p6   = p5   ; set p5   = p4   ;
                            set p9   = p8   ; set p8   = p7   ;
                            set p12  = p11  ; set p11  = p10  ;
                            set p15  = p14  ; set p14  = p13  ;
                            set p18  = p17  ; set p17  = p16  ;
                            set p21  = p20  ; set p20  = p19  ;
                            set p24  = p23  ; set p23  = p22  ;
                            set p27  = p26  ; set p26  = p25  ;
                    END;""")

            try:
                d = datetime.date
                t = datetime.time
                dt = datetime.datetime
                tz = datatypes.TimeZone
                period = datatypes.Period

                aaoParameters = [
                  [ # Use period types in IN/INOUT parameters and cast as PERIOD types
                    teradata.InParam (None, dataType='PERIOD (DATE)'),                                                                                                                                                # p1 period(date)
                    teradata.InOutParam(period(d(2000, 12, 22),d(2008, 10, 27)), "p2", dataType='PERIOD (DATE)'),                                                                                                     # p2 period(date)
                    teradata.OutParam ("p3", dataType="PERIOD (DATE)"),                                                                                                                                               # p3 period(date)

                    teradata.InParam (period (t (9, 9, 9), t (10, 10, 10)), dataType='PERIOD (TIME (0))'),                                                                                                            # p4 period(time (0))
                    teradata.InOutParam(None, "p5", dataType = 'PERIOD (TIME (0))'),                                                                                                                                  # p5 period(time (0))
                    teradata.OutParam ("p6", dataType = 'PERIOD (TIME (0))'),                                                                                                                                         # p6 period(time (0))

                    teradata.InParam(None, dataType='PERIOD (TIME)'),                                                                                                                                                 # p7 period(time)
                    teradata.InOutParam(period(t(8, 45, 59, 500600), t(12, 10, 45, 123000)), "p8", dataType = 'PERIOD (TIME)'),                                                                                       # p8 period(time)
                    teradata.OutParam("p9", dataType = 'PERIOD (TIME)'),                                                                                                                                              # p9 period(time)

                    teradata.InParam(period(t (2, 12, 12, 0, tz("+", 0, 30)), t (22, 3, 44, 0, tz("+", 5, 30))), dataType='PERIOD (TIME (0) WITH TIME ZONE)'),                                                        #p10 period(time)
                    teradata.InOutParam(None, "p11", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                                                                                                                  #p11 period(time)
                    teradata.OutParam("p12", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                                                                                                                          #p12 period(time)

                    teradata.InParam(period(t (3, 4, 5, 60000, tz("+", 0, 30)), t (12, 13, 14, 561000, tz ("+", 5, 30))), dataType='PERIOD (TIME (4) WITH TIME ZONE)'),                                               #p13 period(time (4) with time zone)
                    teradata.InOutParam(None, "p14", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                                                                                                                  #p14 period(time (4) with time zone)
                    teradata.OutParam("p15", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                                                                                                                          #p15 period(time (4) with time zone)

                    teradata.InParam(None, dataType='PERIOD (TIMESTAMP (0))'),                                                                                                                                        #p16 period(time (0) with time zone)
                    teradata.InOutParam(period(dt(1980, 5, 3, 3, 4, 5), dt (1986, 8, 7, 1, 10, 11)), "p17", dataType = 'PERIOD (TIMESTAMP (0))'),                                                                     #p17 period(time (0) with time zone)
                    teradata.OutParam("p18", dataType = 'PERIOD (TIMESTAMP (0))'),                                                                                                                                    #p18 period(time (0) with time zone)

                    teradata.InParam(period(dt(1981, 6, 4,  4,  5,  6,  456000), dt(1986, 7, 8, 11, 10, 11, 135600)), dataType = 'PERIOD (TIMESTAMP)'),                                                               #p19 period(timestamp with time zone)
                    teradata.InOutParam(None, "p20", dataType = 'PERIOD (TIMESTAMP)'),                                                                                                                                #p20 period(timestamp with time zone)
                    teradata.OutParam("p21", dataType = 'PERIOD (TIMESTAMP)'),                                                                                                                                        #p21 period(timestamp with time zone)

                    teradata.InParam(period(dt(2000,  1,  1,  0,  1,  5, 0, tz("+", 5, 30)), dt(2000, 12, 31, 11, 59,  0, 0, tz("+", 5, 30))), dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                   #p22 period(timestamp (0) with time zone)
                    teradata.InOutParam(None, "p23", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                                                                                                             #p23 period(timestamp (0) with time zone)
                    teradata.OutParam("p24", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                                                                                                                     #p24 period(timestamp (0) with time zone)

                    teradata.InParam(None, dataType='PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),                                                                                                                         #p25 period(timestamp (3) with time zone)
                    teradata.InOutParam(period(dt(2003, 10, 27, 8, 10, 30, 123000, tz("+", 5, 30)), dt(2019, 5, 6, 10, 21, 0, 560000, tz ("+", 5, 30))), "p26", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),  #p26 period(timestamp (3) with time zone)
                    teradata.OutParam("p27", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),                                                                                                                     #p27 period(timestamp (3) with time zone)

                ],[ # Same values as above but use string to represent periods and cast as period type
                    teradata.InParam (None, dataType='PERIOD (DATE)'),                                                                                             # p1 period(date)
                    teradata.InOutParam("2000-12-22,2008-10-27", "p2", dataType='PERIOD (DATE)'),                                                                  # p2 period(date)
                    teradata.OutParam ("p3", dataType="PERIOD (DATE)"),                                                                                            # p3 period(date)

                    teradata.InParam ("09:09:09,10:10:10", dataType='PERIOD (TIME (0))'),                                                                          # p4 period(time (0))
                    teradata.InOutParam(None, "p5", dataType = 'PERIOD (TIME (0))'),                                                                               # p5 period(time (0))
                    teradata.OutParam ("p6", dataType = 'PERIOD (TIME (0))'),                                                                                      # p6 period(time (0))

                    teradata.InParam (None, dataType='PERIOD (TIME)'),                                                                                             # p7 period(time)
                    teradata.InOutParam("08:45:59.5006,12:10:45.123", "p8", dataType = 'PERIOD (TIME)'),                                                           # p8 period(time)
                    teradata.OutParam ("p9", dataType = 'PERIOD (TIME)'),                                                                                          # p9 period(time)

                    teradata.InParam ("02:12:12+00:30,22:03:44+05:30", dataType='PERIOD (TIME (0) WITH TIME ZONE)'),                                               #p10 period(time)
                    teradata.InOutParam(None, "p11", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                                                               #p11 period(time)
                    teradata.OutParam ("p12", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                                                                      #p12 period(time)

                    teradata.InParam ("03:04:05.06+00:30,12:13:14.561+05:30", dataType='PERIOD (TIME (4) WITH TIME ZONE)'),                                        #p13 period(time (4) with time zone)
                    teradata.InOutParam(None, "p14", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                                                               #p14 period(time (4) with time zone)
                    teradata.OutParam ("p15", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                                                                      #p15 period(time (4) with time zone)

                    teradata.InParam (None, dataType='PERIOD (TIMESTAMP (0))'),                                                                                    #p16 period(time (0) with time zone)
                    teradata.InOutParam("1980-05-03 03:04:05,1986-08-07 01:10:11", "p17", dataType = 'PERIOD (TIMESTAMP (0))'),                                    #p17 period(time (0) with time zone)
                    teradata.OutParam ("p18", dataType = 'PERIOD (TIMESTAMP (0))'),                                                                                #p18 period(time (0) with time zone)

                    teradata.InParam ("1981-06-04 04:05:06.456000,1986-07-08 11:10:11.135600", dataType = 'PERIOD (TIMESTAMP)'),                                   #p19 period(timestamp with time zone)
                    teradata.InOutParam(None, "p20", dataType = 'PERIOD (TIMESTAMP)'),                                                                             #p20 period(timestamp with time zone)
                    teradata.OutParam ("p21", dataType = 'PERIOD (TIMESTAMP)'),                                                                                    #p21 period(timestamp with time zone)

                    teradata.InParam ("2000-01-01 00:01:05+05:30,2000-12-31 11:59:00+05:30", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                  #p22 period(timestamp (0) with time zone)
                    teradata.InOutParam(None, "p23", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                                                          #p23 period(timestamp (0) with time zone)
                    teradata.OutParam ("p24", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                                                                 #p24 period(timestamp (0) with time zone)

                    teradata.InParam (None, dataType='PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),                                                                     #p25 period(timestamp (3) with time zone)
                    teradata.InOutParam("2003-10-27 08:10:30.123+05:30,2019-05-06 10:21:00.56+05:30", "p26", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),  #p26 period(timestamp (3) with time zone)
                    teradata.OutParam ("p27", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),                                                                 #p27 period(timestamp (3) with time zone)
                ]]

                for i in range (len (aaoParameters)):
                    result = conn.callproc("testProcedurePeriodNulls", aaoParameters [i])
                    self.assertEqual (len (result), len (aaoParameters [i]))
                    nParam = 2
                    for p in range (0, int (len (result) * 2/3)):
                        # Output is returned as period types so always compare against 1st param set
                        self.assertEqual (result ["p{}".format(nParam)], aaoParameters [0][nParam - 2].inValue)
                        nParam += (p + 2) % 2 + 1
            finally:
                conn.execute ("DROP PROCEDURE testProcedurePeriodNulls")
        # end testProcedurePeriodNulls

    def testProcedureIntervalNulls(self):
        with udaExec.connect(self.dsn, username=self.username,
                              password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                """REPLACE PROCEDURE testProcedureIntervalNulls
                    (
                      in p1   interval year                  , inout p2   interval year                  , out p3   interval year,
                      in p4   interval year(4)               , inout p5   interval year(4)               , out p6   interval year(4),
                      in p7   interval year to month         , inout p8   interval year to month         , out p9   interval year to month,
                      in p10  interval year(4) to month      , inout p11  interval year(4) to month      , out p12  interval year(4) to month,
                      in p13  interval month                 , inout p14  interval month                 , out p15  interval month ,
                      in p16  interval month(4)              , inout p17  interval month(4)              , out p18  interval month(4),
                      in p19  interval day                   , inout p20  interval day                   , out p21  interval day,
                      in p22  interval day(4)                , inout p23  interval day(4)                , out p24  interval day(4),
                      in p25  interval day to hour           , inout p26  interval day to hour           , out p27  interval day to hour,
                      in p28  interval day(4) to hour        , inout p29  interval day(4) to hour        , out p30  interval day(4) to hour,
                      in p31  interval day to minute         , inout p32  interval day to minute         , out p33  interval day to minute,
                      in p34  interval day(4) to minute      , inout p35  interval day(4) to minute      , out p36  interval day(4) to minute,
                      in p37  interval day to second(0)      , inout p38  interval day to second(0)      , out p39  interval day to second(0),
                      in p40  interval day to second         , inout p41  interval day to second         , out p42  interval day to second,
                      in p43  interval day(4) to second(0)   , inout p44  interval day(4) to second(0)   , out p45  interval day(4) to second(0),
                      in p46  interval day(4) to second      , inout p47  interval day(4) to second      , out p48  interval day(4) to second,
                      in p49  interval hour                  , inout p50  interval hour                  , out p51  interval hour,
                      in p52  interval hour(4)               , inout p53  interval hour(4)               , out p54  interval hour(4),
                      in p55  interval hour to minute        , inout p56  interval hour to minute        , out p57  interval hour to minute,
                      in p58  interval hour(4) to minute     , inout p59  interval hour(4) to minute     , out p60  interval hour(4) to minute,
                      in p61  interval hour to second(0)     , inout p62  interval hour to second(0)     , out p63  interval hour to second(0),
                      in p64  interval hour to second        , inout p65  interval hour to second        , out p66  interval hour to second,
                      in p67  interval hour(4) to second(0)  , inout p68  interval hour(4) to second(0)  , out p69  interval hour(4) to second(0),
                      in p70  interval hour(4) to second     , inout p71  interval hour(4) to second     , out p72  interval hour(4) to second,
                      in p73  interval minute                , inout p74  interval minute                , out p75  interval minute,
                      in p76  interval minute(4)             , inout p77  interval minute(4)             , out p78  interval minute(4),
                      in p79  interval minute to second(0)   , inout p80  interval minute to second(0)   , out p81  interval minute to second(0),
                      in p82  interval minute to second      , inout p83  interval minute to second      , out p84  interval minute to second,
                      in p85  interval minute(4) to second(0), inout p86  interval minute(4) to second(0), out p87  interval minute(4) to second(0),
                      in p88  interval minute(4) to second   , inout p89  interval minute(4) to second   , out p90  interval minute(4) to second,
                      in p91  interval second(2,0)           , inout p92  interval second(2,0)           , out p93  interval second(2,0),
                      in p94  interval second                , inout p95  interval second                , out p96  interval second,
                      in p97  interval second(4,0)           , inout p98  interval second(4,0)           , out p99  interval second(4,0),
                      in p100 interval second(4)             , inout p101 interval second(4)             , out p102 interval second(4)
                        )  begin
                            set p3   = p2   ; set p2   = p1   ;
                            set p6   = p5   ; set p5   = p4   ;
                            set p9   = p8   ; set p8   = p7   ;
                            set p12  = p11  ; set p11  = p10  ;
                            set p15  = p14  ; set p14  = p13  ;
                            set p18  = p17  ; set p17  = p16  ;
                            set p21  = p20  ; set p20  = p19  ;
                            set p24  = p23  ; set p23  = p22  ;
                            set p27  = p26  ; set p26  = p25  ;
                            set p30  = p29  ; set p29  = p28  ;
                            set p33  = p32  ; set p32  = p31  ;
                            set p36  = p35  ; set p35  = p34  ;
                            set p39  = p38  ; set p38  = p37  ;
                            set p42  = p41  ; set p41  = p40  ;
                            set p45  = p44  ; set p44  = p43  ;
                            set p48  = p47  ; set p47  = p46  ;
                            set p51  = p50  ; set p50  = p49  ;
                            set p54  = p53  ; set p53  = p52  ;
                            set p57  = p56  ; set p56  = p55  ;
                            set p60  = p59  ; set p59  = p58  ;
                            set p63  = p62  ; set p62  = p61  ;
                            set p66  = p65  ; set p65  = p64  ;
                            set p69  = p68  ; set p68  = p67  ;
                            set p72  = p71  ; set p71  = p70  ;
                            set p75  = p74  ; set p74  = p73  ;
                            set p78  = p77  ; set p77  = p76  ;
                            set p81  = p80  ; set p80  = p79  ;
                            set p84  = p83  ; set p83  = p82  ;
                            set p87  = p86  ; set p86  = p85  ;
                            set p90  = p89  ; set p89  = p88  ;
                            set p93  = p92  ; set p92  = p91  ;
                            set p96  = p95  ; set p95  = p94  ;
                            set p99  = p98  ; set p98  = p97  ;
                            set p102 = p101 ; set p101 = p100 ;
                          end;""")

            try:
                interval = datatypes.Interval
                aaoInputParameters = [[
                    # Use interval types in IN/INOUT parameters and cast as PERIOD types

                    teradata.InParam (None, dataType='INTERVAL YEAR'),                                                                                         #p1   interval year
                    teradata.InOutParam(interval(negative=True, years=12), "p2", dataType='INTERVAL YEAR'),                                                    #p2   interval year
                    teradata.OutParam ("p3", dataType='INTERVAL YEAR'),                                                                                        #p3   interval year

                    teradata.InParam (interval(years=1234), dataType='INTERVAL YEAR(4)'),                                                                      #p4   interval year(4)
                    teradata.InOutParam(None, "p5", dataType='INTERVAL YEAR(4)'),                                                                              #p5   interval year(4)
                    teradata.OutParam ("p6", dataType='INTERVAL YEAR(4)'),                                                                                     #p6   interval year(4)

                    teradata.InParam (None, dataType='INTERVAL YEAR TO MONTH'),                                                                                #p7   interval year to month
                    teradata.InOutParam(interval(negative=True, years=12,  months=10), "p8", dataType='INTERVAL YEAR TO MONTH'),                               #p8   interval year to month
                    teradata.OutParam ("p9", dataType='INTERVAL YEAR TO MONTH'),                                                                               #p9   interval year

                    teradata.InParam (interval(years=1234, months=10), dataType='INTERVAL YEAR(4) TO MONTH'),                                                  #p10  interval year(4) to month
                    teradata.InOutParam(None, "p11", dataType='INTERVAL YEAR(4) TO MONTH'),                                                                    #p11  interval year(4) to month
                    teradata.OutParam ("p12", dataType='INTERVAL YEAR(4) TO MONTH'),                                                                           #p12  interval year

                    teradata.InParam (interval(months= 12), dataType='INTERVAL MONTH'),                                                                        #p13  interval month
                    teradata.InOutParam(interval(negative=True, months=12), "p14", dataType='INTERVAL MONTH'),                                                 #p14  interval month
                    teradata.OutParam ("p15", dataType='INTERVAL MONTH'),                                                                                      #p15  interval year

                    teradata.InParam (None, dataType='INTERVAL MONTH(4)'),                                                                                     #p16  interval month(4)
                    teradata.InOutParam(interval(negative=True, months=1234), "p17", dataType='INTERVAL MONTH(4)'),                                            #p17  interval month(4)
                    teradata.OutParam ("p18", dataType='INTERVAL MONTH(4)'),                                                                                   #p18  interval year

                    teradata.InParam (interval(days=11), dataType='INTERVAL DAY'),                                                                             #p19  interval day
                    teradata.InOutParam(None, "p20", dataType='INTERVAL DAY'),                                                                                 #p20  interval day
                    teradata.OutParam ("p21", dataType='INTERVAL DAY'),                                                                                        #p21  interval day

                    teradata.InParam (interval(days= 1234), dataType='INTERVAL DAY(4)'),                                                                       #p22  interval day(4)
                    teradata.InOutParam(interval(negative=True, days=1234), "p23", dataType='INTERVAL DAY(4)'),                                                #p23  interval day(4)
                    teradata.OutParam ("p24", dataType='INTERVAL DAY(4)'),                                                                                     #p24   interval day(4)

                    teradata.InParam (None, dataType='INTERVAL DAY TO HOUR'),                                                                                  #p25  interval day to hour
                    teradata.InOutParam(interval(negative=True, days=12,  hours=11), "p26", dataType='INTERVAL DAY TO HOUR'),                                  #p26  interval day to hour
                    teradata.OutParam ("p27", dataType='INTERVAL DAY TO HOUR'),                                                                                #p27  interval day to hour

                    teradata.InParam (interval(days=1234, hours=11), dataType='INTERVAL DAY(4) TO HOUR'),                                                      #p28  interval day(4) to hour
                    teradata.InOutParam(None, "p29", dataType='INTERVAL DAY(4) TO HOUR'),                                                                      #p29  interval day(4) to hour
                    teradata.OutParam ("p30", dataType='INTERVAL DAY(4) TO HOUR'),                                                                             #p30  interval day(4) to hour

                    teradata.InParam (interval(days=12,  hours=11, minutes=22), dataType='INTERVAL DAY TO MINUTE'),                                            #p31  interval day to minute
                    teradata.InOutParam(interval(negative=True, days=12, hours=11, minutes=22), "p32", dataType='INTERVAL DAY TO MINUTE'),                     #p32  interval day to minute
                    teradata.OutParam ("p33", dataType='INTERVAL DAY TO MINUTE'),                                                                              #p33   interval day to minute

                    teradata.InParam (None, dataType='INTERVAL DAY(4) TO MINUTE'),                                                                             #p34  interval day(4) to minute
                    teradata.InOutParam(interval(negative=True, days=1234, hours=11, minutes=22), "p35", dataType='INTERVAL DAY(4) TO MINUTE'),                #p35  interval day(4) to minute
                    teradata.OutParam ("p36", dataType='INTERVAL DAY(4) TO MINUTE'),                                                                           #p36   interval day (4) to minute

                    teradata.InParam (interval(days=12, hours=11, minutes=22, seconds=33), dataType='INTERVAL DAY TO SECOND(0)'),                              #p37  interval day to second(0)
                    teradata.InOutParam(None, "p38", dataType='INTERVAL DAY TO SECOND(0)'),                                                                    #p38  interval day to second(0)
                    teradata.OutParam ("p39", dataType='INTERVAL DAY TO SECOND(0)'),                                                                           #p39  interval year

                    teradata.InParam (interval(days=12,  hours=11, minutes=22, seconds=33.120001), dataType='INTERVAL DAY TO SECOND'),                         #p40  interval day to second
                    teradata.InOutParam(interval(negative=True, days=12, hours=11, minutes=22, seconds=33.987654), "p41", dataType='INTERVAL DAY TO SECOND'),  #p41  interval day to second
                    teradata.OutParam ("p42", dataType='INTERVAL DAY TO SECOND'),                                                                              #p42  interval year

                    teradata.InParam (None, dataType='INTERVAL DAY(4) TO SECOND(0)'),                                                                          #p43  interval day(4) to second(0)
                    teradata.InOutParam(interval(negative=True, days=1234, hours=11, minutes=22, seconds=33), "p44", dataType='INTERVAL DAY(4) TO SECOND(0)'), #p44  interval day(4) to second(0)
                    teradata.OutParam ("p45", dataType='INTERVAL DAY(4) TO SECOND(0)'),                                                                        #p45  interval day(4) to second(0)

                    teradata.InParam (interval(days=1234, hours=11, minutes=22, seconds=33.124321), dataType='INTERVAL DAY(4) TO SECOND'),                     #p46  interval day(4) to second
                    teradata.InOutParam(None, "p47", dataType='INTERVAL DAY(4) TO SECOND'),                                                                    #p47  interval day(4) to second
                    teradata.OutParam ("p48", dataType='INTERVAL DAY(4) TO SECOND'),                                                                           #p48  interval year

                    teradata.InParam (interval(hours= 12), dataType='INTERVAL HOUR'),                                                                          #p49  interval hour
                    teradata.InOutParam(interval(negative=True, hours=12), "p50", dataType='INTERVAL HOUR'),                                                   #p50  interval hour
                    teradata.OutParam ("p51", dataType='INTERVAL HOUR'),                                                                                       #p51  interval year

                    teradata.InParam (None, dataType='INTERVAL HOUR(4)'),                                                                                      #p52  interval hour(4)
                    teradata.InOutParam(interval(negative=True, hours=1234), "p53", dataType='INTERVAL HOUR(4)'),                                              #p53  interval hour(4)
                    teradata.OutParam ("p54", dataType='INTERVAL HOUR(4)'),                                                                                    #p54  interval hour(4)

                    teradata.InParam (interval(hours=12, minutes=22), dataType='INTERVAL HOUR TO MINUTE'),                                                     #p55  interval hour to minute
                    teradata.InOutParam(None, "p56", dataType='INTERVAL HOUR TO MINUTE'),                                                                      #p56  interval hour to minute
                    teradata.OutParam ("p57", dataType='INTERVAL HOUR TO MINUTE'),                                                                             #p57  interval hour to minute

                    teradata.InParam (interval(hours=1234, minutes=22), dataType='INTERVAL HOUR(4) TO MINUTE'),                                                #p58  interval hour(4) to minute
                    teradata.InOutParam(interval(negative=True, hours=1234, minutes=22), "p59", dataType='INTERVAL HOUR(4) TO MINUTE'),                        #p59  interval hour(4) to minute
                    teradata.OutParam ("p60", dataType='INTERVAL HOUR(4) TO MINUTE'),                                                                          #p60  interval hour(4) to minute

                    teradata.InParam (None, dataType='INTERVAL HOUR TO SECOND (0)'),                                                                           #p61  interval hour to second(0)
                    teradata.InOutParam(interval(negative=True, hours=12, minutes=22, seconds=33), "p62", dataType='INTERVAL HOUR TO SECOND (0)'),             #p62  interval hour to second(0)
                    teradata.OutParam ("p63", dataType='INTERVAL HOUR TO SECOND (0)'),                                                                         #p63   interval hour to second(0)

                    teradata.InParam (interval(negative=True, hours=12, minutes=22, seconds=33.145655), dataType='INTERVAL HOUR TO SECOND'),                   #p64  interval hour to second
                    teradata.InOutParam(None, "p65", dataType='INTERVAL HOUR TO SECOND'),                                                                      #p65  interval hour to second
                    teradata.OutParam ("p66", dataType='INTERVAL HOUR TO SECOND'),                                                                             #p66  interval year

                    teradata.InParam (interval(hours= 1234, minutes=22, seconds=33), dataType='INTERVAL HOUR(4) TO SECOND(0)'),                                #p67  interval hour(4) to second(0)
                    teradata.InOutParam(interval(negative=True, hours=1234, minutes=22, seconds=33), "p68", dataType='INTERVAL HOUR(4) TO SECOND(0)'),         #p68  interval hour(4) to second(0)
                    teradata.OutParam ("p69", dataType='INTERVAL HOUR(4) TO SECOND(0)'),                                                                       #p69  interval hour(4) to second(0)

                    teradata.InParam (None, dataType='INTERVAL HOUR(4) TO SECOND'),                                                                            #p70  interval hour(4) to second
                    teradata.InOutParam(interval(negative=True, hours=1234, minutes=22, seconds=33.145666), "p71", dataType='INTERVAL HOUR(4) TO SECOND'),     #p71  interval hour(4) to second
                    teradata.OutParam ("p72", dataType='INTERVAL HOUR(4) TO SECOND'),                                                                          #p72  interval hour(4) to second

                    teradata.InParam (interval(minutes=12), dataType='INTERVAL MINUTE'),                                                                       #p73  interval minute
                    teradata.InOutParam(None, "p74", dataType='INTERVAL MINUTE'),                                                                              #p74  interval minute
                    teradata.OutParam ("p75", dataType='INTERVAL MINUTE'),                                                                                     #p75  interval minute

                    teradata.InParam (interval(minutes=1234), dataType='INTERVAL MINUTE(4)'),                                                                  #p76  interval minute(4)
                    teradata.InOutParam(interval(negative=True, minutes=1234), "p77", dataType='INTERVAL MINUTE(4)'),                                          #p77  interval minute(4)
                    teradata.OutParam ("p78", dataType='INTERVAL MINUTE(4)'),                                                                                  #p78  interval minute(4)

                    teradata.InParam (None, dataType='INTERVAL MINUTE TO SECOND(0)'),                                                                          #p79  interval minute to second(0)
                    teradata.InOutParam(interval(negative=True, minutes=12, seconds=33), "p80", dataType='INTERVAL MINUTE TO SECOND(0)'),                      #p80  interval minute to second(0)
                    teradata.OutParam ("p81", dataType='INTERVAL MINUTE TO SECOND(0)'),                                                                        #p81  interval second(0)

                    teradata.InParam (interval(minutes=12, seconds=33.400004), dataType='INTERVAL MINUTE TO SECOND'),                                          #p82  interval minute to second
                    teradata.InOutParam(None, "p83", dataType='INTERVAL MINUTE TO SECOND'),                                                                    #p83  interval minute to second
                    teradata.OutParam ("p84", dataType='INTERVAL MINUTE TO SECOND'),                                                                           #p84  interval minute to second

                    teradata.InParam (interval(minutes=1234, seconds=33), dataType='INTERVAL MINUTE(4) TO SECOND(0)'),                                         #p85  interval minute(4) to second(0)
                    teradata.InOutParam(interval(negative=True, minutes=1234, seconds=33), "p86", dataType='INTERVAL MINUTE(4) TO SECOND(0)'),                 #p86  interval minute(4) to second(0)
                    teradata.OutParam ("p87", dataType='INTERVAL MINUTE(4) TO SECOND(0)'),                                                                     #p87  interval minute(4) to second(0)

                    teradata.InParam (None, dataType='INTERVAL MINUTE(4) TO SECOND'),                                                                          #p88  interval minute(4) to second
                    teradata.InOutParam(interval(negative=True, minutes=1234, seconds=33.002001), "p89", dataType='INTERVAL MINUTE(4) TO SECOND'),             #p89  interval minute(4) to second
                    teradata.OutParam ("p90", dataType='INTERVAL MINUTE(4) TO SECOND'),                                                                        #p90  interval minute(4) to second

                    teradata.InParam (interval(seconds=12), dataType='INTERVAL SECOND(2,0)'),                                                                  #p91  interval second(2,0)
                    teradata.InOutParam(None, "p92", dataType='INTERVAL SECOND(2,0)'),                                                                         #p92  interval second(2,0)
                    teradata.OutParam ("p93", dataType='INTERVAL SECOND(2,0)'),                                                                                #p93   interval second(2,0)

                    teradata.InParam (interval(seconds=12.123456), dataType='INTERVAL SECOND'),                                                                #p94  interval second
                    teradata.InOutParam(interval(negative=True, seconds=12.123456), "p95", dataType='INTERVAL SECOND'),                                        #p95  interval second
                    teradata.OutParam ("p96", dataType='INTERVAL SECOND'),                                                                                     #p96  interval second

                    teradata.InParam (None, dataType='INTERVAL SECOND(4,0)'),                                                                                  #p97  interval second(4,0)
                    teradata.InOutParam(interval(negative=True, seconds=1234), "p98", dataType='INTERVAL SECOND(4,0)'),                                        #p98  interval second(4,0)
                    teradata.OutParam ("p99", dataType='INTERVAL SECOND(4,0)'),                                                                                #p99  interval second(4,0)

                    teradata.InParam (interval(seconds=1234.123456), dataType='INTERVAL SECOND(4)'),                                                           #p100 interval second(4)
                    teradata.InOutParam(None, "p101", dataType='INTERVAL SECOND(4)'),                                                                          #p101 interval second(4)
                    teradata.OutParam ("p102", dataType='INTERVAL SECOND(4)')                                                                                  #p102 interval second(4)

                ], [ # Same values as above but use string interval values and cast as interval types
                    teradata.InParam (None, dataType='INTERVAL YEAR'),                                       #p1   interval year
                    teradata.InOutParam("-12", "p2", dataType='INTERVAL YEAR'),                              #p2   interval year
                    teradata.OutParam ("p3", dataType='INTERVAL YEAR'),                                      #p3   interval year

                    teradata.InParam (" 1234", dataType='INTERVAL YEAR(4)'),                                 #p4   interval year(4)
                    teradata.InOutParam(None, "p5", dataType='INTERVAL YEAR(4)'),                            #p5   interval year(4)
                    teradata.OutParam ("p6", dataType='INTERVAL YEAR(4)'),                                   #p6   interval year(4)

                    teradata.InParam (None, dataType='INTERVAL YEAR TO MONTH'),                              #p7   interval year to month
                    teradata.InOutParam("-12-10", "p8", dataType='INTERVAL YEAR TO MONTH'),                  #p8   interval year to month
                    teradata.OutParam ("p9", dataType='INTERVAL YEAR TO MONTH'),                             #p9   interval year

                    teradata.InParam (" 1234-10", dataType='INTERVAL YEAR(4) TO MONTH'),                     #p10  interval year(4) to month
                    teradata.InOutParam(None, "p11", dataType='INTERVAL YEAR(4) TO MONTH'),                  #p11  interval year(4) to month
                    teradata.OutParam ("p12", dataType='INTERVAL YEAR(4) TO MONTH'),                         #p12  interval year

                    teradata.InParam (" 12", dataType='INTERVAL MONTH'),                                     #p13  interval month
                    teradata.InOutParam("-12", "p14", dataType='INTERVAL MONTH'),                            #p14  interval month
                    teradata.OutParam ("p15", dataType='INTERVAL MONTH'),                                    #p15  interval year

                    teradata.InParam (None, dataType='INTERVAL MONTH(4)'),                                   #p16  interval month(4)
                    teradata.InOutParam("-1234", "p17", dataType='INTERVAL MONTH(4)'),                       #p17  interval month(4)
                    teradata.OutParam ("p18", dataType='INTERVAL MONTH(4)'),                                 #p18  interval year

                    teradata.InParam (" 11", dataType='INTERVAL DAY'),                                       #p19  interval day
                    teradata.InOutParam(None, "p20", dataType='INTERVAL DAY'),                               #p20  interval day
                    teradata.OutParam ("p21", dataType='INTERVAL DAY'),                                      #p21  interval day

                    teradata.InParam (" 1234", dataType='INTERVAL DAY(4)'),                                  #p22  interval day(4)
                    teradata.InOutParam("-1234", "p23", dataType='INTERVAL DAY(4)'),                         #p23  interval day(4)
                    teradata.OutParam ("p24", dataType='INTERVAL DAY(4)'),                                   #p24   interval day(4)

                    teradata.InParam (None, dataType='INTERVAL DAY TO HOUR'),                                #p25  interval day to hour
                    teradata.InOutParam("-12 11", "p26", dataType='INTERVAL DAY TO HOUR'),                   #p26  interval day to hour
                    teradata.OutParam ("p27", dataType='INTERVAL DAY TO HOUR'),                              #p27  interval day to hour

                    teradata.InParam (" 1234 11", dataType='INTERVAL DAY(4) TO HOUR'),                       #p28  interval day(4) to hour
                    teradata.InOutParam(None, "p29", dataType='INTERVAL DAY(4) TO HOUR'),                    #p29  interval day(4) to hour
                    teradata.OutParam ("p30", dataType='INTERVAL DAY(4) TO HOUR'),                           #p30  interval day(4) to hour

                    teradata.InParam (" 12 11:22", dataType='INTERVAL DAY TO MINUTE'),                       #p31  interval day to minute
                    teradata.InOutParam("-12 11:22", "p32", dataType='INTERVAL DAY TO MINUTE'),              #p32  interval day to minute
                    teradata.OutParam ("p33", dataType='INTERVAL DAY TO MINUTE'),                            #p33   interval day to minute

                    teradata.InParam (None, dataType='INTERVAL DAY(4) TO MINUTE'),                           #p34  interval day(4) to minute
                    teradata.InOutParam("-1234 11:22", "p35", dataType='INTERVAL DAY(4) TO MINUTE'),         #p35  interval day(4) to minute
                    teradata.OutParam ("p36", dataType='INTERVAL DAY(4) TO MINUTE'),                         #p36   interval day (4) to minute

                    teradata.InParam (" 12 11:22:33", dataType='INTERVAL DAY TO SECOND(0)'),                 #p37  interval day to second(0)
                    teradata.InOutParam(None, "p38", dataType='INTERVAL DAY TO SECOND(0)'),                  #p38  interval day to second(0)
                    teradata.OutParam ("p39", dataType='INTERVAL DAY TO SECOND(0)'),                         #p39  interval year

                    teradata.InParam (" 12 11:22:33.120001", dataType='INTERVAL DAY TO SECOND'),             #p40  interval day to second
                    teradata.InOutParam("-12 11:22:33.987654", "p41", dataType='INTERVAL DAY TO SECOND'),    #p41  interval day to second
                    teradata.OutParam ("p42", dataType='INTERVAL DAY TO SECOND'),                            #p42  interval year

                    teradata.InParam (None, dataType='INTERVAL DAY(4) TO SECOND(0)'),                        #p43  interval day(4) to second(0)
                    teradata.InOutParam("-1234 11:22:33", "p44", dataType='INTERVAL DAY(4) TO SECOND(0)'),   #p44  interval day(4) to second(0)
                    teradata.OutParam ("p45", dataType='INTERVAL DAY(4) TO SECOND(0)'),                      #p45  interval day(4) to second(0)

                    teradata.InParam (" 1234 11:22:33.124321", dataType='INTERVAL DAY(4) TO SECOND'),        #p46  interval day(4) to second
                    teradata.InOutParam(None, "p47", dataType='INTERVAL DAY(4) TO SECOND'),                  #p47  interval day(4) to second
                    teradata.OutParam ("p48", dataType='INTERVAL DAY(4) TO SECOND'),                         #p48  interval year

                    teradata.InParam (" 12", dataType='INTERVAL HOUR'),                                      #p49  interval hour
                    teradata.InOutParam("-12", "p50", dataType='INTERVAL HOUR'),                             #p50  interval hour
                    teradata.OutParam ("p51", dataType='INTERVAL HOUR'),                                     #p51  interval year

                    teradata.InParam (None, dataType='INTERVAL HOUR(4)'),                                    #p52  interval hour(4)
                    teradata.InOutParam("-1234", "p53", dataType='INTERVAL HOUR(4)'),                        #p53  interval hour(4)
                    teradata.OutParam ("p54", dataType='INTERVAL HOUR(4)'),                                  #p54  interval hour(4)

                    teradata.InParam (" 12:22", dataType='INTERVAL HOUR TO MINUTE'),                         #p55  interval hour to minute
                    teradata.InOutParam(None, "p56", dataType='INTERVAL HOUR TO MINUTE'),                    #p56  interval hour to minute
                    teradata.OutParam ("p57", dataType='INTERVAL HOUR TO MINUTE'),                           #p57  interval hour to minute

                    teradata.InParam (" 1234:22", dataType='INTERVAL HOUR(4) TO MINUTE'),                    #p58  interval hour(4) to minute
                    teradata.InOutParam("-1234:22", "p59", dataType='INTERVAL HOUR(4) TO MINUTE'),           #p59  interval hour(4) to minute
                    teradata.OutParam ("p60", dataType='INTERVAL HOUR(4) TO MINUTE'),                        #p60  interval hour(4) to minute

                    teradata.InParam (None, dataType='INTERVAL HOUR TO SECOND (0)'),                         #p61  interval hour to second(0)
                    teradata.InOutParam("-12:22:33", "p62", dataType='INTERVAL HOUR TO SECOND (0)'),         #p62  interval hour to second(0)
                    teradata.OutParam ("p63", dataType='INTERVAL HOUR TO SECOND (0)'),                       #p63   interval hour to second(0)

                    teradata.InParam ("-12:22:33.145655", dataType='INTERVAL HOUR TO SECOND'),               #p64  interval hour to second
                    teradata.InOutParam(None, "p65", dataType='INTERVAL HOUR TO SECOND'),                    #p65  interval hour to second
                    teradata.OutParam ("p66", dataType='INTERVAL HOUR TO SECOND'),                           #p66  interval year

                    teradata.InParam (" 1234:22:33", dataType='INTERVAL HOUR(4) TO SECOND(0)'),              #p67  interval hour(4) to second(0)
                    teradata.InOutParam("-1234:22:33", "p68", dataType='INTERVAL HOUR(4) TO SECOND(0)'),     #p68  interval hour(4) to second(0)
                    teradata.OutParam ("p69", dataType='INTERVAL HOUR(4) TO SECOND(0)'),                     #p69  interval hour(4) to second(0)

                    teradata.InParam (None, dataType='INTERVAL HOUR(4) TO SECOND'),                          #p70  interval hour(4) to second
                    teradata.InOutParam("-1234:22:33.145666", "p71", dataType='INTERVAL HOUR(4) TO SECOND'), #p71  interval hour(4) to second
                    teradata.OutParam ("p72", dataType='INTERVAL HOUR(4) TO SECOND'),                        #p72  interval hour(4) to second

                    teradata.InParam (" 12", dataType='INTERVAL MINUTE'),                                    #p73  interval minute
                    teradata.InOutParam(None, "p74", dataType='INTERVAL MINUTE'),                            #p74  interval minute
                    teradata.OutParam ("p75", dataType='INTERVAL MINUTE'),                                   #p75  interval minute

                    teradata.InParam (" 1234", dataType='INTERVAL MINUTE(4)'),                               #p76  interval minute(4)
                    teradata.InOutParam("-1234", "p77", dataType='INTERVAL MINUTE(4)'),                      #p77  interval minute(4)
                    teradata.OutParam ("p78", dataType='INTERVAL MINUTE(4)'),                                #p78  interval minute(4)

                    teradata.InParam (None, dataType='INTERVAL MINUTE TO SECOND(0)'),                        #p79  interval minute to second(0)
                    teradata.InOutParam("-12:33", "p80", dataType='INTERVAL MINUTE TO SECOND(0)'),           #p80  interval minute to second(0)
                    teradata.OutParam ("p81", dataType='INTERVAL MINUTE TO SECOND(0)'),                      #p81  interval second(0)

                    teradata.InParam (" 12:33.400004", dataType='INTERVAL MINUTE TO SECOND'),                #p82  interval minute to second
                    teradata.InOutParam(None, "p83", dataType='INTERVAL MINUTE TO SECOND'),                  #p83  interval minute to second
                    teradata.OutParam ("p84", dataType='INTERVAL MINUTE TO SECOND'),                         #p84  interval minute to second

                    teradata.InParam (" 1234:33", dataType='INTERVAL MINUTE(4) TO SECOND(0)'),               #p85  interval minute(4) to second(0)
                    teradata.InOutParam("-1234:33", "p86", dataType='INTERVAL MINUTE(4) TO SECOND(0)'),      #p86  interval minute(4) to second(0)
                    teradata.OutParam ("p87", dataType='INTERVAL MINUTE(4) TO SECOND(0)'),                   #p87  interval minute(4) to second(0)

                    teradata.InParam (None, dataType='INTERVAL MINUTE(4) TO SECOND'),                        #p88  interval minute(4) to second
                    teradata.InOutParam("-1234:33.002001", "p89", dataType='INTERVAL MINUTE(4) TO SECOND'),  #p89  interval minute(4) to second
                    teradata.OutParam ("p90", dataType='INTERVAL MINUTE(4) TO SECOND'),                      #p90  interval minute(4) to second

                    teradata.InParam (" 12", dataType='INTERVAL SECOND(2,0)'),                               #p91  interval second(2,0)
                    teradata.InOutParam(None, "p92", dataType='INTERVAL SECOND(2,0)'),                       #p92  interval second(2,0)
                    teradata.OutParam ("p93", dataType='INTERVAL SECOND(2,0)'),                              #p93   interval second(2,0)

                    teradata.InParam (" 12.123456", dataType='INTERVAL SECOND'),                             #p94  interval second
                    teradata.InOutParam("-12.123456", "p95", dataType='INTERVAL SECOND'),                    #p95  interval second
                    teradata.OutParam ("p96", dataType='INTERVAL SECOND'),                                   #p96  interval second

                    teradata.InParam (None, dataType='INTERVAL SECOND(4,0)'),                                #p97  interval second(4,0)
                    teradata.InOutParam("-1234", "p98", dataType='INTERVAL SECOND(4,0)'),                    #p98  interval second(4,0)
                    teradata.OutParam ("p99", dataType='INTERVAL SECOND(4,0)'),                              #p99  interval second(4,0)

                    teradata.InParam (" 1234.123456", dataType='INTERVAL SECOND(4)'),                        #p100 interval second(4)
                    teradata.InOutParam(None, "p101", dataType='INTERVAL SECOND(4)'),                        #p101 interval second(4)
                    teradata.OutParam ("p102", dataType='INTERVAL SECOND(4)')                                #p102 interval second(4)

                ]]

                results1 = conn.callproc("testProcedureIntervalNulls", aaoInputParameters [0])
                results2 = conn.callproc("testProcedureIntervalNulls", aaoInputParameters [1])
                self.assertEqual (len (results1), len (aaoInputParameters [0]))
                self.assertEqual (len (results2), len (aaoInputParameters [1]))
                nParam = 2
                for p in range (0, int (len (results1) * 2/3)):
                    # Use aaoInputParameters [0] for both compares since the results are always returned as interval
                    # values and not strings
                    self.assertEqual (results1 ["p{}".format(nParam)], aaoInputParameters [0][nParam - 2].inValue)
                    self.assertEqual (results2 ["p{}".format(nParam)], aaoInputParameters [0][nParam - 2].inValue)
                    nParam += (p + 2) % 2 + 1
            finally:
                conn.execute ("DROP PROCEDURE testProcedureIntervalNulls")
        # end testProcedureIntervalNulls

    def testProcedureDateTimeNulls(self):
        with udaExec.connect(self.dsn, username=self.username,
                              password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                """REPLACE PROCEDURE testProcedureDateTimeNulls
                    (
                        in p1   date                        , inout p2   date                        , out p3   date                        ,
                        in p4   time(0)                     , inout p5   time(0)                     , out p6   time(0)                     ,
                        in p7   time(3)                     , inout p8   time(3)                     , out p9   time(3)                     ,
                        in p10  time(0) with time zone      , inout p11  time(0) with time zone      , out p12  time(0) with time zone      ,
                        in p13  time (4) with time zone     , inout p14  time (4) with time zone     , out p15  time (4) with time zone     ,
                        in p16  timestamp (0)               , inout p17  timestamp (0)               , out p18  timestamp (0)               ,
                        in p19  timestamp                   , inout p20  timestamp                   , out p21  timestamp                   ,
                        in p22  timestamp (0) with time zone, inout p23  timestamp (0) with time zone, out p24  timestamp (0) with time zone,
                        in p25  timestamp (3) with time zone, inout p26  timestamp (3) with time zone, out p27  timestamp (3) with time zone
                    )  begin
                            set p3   = p2   ; set p2   = p1   ;
                            set p6   = p5   ; set p5   = p4   ;
                            set p9   = p8   ; set p8   = p7   ;
                            set p12  = p11  ; set p11  = p10  ;
                            set p15  = p14  ; set p14  = p13  ;
                            set p18  = p17  ; set p17  = p16  ;
                            set p21  = p20  ; set p20  = p19  ;
                            set p24  = p23  ; set p23  = p22  ;
                            set p27  = p26  ; set p26  = p25  ;
                    END;""")

            try:
                d = datetime.date
                t = datetime.time
                dt = datetime.datetime
                tz = datatypes.TimeZone
                period = datatypes.Period

                aaoParameters = [
                  [ # Use data, time & timestamp types in IN/INOUT parameters and cast
                    teradata.InParam (None, dataType='DATE ANSI'),                                                                                                                                                # p1 date)
                    teradata.InOutParam(d(1899, 1, 11), "p2", dataType='DATE ANSI'),                                                                                                     # p2 date)
                    teradata.OutParam ("p3", dataType="DATE ANSI"),                                                                                                                                               # p3 date)

                    teradata.InParam (t (9, 9, 9), dataType='TIME (0)'),                                                                                                            # p4 time (0))
                    teradata.InOutParam(None, "p5", dataType = 'TIME (0)'),                                                                                                                                  # p5 time (0))
                    teradata.OutParam ("p6", dataType = 'TIME (0)'),                                                                                                                                         # p6 time (0))

                    teradata.InParam(t(11,  5, 16, 123000), dataType='TIME(3)'),                                                                                                                                                 # p7 time)
                    teradata.InOutParam(t(12, 10, 45, 100000), "p8", dataType = 'TIME(3)'),                                                                                       # p8 time)
                    teradata.OutParam("p9", dataType = 'TIME(3)'),                                                                                                                                              # p9 time)

                    teradata.InParam(t (2, 12, 12, 0, tz("+", 0, 30)), dataType='TIME (0) WITH TIME ZONE'),                                                        #p10 time)
                    teradata.InOutParam(None, "p11", dataType = 'TIME (0) WITH TIME ZONE'),                                                                                                                  #p11 time)
                    teradata.OutParam("p12", dataType = 'TIME (0) WITH TIME ZONE'),                                                                                                                          #p12 time)

                    teradata.InParam(t (3, 4, 5, 60000, tz("+", 0, 30)), dataType='TIME (4) WITH TIME ZONE'),                                               #p13 time (4) with time zone)
                    teradata.InOutParam(None, "p14", dataType = 'TIME (4) WITH TIME ZONE'),                                                                                                                  #p14 time (4) with time zone)
                    teradata.OutParam("p15", dataType = 'TIME (4) WITH TIME ZONE'),                                                                                                                          #p15 time (4) with time zone)

                    teradata.InParam(None, dataType='TIMESTAMP (0)'),                                                                                                                                        #p16 time (0) with time zone)
                    teradata.InOutParam(dt (1980, 5, 3, 3, 4, 5), "p17", dataType = 'TIMESTAMP (0)'),                                                                     #p17 time (0) with time zone)
                    teradata.OutParam("p18", dataType = 'TIMESTAMP (0)'),                                                                                                                                    #p18 time (0) with time zone)

                    teradata.InParam(dt(1981, 6, 4,  4,  5,  6,  456000), dataType = 'TIMESTAMP'),                                                               #p19 timestamp with time zone)
                    teradata.InOutParam(None, "p20", dataType = 'TIMESTAMP'),                                                                                                                                #p20 timestamp with time zone)
                    teradata.OutParam("p21", dataType = 'TIMESTAMP'),                                                                                                                                        #p21 timestamp with time zone)

                    teradata.InParam(dt(2000, 1, 1, 0, 1,  5, 0, tz("+", 5, 30)), dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                   #p22 timestamp (0) with time zone)
                    teradata.InOutParam(None, "p23", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                                                                                                             #p23 timestamp (0) with time zone)
                    teradata.OutParam("p24", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                                                                                                                     #p24 timestamp (0) with time zone)

                    teradata.InParam(None, dataType='TIMESTAMP (3) WITH TIME ZONE'),                                                                                                                         #p25 timestamp (3) with time zone)
                    teradata.InOutParam(dt(2003, 10, 27, 8, 10, 30, 123000, tz("+", 5, 30)), "p26", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),  #p26 timestamp (3) with time zone)
                    teradata.OutParam("p27", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),                                                                                                                     #p27 timestamp (3) with time zone)

                ],[ # Same values as above but use string for params but cast as date, time or timestamp types
                    teradata.InParam (None, dataType='DATE INTEGER'),                                                                                             # p1 date)
                    teradata.InOutParam("1899-01-11", "p2", dataType='DATE INTEGER'),                                                                  # p2 date)
                    teradata.OutParam ("p3", dataType="DATE)"),                                                                                            # p3 date)

                    teradata.InParam ("09:09:09", dataType='TIME (0)'),                                                                          # p4 time (0))
                    teradata.InOutParam(None, "p5", dataType = 'TIME (0)'),                                                                               # p5 time (0))
                    teradata.OutParam ("p6", dataType = 'TIME (0)'),                                                                                      # p6 time (0))

                    teradata.InParam ("11:05:16.123", dataType = 'TIME(3)'),                                                           # p8 time)
                    teradata.InOutParam ("12:10:45.1", "p8", dataType = 'TIME(3)'),                                                           # p8 time)
                    teradata.OutParam ("p9", dataType = 'TIME(3)'),                                                                                          # p9 time)

                    teradata.InParam ("02:12:12+00:30", dataType='TIME (0) WITH TIME ZONE'),                                               #p10 time)
                    teradata.InOutParam(None, "p11", dataType = 'TIME (0) WITH TIME ZONE'),                                                               #p11 time)
                    teradata.OutParam ("p12", dataType = 'TIME (0) WITH TIME ZONE'),                                                                      #p12 time)

                    teradata.InParam ("03:04:05.06+00:30", dataType='TIME (4) WITH TIME ZONE'),                                        #p13 time (4) with time zone)
                    teradata.InOutParam(None, "p14", dataType = 'TIME (4) WITH TIME ZONE'),                                                               #p14 time (4) with time zone)
                    teradata.OutParam ("p15", dataType = 'TIME (4) WITH TIME ZONE'),                                                                      #p15 time (4) with time zone)

                    teradata.InParam (None, dataType='TIMESTAMP (0)'),                                                                                    #p16 time (0) with time zone)
                    teradata.InOutParam("1980-05-03 03:04:05", "p17", dataType = 'TIMESTAMP (0)'),                                    #p17 time (0) with time zone)
                    teradata.OutParam ("p18", dataType = 'TIMESTAMP (0)'),                                                                                #p18 time (0) with time zone)

                    teradata.InParam ("1981-06-04 04:05:06.456000", dataType = 'TIMESTAMP'),                                   #p19 timestamp with time zone)
                    teradata.InOutParam(None, "p20", dataType = 'TIMESTAMP'),                                                                             #p20 timestamp with time zone)
                    teradata.OutParam ("p21", dataType = 'TIMESTAMP'),                                                                                    #p21 timestamp with time zone)

                    teradata.InParam ("2000-01-01 00:01:05+05:30", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                  #p22 timestamp (0) with time zone)
                    teradata.InOutParam(None, "p23", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                                                          #p23 timestamp (0) with time zone)
                    teradata.OutParam ("p24", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                                                                 #p24 timestamp (0) with time zone)

                    teradata.InParam (None, dataType='TIMESTAMP (3) WITH TIME ZONE'),                                                                     #p25 timestamp (3) with time zone)
                    teradata.InOutParam("2003-10-27 08:10:30.123+05:30", "p26", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),  #p26 timestamp (3) with time zone)
                    teradata.OutParam ("p27", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),                                                                 #p27 timestamp (3) with time zone)
                ]]

                for i in range (len (aaoParameters)):
                    result = conn.callproc("testProcedureDateTimeNulls", aaoParameters [i])
                    self.assertEqual (len (result), len (aaoParameters [i]))
                    nParam = 2
                    for p in range (0, int (len (result) * 2/3)):
                        # Output is returned as date, time or timestamp so always compare against 1st parameter set
                        self.assertEqual (result ["p{}".format(nParam)], aaoParameters [0][nParam - 2].inValue)
                        nParam += (p + 2) % 2 + 1
            finally:
                conn.execute ("DROP PROCEDURE testProcedureDateTimeNulls")
        # end testProcedureDateTimeNulls

    def testProcedureInOutPeriod(self):
        with udaExec.connect(self.dsn, username=self.username,
                              password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                """REPLACE PROCEDURE testProcedureInOutPeriod
                    (
                        in p1   period (date)                        , inout p2   period (date)                        , out p3   period (date)                        ,
                        in p4   period (time(2))                     , inout p5   period (time(2))                     , out p6   period (time(2))                     ,
                        in p7   period (time)                        , inout p8   period (time)                        , out p9   period (time)                        ,
                        in p10  period (time(0) with time zone)      , inout p11  period (time(0) with time zone)      , out p12  period (time(0) with time zone)      ,
                        in p13  period (time (4) with time zone)     , inout p14  period (time (4) with time zone)     , out p15  period (time (4) with time zone)     ,
                        in p16  period (timestamp (1))               , inout p17  period (timestamp (1))               , out p18  period (timestamp (1))               ,
                        in p19  period (timestamp)                   , inout p20  period (timestamp)                   , out p21  period (timestamp)                   ,
                        in p22  period (timestamp (0) with time zone), inout p23  period (timestamp (0) with time zone), out p24  period (timestamp (0) with time zone),
                        in p25  period (timestamp (3) with time zone), inout p26  period (timestamp (3) with time zone), out p27  period (timestamp (3) with time zone)
                    )  begin
                            set p3   = p2   ; set p2   = p1   ;
                            set p6   = p5   ; set p5   = p4   ;
                            set p9   = p8   ; set p8   = p7   ;
                            set p12  = p11  ; set p11  = p10  ;
                            set p15  = p14  ; set p14  = p13  ;
                            set p18  = p17  ; set p17  = p16  ;
                            set p21  = p20  ; set p20  = p19  ;
                            set p24  = p23  ; set p23  = p22  ;
                            set p27  = p26  ; set p26  = p25  ;
                    END;""")

            try:
                d = datetime.date
                t = datetime.time
                dt = datetime.datetime
                tz = datatypes.TimeZone
                period = datatypes.Period

                aaoParameters = [
                  [ # Use period types in IN/INOUT parameters and cast as PERIOD types
                    teradata.InParam (period(d(1970, 1, 2),d(1973, 4, 5)), dataType='PERIOD (DATE)'),                                                                                                         # p1 period(date)
                    teradata.InOutParam(period(d(1998, 3, 21),d(1999, 12, 10)), "p2", dataType='PERIOD (DATE)'),                                                                                              # p2 period(date)
                    teradata.OutParam ("p3", dataType="PERIOD (DATE)"),                                                                                                                                       # p3 period(date)

                    teradata.InParam (period(t(11, 22, 33, 10000), t(22, 33, 44, 210000)), dataType='PERIOD (TIME (2))'),                                                                                     # p4 period(time (0))
                    teradata.InOutParam(period(t(9, 9, 9), t(10, 10, 10)), "p5", dataType = 'PERIOD (TIME (2))'),                                                                                             # p5 period(time (0))
                    teradata.OutParam ("p6", dataType = 'PERIOD (TIME (2))'),                                                                                                                                 # p6 period(time (0))

                    teradata.InParam(period(t(11, 22, 33, 234560), t(22, 33, 44, 345600)), dataType='PERIOD (TIME)'),                                                                                         # p7 period(time)
                    teradata.InOutParam(period(t(1, 2, 3, 456000), t(2, 3, 4, 560000)), "p8", dataType = 'PERIOD (TIME)'),                                                                                    # p8 period(time)
                    teradata.OutParam("p9", dataType = 'PERIOD (TIME)'),                                                                                                                                      # p9 period(time)

                    teradata.InParam(period(t (1, 22, 33, 0, tz("+", 0, 30)), t (22, 33, 44, 0, tz("+", 5, 30))), dataType='PERIOD (TIME (0) WITH TIME ZONE)'),                                               #p10 period(time)
                    teradata.InOutParam(period(t(2, 12, 12, 0, tz("+", 0, 30)), t(22, 3, 44, 0,tz("+", 5, 30))), "p11", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                                       #p11 period(time)
                    teradata.OutParam("p12", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                                                                                                                  #p12 period(time)

                    teradata.InParam(period(t (1, 22, 33, 60000, tz("+", 0, 30)), t (22, 33, 44, 561000, tz ("+", 5, 30))), dataType='PERIOD (TIME (4) WITH TIME ZONE)'),                                     #p13 period(time (4) with time zone)
                    teradata.InOutParam(period(t(3, 4, 5, 600000, tz("+", 0, 30)), t (12, 13, 14, 140000, tz ("+", 5, 30))), "p14", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                           #p14 period(time (4) with time zone)
                    teradata.OutParam("p15", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                                                                                                                  #p15 period(time (4) with time zone)

                    teradata.InParam(period(dt(1970, 1, 2, 3, 4, 5, 100000), dt(1976, 7, 8, 9, 10, 11, 900000)), dataType='PERIOD (TIMESTAMP (1))'),                                                          #p16 period(time (0) with time zone)
                    teradata.InOutParam(period(dt(1980, 5, 3, 3, 4, 5), dt (1986, 8, 7, 1, 10, 11)), "p17", dataType = 'PERIOD (TIMESTAMP (1))'),                                                             #p17 period(time (0) with time zone)
                    teradata.OutParam("p18", dataType = 'PERIOD (TIMESTAMP (1))'),                                                                                                                            #p18 period(time (0) with time zone)

                    teradata.InParam(period(dt(1970, 1, 2, 3, 4, 5,  456000), dt(1976, 7, 8, 9, 10, 11, 125600)), dataType = 'PERIOD (TIMESTAMP)'),                                                           #p19 period(timestamp with time zone)
                    teradata.InOutParam(period(dt(1981, 6, 4, 4, 5, 6, 124560), dt(1986, 7, 8, 11, 10, 11, 135600)), "p20", dataType = 'PERIOD (TIMESTAMP)'),                                                 #p20 period(timestamp with time zone)
                    teradata.OutParam("p21", dataType = 'PERIOD (TIMESTAMP)'),                                                                                                                                #p21 period(timestamp with time zone)

                    teradata.InParam(period(dt(1970, 1, 2, 3, 4, 5, 0, tz("+", 5, 30)), dt(1976, 7, 8, 9, 10, 11, 0, tz("+", 5, 30))), dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                   #p22 period(timestamp (0) with time zone)
                    teradata.InOutParam(period(dt(2000,  1,  1, 0, 1, 5, 0,tz("+", 5, 30)), dt(2020, 12, 31, 11, 59, 0, 0, tz("+", 5, 30))), "p23", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),      #p23 period(timestamp (0) with time zone)
                    teradata.OutParam("p24", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                                                                                                             #p24 period(timestamp (0) with time zone)

                    teradata.InParam(period(dt(1970, 1, 2, 3, 4, 5,  123000, tz("+", 5, 30)), dt(1976, 7, 8, 9, 10, 11, 123000, tz("+", 5, 30))), dataType='PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),          #p25 period(timestamp (3) with time zone)
                    teradata.InOutParam(period(dt(2003, 10, 27,  8,  10, 30, 0, tz("+", 5, 30)), dt(2019, 5, 6, 10, 21, 0, 0, tz ("+", 5, 30))), "p26", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),  #p26 period(timestamp (3) with time zone)
                    teradata.OutParam("p27", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),                                                                                                             #p27 period(timestamp (3) with time zone)

                ],[ # Same values as above but use string to represent periods and cast as period type
                    teradata.InParam ("1970-01-02,1973-04-05", dataType='PERIOD (DATE)'),                                                                   # p1 period(date)
                    teradata.InOutParam("1998-03-21,1999-12-10", "p2", dataType='PERIOD (DATE)'),                                                           # p2 period(date)
                    teradata.OutParam ("p3", dataType="PERIOD (DATE)"),                                                                                     # p3 period(date)

                    teradata.InParam ("11:22:33.01,22:33:44.21", dataType='PERIOD (TIME (2))'),                                                             # p4 period(time (0))
                    teradata.InOutParam("09:09:09,10:10:10", "p5", dataType = 'PERIOD (TIME (2))'),                                                         # p5 period(time (0))
                    teradata.OutParam ("p6", dataType = 'PERIOD (TIME (2))'),                                                                               # p6 period(time (0))

                    teradata.InParam ("11:22:33.23456,22:33:44.3456", dataType='PERIOD (TIME)'),                                                            # p7 period(time)
                    teradata.InOutParam("01:02:03.456,02:03:04.56", "p8", dataType = 'PERIOD (TIME)'),                                                      # p8 period(time)
                    teradata.OutParam ("p9", dataType = 'PERIOD (TIME)'),                                                                                   # p9 period(time)

                    teradata.InParam ("01:22:33+00:30,22:33:44+05:30", dataType='PERIOD (TIME (0) WITH TIME ZONE)'),                                        #p10 period(time)
                    teradata.InOutParam("02:12:12+00:30,22:03:44+05:30", "p11", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                             #p11 period(time)
                    teradata.OutParam ("p12", dataType = 'PERIOD (TIME (0) WITH TIME ZONE)'),                                                               #p12 period(time)

                    teradata.InParam ("01:22:33.06+00:30,22:33:44.561+05:30", dataType='PERIOD (TIME (4) WITH TIME ZONE)'),                                 #p13 period(time (4) with time zone)
                    teradata.InOutParam("03:04:05.6+00:30,12:13:14.14+05:30", "p14", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                        #p14 period(time (4) with time zone)
                    teradata.OutParam ("p15", dataType = 'PERIOD (TIME (4) WITH TIME ZONE)'),                                                               #p15 period(time (4) with time zone)

                    teradata.InParam ("1970-01-02 03:04:05.1,1976-07-08 09:10:11.9", dataType='PERIOD (TIMESTAMP (1))'),                                    #p16 period(time (0) with time zone)
                    teradata.InOutParam("1980-05-03 03:04:05,1986-08-07 01:10:11", "p17", dataType = 'PERIOD (TIMESTAMP (1))'),                             #p17 period(time (0) with time zone)
                    teradata.OutParam ("p18", dataType = 'PERIOD (TIMESTAMP (1))'),                                                                         #p18 period(time (0) with time zone)

                    teradata.InParam ("1970-01-02 03:04:05.456,1976-07-08 09:10:11.1256", dataType = 'PERIOD (TIMESTAMP)'),                                 #p19 period(timestamp with time zone)
                    teradata.InOutParam("1981-06-04 04:05:06.12456,1986-07-08 11:10:11.135600", "p20", dataType = 'PERIOD (TIMESTAMP)'),                    #p20 period(timestamp with time zone)
                    teradata.OutParam ("p21", dataType = 'PERIOD (TIMESTAMP)'),                                                                             #p21 period(timestamp with time zone)

                    teradata.InParam ("1970-01-02 03:04:05+05:30,1976-07-08 09:10:11+05:30", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),           #p22 period(timestamp (0) with time zone)
                    teradata.InOutParam("2000-01-01 00:01:05+05:30,2020-12-31 11:59:00+05:30", "p23", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),  #p23 period(timestamp (0) with time zone)
                    teradata.OutParam ("p24", dataType = 'PERIOD (TIMESTAMP (0) WITH TIME ZONE)'),                                                          #p24 period(timestamp (0) with time zone)

                    teradata.InParam ("1970-01-02 03:04:05.123+05:30,1976-07-08 09:10:11.123+05:30", dataType='PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),     #p25 period(timestamp (3) with time zone)
                    teradata.InOutParam("2003-10-27 08:10:30+05:30,2019-05-06 10:21:00+05:30", "p26", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),  #p26 period(timestamp (3) with time zone)
                    teradata.OutParam ("p27", dataType = 'PERIOD (TIMESTAMP (3) WITH TIME ZONE)'),                                                          #p27 period(timestamp (3) with time zone)
                ]]

                for i in range (len (aaoParameters)):
                    result = conn.callproc("testProcedureInOutPeriod", aaoParameters [i])
                    self.assertEqual (len (result), len (aaoParameters [i]))
                    nParam = 2
                    for p in range (0, int (len (result) * 2/3)):
                        # Output is returned as period types so always compare against 1st param set
                        self.assertEqual (result ["p{}".format(nParam)], aaoParameters [0][nParam - 2].inValue)
                        nParam += (p + 2) % 2 + 1
            finally:
                conn.execute ("DROP PROCEDURE testProcedureInOutPeriod")
        # end testProcedureInOutPeriod

    def testProcedureDateTime(self):
        with udaExec.connect(self.dsn, username=self.username,
                              password=self.password) as conn:
            self.assertIsNotNone(conn)
            conn.execute(
                """REPLACE PROCEDURE testProcedureDateTime
                    (
                        in p1   date                        , inout p2   date                        , out p3   date                        ,
                        in p4   time(0)                     , inout p5   time(0)                     , out p6   time(0)                     ,
                        in p7   time(3)                     , inout p8   time(3)                     , out p9   time(3)                     ,
                        in p10  time(0) with time zone      , inout p11  time(0) with time zone      , out p12  time(0) with time zone      ,
                        in p13  time (4) with time zone     , inout p14  time (4) with time zone     , out p15  time (4) with time zone     ,
                        in p16  timestamp (0)               , inout p17  timestamp (0)               , out p18  timestamp (0)               ,
                        in p19  timestamp                   , inout p20  timestamp                   , out p21  timestamp                   ,
                        in p22  timestamp (0) with time zone, inout p23  timestamp (0) with time zone, out p24  timestamp (0) with time zone,
                        in p25  timestamp (3) with time zone, inout p26  timestamp (3) with time zone, out p27  timestamp (3) with time zone
                    )  begin
                            set p3   = p2   ; set p2   = p1   ;
                            set p6   = p5   ; set p5   = p4   ;
                            set p9   = p8   ; set p8   = p7   ;
                            set p12  = p11  ; set p11  = p10  ;
                            set p15  = p14  ; set p14  = p13  ;
                            set p18  = p17  ; set p17  = p16  ;
                            set p21  = p20  ; set p20  = p19  ;
                            set p24  = p23  ; set p23  = p22  ;
                            set p27  = p26  ; set p26  = p25  ;
                    END;""")

            try:
                d = datetime.date
                t = datetime.time
                dt = datetime.datetime
                td = datetime.timedelta
                tz = datetime.timezone
                period = datatypes.Period

                aaoParameters = [
                  [ # Use data, time & timestamp types in IN/INOUT parameters and cast
                    teradata.InParam   (d(1899, 1, 11), dataType='DATE ANSI'),                                                                                   # p1 date
                    teradata.InOutParam(d(2001, 2,  5), "p2", dataType='DATE ANSI'),                                                                             # p2 date
                    teradata.OutParam  ("p3", dataType="DATE ANSI"),                                                                                             # p3 date

                    teradata.InParam   (t(17, 43, 53), dataType='TIME (0)'),                                                                                     # p4 time (0)
                    teradata.InOutParam(t( 5, 55,  5), "p5", dataType = 'TIME (0)'),                                                                             # p5 time (0)
                    teradata.OutParam  ("p6", dataType = 'TIME (0)'),                                                                                            # p6 time (0)

                    teradata.InParam   (t(20, 46, 56, 123000), dataType='TIME(3)'),                                                                              # p7 time (3)
                    teradata.InOutParam(t(10, 23, 28, 300000), "p8", dataType = 'TIME(3)'),                                                                      # p8 time (3)
                    teradata.OutParam  ("p9", dataType = 'TIME(3)'),                                                                                             # p9 time (3)

                    teradata.InParam   (t(10, 43, 53, tzinfo=tz(td(hours= 11, minutes= 45))), dataType='TIME (0) WITH TIME ZONE'),                               #p10 time (0) with time zone
                    teradata.InOutParam(t( 4,  4,  4, tzinfo=tz(td(hours= 10, minutes= 45))), "p11", dataType = 'TIME (0) WITH TIME ZONE'),                      #p11 time (0) with time zone
                    teradata.OutParam  ("p12", dataType = 'TIME (0) WITH TIME ZONE'),                                                                            #p12 time (0) with time zone

                    teradata.InParam   (t(14, 47, 57, 123400, tz(td(hours= -1))), dataType='TIME (4) WITH TIME ZONE'),                                           #p13 time (4) with time zone
                    teradata.InOutParam(t(12, 13, 13, 100000, tz(td(hours= -1))), "p14", dataType = 'TIME (4) WITH TIME ZONE'),                                  #p14 time (4) with time zone
                    teradata.OutParam  ("p15", dataType = 'TIME (4) WITH TIME ZONE'),                                                                            #p15 time (4) with time zone

                    teradata.InParam   (dt(1899, 6, 19, 12, 33, 50), dataType='TIMESTAMP (0)'),                                                                  #p16 timestamp (0)
                    teradata.InOutParam(dt(1999, 1, 22,  6, 15, 00), "p17", dataType = 'TIMESTAMP (0)'),                                                         #p17 timestamp (0)
                    teradata.OutParam  ("p18", dataType = 'TIMESTAMP (0)'),                                                                                      #p18 timestamp (0)

                    teradata.InParam   (dt(1901,  8, 21, 14, 35, 52,     12), dataType = 'TIMESTAMP'),                                                           #p19 timestamp
                    teradata.InOutParam(dt(1952, 11,  9,  7, 42, 12,     10), "p20", dataType = 'TIMESTAMP'),                                                    #p20 timestamp
                    teradata.OutParam  ("p21", dataType = 'TIMESTAMP'),                                                                                          #p21 timestamp

                    teradata.InParam   (dt(1899,  5, 24, 17, 28, 50,  tzinfo=tz(td(hours= 11, minutes= 45))), dataType = 'TIMESTAMP (0) WITH TIME ZONE'),        #p22 timestamp (0) with time zone
                    teradata.InOutParam(dt(2020, 10, 12,  8, 14, 25,  tzinfo=tz(td(hours= 11, minutes= 45))), "p23", dataType = 'TIMESTAMP (0) WITH TIME ZONE'), #p23 timestamp (0) with time zone
                    teradata.OutParam  ("p24", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                                                                       #p24 timestamp (0) with time zone

                    teradata.InParam   (dt(1901,  7, 26, 19, 30, 52,  12000, tz(td(hours= 1))), dataType='TIMESTAMP (3) WITH TIME ZONE'),                        #p25 timestamp (3) with time zone
                    teradata.InOutParam(dt(1863, 12,  2,  9, 15, 26,  50000, tz(td(hours= 1))), "p26", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),               #p26 timestamp (3) with time zone
                    teradata.OutParam  ("p27", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),                                                                       #p27 timestamp (3) with time zone

                ],[ # Same values as above but use string for params but cast as date, time or timestamp types
                    teradata.InParam   ("1899-01-11", dataType='DATE INTEGER'),                                             # p1 date
                    teradata.InOutParam("2001-02-05", "p2", dataType='DATE INTEGER'),                                       # p2 date
                    teradata.OutParam  ("p3", dataType="DATE)"),                                                            # p3 date

                    teradata.InParam   ("17:43:53", dataType='TIME (0)'),                                                   # p4 time (0)
                    teradata.InOutParam("05:55:05", "p5", dataType = 'TIME (0)'),                                           # p5 time (0)
                    teradata.OutParam  ("p6", dataType = 'TIME (0)'),                                                       # p6 time (0)

                    teradata.InParam   ("20:46:56.123", dataType = 'TIME(3)'),                                              # p8 time(3)
                    teradata.InOutParam("10:23:28.3", "p8", dataType = 'TIME(3)'),                                          # p8 time(3)
                    teradata.OutParam  ("p9", dataType = 'TIME(3)'),                                                        # p9 time(3)

                    teradata.InParam   ("10:43:53+11:45", dataType='TIME (0) WITH TIME ZONE'),                              #p10 time(0) with time zone
                    teradata.InOutParam("04:04:04+10:45", "p11", dataType = 'TIME (0) WITH TIME ZONE'),                     #p11 time(0) with time zone
                    teradata.OutParam  ("p12", dataType = 'TIME (0) WITH TIME ZONE'),                                       #p12 time(0) with time zone

                    teradata.InParam   ("14:47:57.1234-01:00", dataType='TIME (4) WITH TIME ZONE'),                         #p13 time (4) with time zone
                    teradata.InOutParam("12:13:13.1-01:00", "p14", dataType = 'TIME (4) WITH TIME ZONE'),                   #p14 time (4) with time zone
                    teradata.OutParam  ("p15", dataType = 'TIME (4) WITH TIME ZONE'),                                       #p15 time (4) with time zone

                    teradata.InParam   ("1899-06-19 12:33:50", dataType='TIMESTAMP (0)'),                                   #p16 timestamp (0)
                    teradata.InOutParam("1999-01-22 06:15:00", "p17", dataType = 'TIMESTAMP (0)'),                          #p17 timestamp (0)
                    teradata.OutParam  ("p18", dataType = 'TIMESTAMP (0)'),                                                 #p18 timestamp (0)

                    teradata.InParam   ("1901-08-21 14:35:52.000012", dataType = 'TIMESTAMP'),                              #p19 timestamp
                    teradata.InOutParam("1952-11-09 07:42:12.00001", "p20", dataType = 'TIMESTAMP'),                        #p20 timestamp
                    teradata.OutParam  ("p21", dataType = 'TIMESTAMP'),                                                     #p21 timestamp

                    teradata.InParam   ("1899-05-24 17:28:50+11:45", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),            #p22 timestamp (0) with time zone
                    teradata.InOutParam("2020-10-12 08:14:25+11:45", "p23", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),     #p23 timestamp (0) with time zone
                    teradata.OutParam  ("p24", dataType = 'TIMESTAMP (0) WITH TIME ZONE'),                                  #p24 timestamp (0) with time zone

                    teradata.InParam   ("1901-07-26 19:30:52.012+01:00", dataType='TIMESTAMP (3) WITH TIME ZONE'),          #p25 timestamp (3) with time zone
                    teradata.InOutParam("1863-12-02 09:15:26.05+01:00", "p26", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),  #p26 timestamp (3) with time zone
                    teradata.OutParam  ("p27", dataType = 'TIMESTAMP (3) WITH TIME ZONE'),                                  #p27 timestamp (3) with time zone
                ]]

                for i in range (len (aaoParameters)):
                    result = conn.callproc("testProcedureDateTime", aaoParameters [i])
                    self.assertEqual (len (result), len (aaoParameters [i]))
                    nParam = 2
                    for p in range (0, int (len (result) * 2/3)):
                        # Output is returned as date, time or timestamp so always compare against 1st parameter set
                        self.assertEqual (result ["p{}".format(nParam)], aaoParameters [0][nParam - 2].inValue)
                        nParam += (p + 2) % 2 + 1
            finally:
                conn.execute ("DROP PROCEDURE testProcedureDateTime")
        # end testProcedureDateTime

util.createTestCasePerDSN(
    UdaExecDataTypesTest, unittest.TestCase,  ("TERADATASQL",))

if __name__ == '__main__':
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stream = codecs.StreamWriter(sys.stdout, errors="replace")
    stream.encode = lambda msg, errors="strict": (msg.encode(locale.getpreferredencoding(False), errors).decode(), msg)
    sh = logging.StreamHandler(stream)
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
    suite.addTest(UdaExecExecuteTest_TERADATASQL(testName)) # @UndefinedVariable # noqa
    unittest.TextTestRunner().run(suite)

if __name__ == '__main__':
    unittest.main()
