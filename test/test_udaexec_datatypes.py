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
import datetime
import teradata
import json
import math
from teradata import util, datatypes


class UdaExecDataTypesTest ():

    """Test UdaExec support for data types."""

    @classmethod
    def setUpClass(cls):
        cls.username = cls.password = util.setupTestUser(udaExec, cls.dsn)
        cls.failure = False

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
                # SEE REST-309 for more details about why the strip is
                # required.
                self.assertEqual(row.a.strip(), str(row.id % 10))
                self.assertEqual(row.a2.strip(), str(row.id % 100))
                self.assertEqual(row.b, str(row.id) * 10)
                self.assertEqual(row.c, str(row.id) * 20)
                self.assertIsNone(row.d)
            # REST-310 - REST does not support CLOB inserts more than 64k
            # characters.
            if self.dsn == "ODBC":
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
                for row in conn.execute("SELECT * FROM testStringDataTypes "
                                        "WHERE id > 101"):
                    self.assertEqual(row.c, str(row.id % 10) * 64000)

    def testBinaryDataTypes(self):
        # REST Does not support binary data types at this time.
        if self.dsn == "ODBC":
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
        # Test for GitHub issue #7
        # REST Does not support binary data types at this time.
        if self.dsn == "ODBC":
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
                        for i in range(0, 128)],
                batch=True)
            conn.execute(
                "INSERT INTO testNumericDataTypes VALUES (128, 99, 999, "
                "9999, 99999, 99999.9, 99999.99, 99999.999, 99999.9999, "
                "99999.99999, 99999.999999)")
            cursor = conn.execute(
                "SELECT * FROM testNumericDataTypes ORDER BY id")
            # for t in cursor.types:
            # print(t)
            for row in cursor:
                # print(row)
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
                        elif count < 9 or self.dsn != 'ODBC':
                            self.assertEqual(
                                col, decimal.Decimal("99999." + "9" *
                                                     (count - 5)))
                        else:
                            self.assertEqual(
                                col, float("99999." + "9" *
                                           (count - 5)))
                        count += 1

    def testInfinityAndNaN(self):
        self.assertEqual(float('inf'), decimal.Decimal('Infinity'))
        self.assertEqual(float('-inf'), decimal.Decimal('-Infinity'))
        self.assertEqual(
            math.isnan(float('NaN')), math.isnan(decimal.Decimal('NaN')))
        # Infinities are not support by REST.
        if self.dsn == "ODBC":
            with udaExec.connect(self.dsn, username=self.username,
                                 password=self.password) as conn:
                self.assertIsNotNone(conn)
                conn.execute("CREATE TABLE testInfinity (id INTEGER, "
                             "a FLOAT)")
                for batch in (False, True):
                    offset = 6 if batch else 0
                    conn.executemany(
                        "INSERT INTO testInfinity (?, ?)",
                        ((1 + offset, float('Inf')),
                         (2 + offset, decimal.Decimal('Infinity'))),
                        batch=batch)
                    for row in conn.execute("SELECT * FROM testInfinity "
                                            "WHERE id > ?",  (offset, )):
                        self.assertEqual(row[1], float('inf'))
                    conn.executemany(
                        "INSERT INTO testInfinity (?, ?)",
                        ((3 + offset, float('-Inf')),
                         (4 + offset, decimal.Decimal('-Infinity'))),
                        batch=batch)
                    for row in conn.execute("SELECT * FROM testInfinity "
                                            "WHERE id > ?", (2 + offset, )):
                        self.assertEqual(row[1], float('-inf'))
                    conn.executemany(
                        "INSERT INTO testInfinity (?, ?)",
                        ((5 + offset, float('NaN')),
                         (6 + offset, decimal.Decimal('NaN'))),
                        batch=batch)
                    for row in conn.execute("SELECT * FROM testInfinity "
                                            "WHERE id > ?", (4 + offset, )):
                        self.assertTrue(math.isnan(row[1]))

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
                    if self.dsn == 'ODBC':
                        params.append(
                            [i, f, decimal.Decimal(f), f, str(f), f,
                             decimal.Decimal(f)])
                    else:
                        # REST doesn't like large str conversion of
                        # decimal.Decimal(f)
                        params.append([i, f, f, str(f), f, f, f])
                params.append([paramCount, None, None, None, None, None, None])
                f = math.sqrt(3)
                self.assertEqual(f, decimal.Decimal(f))
                self.assertEqual(f, float(decimal.Decimal(f)))
                params.append([paramCount + 1, f, f, f, f, f, f])
                for batch in (False, True):
                    conn.executemany(
                        "INSERT INTO testFloatTypes (?, ?, ?, ?, ?, ?, ?)",
                        params, batch=batch)
                    count = 0
                    for row in conn.execute("SELECT * FROM testFloatTypes "
                                            "ORDER BY id"):
                        # REST-312 - floating point number precision is lost
                        # when get float as a string from JDBC driver.
                        if self.dsn == 'ODBC':
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
                            self.assertEqual(t.hour, 12)
                            self.assertEqual(t.minute, 34)
                            self.assertEqual(t.second, 56)
                            self.assertEqual(t.microsecond, 789000)
                        count += 1
                    # Time zone information is not coming back for REST per
                    # REST-302.
                    if self.dsn == "ODBC":
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
            # REST-304 - REST Does not support array data types.
            if self.dsn == "ODBC":
                cursor = conn.execute(
                    "SELECT * FROM testArrayDataTypes ORDER BY id")
                # for t in cursor.types:
                # Type comes back as VARCHAR() =(
                # print(t)
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
        # REST-304 - REST Does not support for period data types.
        if self.dsn == "ODBC":
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

# The unit tests in the UdaExecExecuteTest are execute once for each named
# data source below.
util.createTestCasePerDSN(
    UdaExecDataTypesTest, unittest.TestCase, ("HTTP", "HTTPS", "ODBC"))

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
    suite.addTest(UdaExecDataTypesTest_ODBC(testName))  # @UndefinedVariable # noqa
    suite.addTest(UdaExecDataTypesTest_HTTPS(testName))  # @UndefinedVariable # noqa
    unittest.TextTestRunner().run(suite)

if __name__ == '__main__':
    # runTest('testFloatTypes')
    unittest.main()
