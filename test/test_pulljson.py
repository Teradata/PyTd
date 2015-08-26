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
from teradata import pulljson
import unittest
import sys

if sys.version_info[0] == 2:
    from StringIO import StringIO  # @UnresolvedImport #@UnusedImport
else:
    from io import StringIO  # @UnresolvedImport @UnusedImport @Reimport


class TestJSONPullParser (unittest.TestCase):

    def testNextEvent(self):
        stream = StringIO("""{"key1":"value", "key2":100, "key3":null,
        "key4": true, "key5":false, "key6":-201.50E1, "key7":{"key8":"value2",
        "key9":null}, "key10":["value3", 10101010101010101010101, null,
        {} ] }""")
        reader = pulljson.JSONPullParser(stream)

        # Start of object
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)

        # Key1 - "value"
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key1")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertEqual(event.value, "value")
        self.assertEqual(event.valueType, pulljson.STRING)

        # Key2 - 100
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key2")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertEqual(event.value, 100)
        self.assertEqual(event.valueType, pulljson.NUMBER)

        # Key3 - null
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key3")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertIsNone(event.value)
        self.assertEqual(event.valueType, pulljson.NULL)

        # Key4 - true
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key4")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertTrue(event.value)
        self.assertEqual(event.valueType, pulljson.BOOLEAN)

        # Key5 - false
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key5")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertFalse(event.value)
        self.assertEqual(event.valueType, pulljson.BOOLEAN)

        # Key6
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key6")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertEqual(event.value, -2015)
        self.assertEqual(event.valueType, pulljson.NUMBER)

        # Key7
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key7")

        # Start of key7 object
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)

        # Key8 - value2
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key8")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertEqual(event.value, "value2")
        self.assertEqual(event.valueType, pulljson.STRING)

        # Key9 - null
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key9")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertIsNone(event.value)

        # End of key7 object
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.END_OBJECT)

        # Key10 - array[0] - value3
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key10")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_ARRAY)

        # Key10 - array[0] - value3
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.ARRAY_VALUE)
        self.assertEqual(event.value, "value3")
        self.assertEqual(event.valueType, pulljson.STRING)
        self.assertEqual(event.arrayIndex, 0)

        # Key10 - array[1] - 10101010101010101010101
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.ARRAY_VALUE)
        self.assertEqual(event.value, 10101010101010101010101)
        self.assertEqual(event.valueType, pulljson.NUMBER)
        self.assertEqual(event.arrayIndex, 1)

        # Key10 - array[2] - null
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.ARRAY_VALUE)
        self.assertIsNone(event.value)
        self.assertEqual(event.valueType, pulljson.NULL)
        self.assertEqual(event.arrayIndex, 2)

        # Key10 - array[3] - object
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        self.assertEqual(event.arrayIndex, 3)

        # Key10 - array[3] - object
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.END_OBJECT)
        self.assertEqual(event.arrayIndex, 3)

        # End of key 10 array.
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.END_ARRAY)

        # End of object
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.END_OBJECT)
        event = reader.nextEvent()
        self.assertIsNone(event)

    def testDocumentIncomplete(self):
        stream = StringIO('{"key":"value"')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key")

        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_INCOMPLETE_ERROR,
            cm.exception.msg)

    def testEmptyName(self):
        stream = StringIO('{:"value"}')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testExtraWhiteSpace(self):
        stream = StringIO('{\n\t "key"\n\t\t:   "\t value\n"}   ')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, "key")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertEqual(event.value, "\t value\n")
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.END_OBJECT)
        event = reader.nextEvent()
        self.assertIsNone(event)

    def testEscapeCharacter(self):
        stream = StringIO('{"\\"ke\\"y\\\\"  : "va\\"l\\"ue"}   ')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        self.assertEqual(event.value, '"ke"y\\')
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_VALUE)
        self.assertEqual(event.value, 'va"l"ue')
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.END_OBJECT)
        event = reader.nextEvent()
        self.assertIsNone(event)

    def testEmptyArray(self):
        stream = StringIO('[]')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_ARRAY)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.END_ARRAY)
        event = reader.nextEvent()
        self.assertIsNone(event)

    def testMissingColon(self):
        stream = StringIO('{"key" "value"}')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testCommaInsteadOfColon(self):
        stream = StringIO('{"key","value"}')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testColonInsteadOfComma(self):
        stream = StringIO('["key":"value"]')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_ARRAY)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testNumberLiteral(self):
        stream = StringIO('1')
        reader = pulljson.JSONPullParser(stream)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testStringLiteral(self):
        stream = StringIO('"This is a test"')
        reader = pulljson.JSONPullParser(stream)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testObjectMissingValue(self):
        stream = StringIO('{"key":}')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.FIELD_NAME)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testArrayMissingValue(self):
        stream = StringIO('[1, ,2}')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_ARRAY)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.ARRAY_VALUE)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testArrayInObject(self):
        stream = StringIO('{[]}')
        reader = pulljson.JSONPullParser(stream)
        event = reader.nextEvent()
        self.assertEqual(event.type, pulljson.START_OBJECT)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            event = reader.nextEvent()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testReadObject(self):
        stream = StringIO(
            '{"key1":[0,1,2,3,4,{"value":"5"}], "key2":\
            {"key1":[0,1,2,3,4,{"value":"5"}]}}')
        reader = pulljson.JSONPullParser(stream)
        obj = reader.readObject()
        self.assertEqual(len(obj), 2)
        for i in range(0, 2):
            self.assertEqual(len(obj["key1"]), 6)
            for i in range(0, 5):
                self.assertEqual(obj["key1"][i], i)
            self.assertEqual(obj["key1"][5]["value"], "5")
            if i == 1:
                obj = obj["key2"]
                self.assertEqual(len(obj), 1)

    def testReadArray(self):
        stream = StringIO('[0,1,2,3,4,[0,1,2,3,4,[0,1,2,3,4]],[0,1,2,3,4]]')
        reader = pulljson.JSONPullParser(stream)
        arr = reader.readArray()
        self.assertEqual(len(arr), 7)
        for i in range(0, 5):
            self.assertEqual(arr[i], i)
        for i in range(0, 5):
            self.assertEqual(arr[5][i], i)
        for i in range(0, 5):
            self.assertEqual(arr[5][5][i], i)
        for i in range(0, 5):
            self.assertEqual(arr[6][i], i)

    def testArraySyntaxError(self):
        stream = StringIO('[[0,1][0,1]]')
        reader = pulljson.JSONPullParser(stream)
        with self.assertRaises(pulljson.JSONParseError) as cm:
            reader.readArray()
        self.assertEqual(
            cm.exception.code, pulljson.JSON_SYNTAX_ERROR, cm.exception.msg)

    def testIterateArray(self):
        stream = StringIO(
            '[{"key0}":["}\\"","\\"}","}"]}, {"key1}":["}","\\"}","}"]}, '
            '{"key2}":["}","}","\\"}"]}]')
        reader = pulljson.JSONPullParser(stream)
        i = 0
        for x in reader.expectArray():
            self.assertEqual(len(x["key" + str(i) + "}"]), 3)
            i += 1


if __name__ == '__main__':
    unittest.main()
