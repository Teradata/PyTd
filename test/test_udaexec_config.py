# -*- coding: utf-8 -*-
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
import teradata
import os
import sys
import logging

configFiles = [os.path.join(os.path.dirname(__file__), file)
               for file in ('udaexec.ini', 'udaexec2.ini')]


class UdaExecConfigTest (unittest.TestCase):

    """Test UdaExec DevOps features."""

    def setUp(self):
        self.udaExec = teradata.UdaExec(
            configFiles=configFiles, configureLogging=False)
        self.udaExec.checkpoint()
        self.assertIsNotNone(self.udaExec)

    def testGlobals(self):
        self.assertEqual(teradata.apilevel, "2.0")
        self.assertEqual(teradata.threadsafety, 1)
        self.assertEqual(teradata.paramstyle, "qmark")

    def testMissingAppName(self):
        with self.assertRaises(teradata.InterfaceError) as cm:
            teradata.UdaExec(configFiles=[], configureLogging=False)
        self.assertEqual(cm.exception.code, teradata.CONFIG_ERROR)

    def testConfig(self):
        udaExec = self.udaExec
        self.assertEqual(udaExec.config['appName'], u'PyTdUnitTestsの')
        self.assertEqual(udaExec.config['version'], '1.00.00.01')
        self.assertEqual(udaExec.config['key1'], 'file1')
        self.assertEqual(udaExec.config['key2'], 'file2')
        self.assertEqual(udaExec.config['key3'], 'file2')
        self.assertEqual(udaExec.config['key5'], 'file1')

    def testConfigEscapeCharacter(self):
        udaExec = self.udaExec
        self.assertEqual(udaExec.config['escapeTest'], 'this$isatest')

    def testEscapeCharacterInDataSource(self):
        section = self.udaExec.config.section("ESCAPE_TEST")
        self.assertEqual(section['password'], 'pa$$word')
        self.assertEqual(section['escapeTest2'], 'this$isatest')

    def testConnectUsingBadDSN(self):
        with self.assertRaises(teradata.InterfaceError) as cm:
            self.udaExec.connect("UNKNOWN")
        self.assertEqual(cm.exception.code, teradata.CONFIG_ERROR)

    def testRunNumber(self):
        # Check that runNumber is incremented by 1.
        udaExec = teradata.UdaExec(
            configFiles=configFiles, configureLogging=False)
        self.assertEqual(int(udaExec.runNumber.split(
            "-")[1]), int(self.udaExec.runNumber.split("-")[1]) + 1)

    def testResumeFromCheckPoint(self):
        checkpoint = "testResumeFromCheckPoint"
        self.udaExec.checkpoint(checkpoint)
        udaExec = teradata.UdaExec(
            configFiles=configFiles, configureLogging=False)
        self.assertEqual(udaExec.resumeFromCheckpoint, checkpoint)
        with udaExec.connect("ODBC") as session:
            self.assertIsNone(session.execute(
                "SELECT 1").fetchone(),
                "Query was executed but should have been skipped.")
            udaExec.checkpoint("notTheExpectedCheckpoint")
            self.assertIsNone(session.execute(
                "SELECT 1").fetchone(),
                "Query was executed but should have been skipped.")
            udaExec.checkpoint(checkpoint)
            self.assertEqual(session.execute("SELECT 1").fetchone()[0], 1)
        # Clear the checkpoint.
        self.udaExec.checkpoint()
        udaExec = teradata.UdaExec(
            configFiles=configFiles, configureLogging=False)
        self.assertIsNone(udaExec.resumeFromCheckpoint)
        udaExec.setResumeCheckpoint(checkpoint)
        self.assertEqual(udaExec.resumeFromCheckpoint, checkpoint)

    def testVariableResolutionEscapeCharacter(self):
        with self.udaExec.connect("ODBC") as session:
            self.assertEqual(
                session.execute(
                    "SELECT '$${ThisShouldBeTreatedAsALiteral}'").fetchone()[
                    0], "${ThisShouldBeTreatedAsALiteral}")
            self.assertEqual(
                session.execute(
                    "SELECT '$$ThisShouldBeTreatedAsALiteral'").fetchone()[
                    0], "$ThisShouldBeTreatedAsALiteral")


if __name__ == '__main__':
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(sh)
    unittest.main()
