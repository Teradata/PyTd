#!/usr/bin/python
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
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import sys
from setuptools import setup

# Make sure correct version of python is being used.
if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 4):
    print("The teradata module does not support this version of Python, the version must be 3.4 or later.")
    sys.exit(1)

with open('teradata/version.py') as f:
    exec(f.read())

setup(name='teradata',
      version=__version__,  # @UndefinedVariable
      author = 'Teradata Corporation',
      description='The Teradata python module for DevOps enabled SQL scripting for Teradata UDA.',
      url='http://github.com/teradata/PyTd',
      license='MIT',
      packages=['teradata'],
      install_requires=['teradatasql'],
      platforms = ['Windows', 'MacOS X', 'Linux'],
      python_requires = '>=3.4',
      zip_safe=True)
