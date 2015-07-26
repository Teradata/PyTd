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
if sys.version_info[0] < 2 or (sys.version_info[0] == 2 and sys.version_info[1] < 7):
    print("The teradata module does not support this version of Python, the version must be 2.7 or later.")
    sys.exit(1)
    
with open('teradata/version.py') as f: 
    exec(f.read())

setup(name='teradata',
      version=__version__,  # @UndefinedVariable
      description='The Teradata python module for DevOps enabled SQL scripting for Teradata UDA.',
      url='http://github.com/teradata/PyTd',
      author='Teradata Corporation',
      author_email='eric.scheie@teradata.com',
      license='MIT',
      packages=['teradata'],
      zip_safe=True)
