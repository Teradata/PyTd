#!/bin/sh
# Runs the pytd unit tests on a remote server that supports ssh.
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

#################################################################################################################################
# TEST SERVER SETUP
# ---------------------------
#
# The pydtest.sh bash script can be run to execute the unit test on a remote host that has the following prerequisites:
#
# 1)  SSH Daemon running3
# 2)  BASH shell
# 3)  A pytd user that has the testidentity.pub file as an authorized_key for password less authentication.
# 4)  Python installed (e.g. multiple version of python can be installed for testing multiple versions.)
#  
# To install by source on Linux, download source distribution and run:
# 
# ./configure --prefix=/usr/local
# make
# make altinstall
# 
# 4)  Install pip if python version is before 3.4:  curl https://raw.githubusercontent.com/pypa/pip/master/contrib/get-pip.py | python2.7 -  
# 5)  Install virtualenv: python3.4 -m pip install virtualenv
# 6)  Install Teradata ODBC Driver
###################################################################################################################################

function printUsage
{
   echo ""
   echo "Usage: $0 [host] [pythonVersion]"
   echo ""
   echo "Example: $0 sdlc0000.labs.teradata.com python3.4"
   echo ""
   exit 1
}

# Asserts that the previous command succeeded.
function assert {
  # Gets the return code from the previous command.
  local returnCode=$?

  # Checks if the return code is not zero, signaling a failure.
  if [ $returnCode -ne 0 ] ;
  then
    error "$1 failed (Error Code: $returnCode)."
    exit 1
  else
    echo "$1 successful."
  fi
}

# Writes error information to the console.
# Param 1 - The error message. 
function error {
    echo "ERROR: $1"
}

if [ -z "$1" ]; then
   error "Missing host argument."
   printUsage
   exit 1
fi

if [ -z "$2" ]; then
  error "Missing python version argument."
  printUsage
  exit 1
fi

ENV_NAME=pytd_$2
TEST_DIR="~/$ENV_NAME/pytd"
SSH_COMMAND="ssh -i testidentity -oBatchMode=yes -oStrictHostKeyChecking=no pytd@$1"

chmod 600 testidentity
assert "Setup test identity"

echo "Checking login to $1..."
$SSH_COMMAND "echo \"Connected to $1\""
assert "Login check to $1"

$SSH_COMMAND "rm -rf ~/$ENV_NAME" 
assert "Cleanup of previous virtual environment"

$SSH_COMMAND "virtualenv -p $2 $ENV_NAME"
assert "Creation of test virtual environment"

$SSH_COMMAND "~/$ENV_NAME/bin/pip install teamcity-messages"
assert "Install teamcity integration module"

$SSH_COMMAND "mkdir $TEST_DIR"
assert "Creation of test directory"

scp -i testidentity -r * pytd@$1:$TEST_DIR/
assert "Copy of source files to test machine"

$SSH_COMMAND "cd $TEST_DIR;../bin/$2 -m teamcity.unittestpy discover -s test"
