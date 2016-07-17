""" A Python Database API Specification v2.0 implementation that provides
 configuration loading, variable substitution, logging, query banding,
 etc and options to use either ODBC or REST"""

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

import atexit
import codecs
import collections
import datetime
import getpass
import logging
import os.path
import platform
import string
import subprocess
import sys
import time

from . import tdodbc, util, api, datatypes
from . import tdrest  # @UnresolvedImport
from .util import toUnicode
from .version import __version__  # @UnresolvedImport


# The module logger
logger = logging.getLogger(__name__)

METHOD_REST = "rest"
METHOD_ODBC = "odbc"

# Implement python version specific setup.
if sys.version_info[0] == 2:
    import ConfigParser as configparser  # @UnresolvedImport #@UnusedImport
else:
    import configparser  # @UnresolvedImport @UnusedImport @Reimport


def handleUncaughtException(exc_type, exc_value, exc_traceback):
    """Make sure that uncaught exceptions are logged"""
    logger.error("Uncaught exception", exc_info=(
        exc_type, exc_value, exc_traceback))
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


def exiting():
    """Invoked when the python interpreter is exiting."""
    logger.info("UdaExec exiting.")


class UdaExec:

    """Helper class for scripting with Teradata systems"""

    def __init__(self, appName="${appName}",
                 version="${version}",
                 checkpointFile="${checkpointFile}",
                 runNumberFile="${runNumberFile}",
                 runNumber=None,
                 configureLogging="${configureLogging}",
                 logDir="${logDir}",
                 logFile="${logFile}",
                 logConsole="${logConsole}",
                 logLevel="${logLevel}",
                 logRetention="${logRetention}",
                 systemConfigFile="/etc/udaexec.ini",
                 userConfigFile="~/udaexec.ini",
                 appConfigFile="udaexec.ini",
                 configFiles=None,
                 configSection="CONFIG",
                 configEncoding="utf8",
                 parseCmdLineArgs=True,
                 gitPath="${gitPath}",
                 production="${production}",
                 odbcLibPath="${odbcLibPath}",
                 dataTypeConverter=datatypes.DefaultDataTypeConverter()):
        """ Initializes the UdaExec framework """
        # Load configuration files.
        if configFiles is None:
            configFiles = []
            _appendConfigFiles(
                configFiles, systemConfigFile, userConfigFile, appConfigFile)
        logMsgs = [(logging.INFO, "Initializing UdaExec...")]
        self.config = UdaExecConfig(configFiles, configEncoding,
                                    configSection, parseCmdLineArgs, logMsgs)
        # Verify required configuration parameters are specified.
        self.config['appName'] = self.config.resolve(
            appName, errorMsg="appName is a required field, it must be "
            "passed in as a parameter or specified in a config file.")
        # Initialize runNumbers.
        self._initRunNumber(runNumberFile, runNumber, logMsgs)
        # Configure Logging
        self.configureLogging = util.booleanValue(
            self.config.resolve(configureLogging, default="True"))
        if self.configureLogging:
            self._initLogging(
                self.config.resolve(logDir, default="logs"),
                self.config.resolve(
                    logFile, default=self.config.resolve(
                        "${appName}.${runNumber}.log")),
                util.booleanValue(
                    self.config.resolve(logConsole, default="True")),
                getattr(
                    logging, self.config.resolve(logLevel, default="INFO")),
                int(self.config.resolve(logRetention, default="90")), logMsgs)
        # Log messages that were collected prior to logging being configured.
        for (level, msg) in logMsgs:
            logger.log(level, toUnicode(msg))
        self._initVersion(self.config.resolve(
            version, default=""), self.config.resolve(gitPath, default=""))
        self._initQueryBands(self.config.resolve(production, default="false"))
        self._initCheckpoint(checkpointFile)
        self.odbcLibPath = self.config.resolve(odbcLibPath, default="")
        self.dataTypeConverter = dataTypeConverter
        logger.info(self)
        logger.debug(self.config)
        # Register exit function. d
        atexit.register(exiting)

    def connect(self, externalDSN=None, dataTypeConverter=None, **kwargs):
        """Creates a database connection"""
        # Construct data source configuration parameters
        args = {}
        if externalDSN is not None:
            externalDSN = self.config.resolve(externalDSN)
            args = self.config.section(externalDSN)
            if args is None:
                raise api.InterfaceError(
                    api.CONFIG_ERROR,
                    "No data source named \"{}\".".format(externalDSN))
        args.update(self.config.resolveDict(kwargs))
        # Log connection details.
        paramsToLog = dict(args)
        paramsToLog['password'] = 'XXXXXX'
        if externalDSN:
            paramsToLog['externalDSN'] = externalDSN
        logger.info("Creating connection: %s", paramsToLog)
        # Determine connection method.
        method = None
        if 'method' in args:
            method = args.pop('method')
        util.raiseIfNone('method', method)
        if 'queryBands' in args:
            queryBands = args.pop('queryBands')
            self.queryBands.update(queryBands)
        if 'autoCommit' not in args:
            args['autoCommit'] = "true"
        if not dataTypeConverter:
            dataTypeConverter = self.dataTypeConverter
        # Create the connection
        try:
            start = time.time()
            if method.lower() == METHOD_REST:
                conn = UdaExecConnection(
                    self, tdrest.connect(queryBands=self.queryBands,
                                         dataTypeConverter=dataTypeConverter,
                                         **args))
            elif method.lower() == METHOD_ODBC:
                conn = UdaExecConnection(
                    self, tdodbc.connect(queryBands=self.queryBands,
                                         odbcLibPath=self.odbcLibPath,
                                         dataTypeConverter=dataTypeConverter,
                                         **args))
            else:
                raise api.InterfaceError(
                    api.CONFIG_ERROR,
                    "Connection method \"{}\" not supported".format(method))
            duration = time.time() - start
            logger.info(
                "Connection successful. Duration: %.3f seconds. Details: %s",
                duration, paramsToLog)
            return conn
        except Exception:
            logger.exception("Unable to create connection: %s", paramsToLog)
            raise

    def checkpoint(self, checkpointName=None):
        """ Sets or clears the current checkpoint."""
        if checkpointName is None:
            logger.info("Clearing checkpoint....")
            self.currentCheckpoint = None
            self.skip = False
            if self.checkpointManager:
                self.checkpointManager.clearCheckpoint()
        else:
            self.currentCheckpoint = checkpointName
            if self.skip:
                if self.resumeFromCheckpoint == self.currentCheckpoint:
                    logger.info(
                        "Reached resume checkpoint: \"%s\".  "
                        "Resuming execution...", checkpointName)
                    self.skip = False
            else:
                logger.info("Reached checkpoint: \"%s\"",  checkpointName)
                if self.checkpointManager:
                    self.checkpointManager.saveCheckpoint(checkpointName)

    def setCheckpointManager(self, checkpointManager):
        """ Sets a custom Checkpoint Manager. """
        util.raiseIfNone("checkpointManager", checkpointManager)
        logger.info("Setting custom checkpoint manager: %s", checkpointManager)
        self.checkpointManager = checkpointManager
        logger.info("Loading resume checkpoint from checkpoint manager...")
        self.setResumeCheckpoint(checkpointManager.loadCheckpoint())

    def setResumeCheckpoint(self, resumeCheckpoint):
        """ Sets the checkpoint that must be hit for executes to not
         be skipped."""
        self.resumeFromCheckpoint = resumeCheckpoint
        if resumeCheckpoint:
            logger.info(
                "Resume checkpoint changed to \"%s\".  Skipping all calls to "
                "execute until checkpoint is reached.",
                self.resumeFromCheckpoint)
            self.skip = True
        else:
            self.resumeFromCheckpoint = None
            if self.skip:
                self.skip = False
                logger.info(
                    "Resume checkpoint cleared.  Execute calls will "
                    "no longer be skipped.")
            else:
                logger.info(
                    "No resume checkpoint set, continuing execution...")

    def _initLogging(self, logDir, logFile, logConsole, level, logRetention,
                     logMsgs):
        """Initialize UdaExec logging"""
        if not os.path.exists(logDir):
            os.makedirs(logDir)
        self._cleanupLogs(logDir, logRetention, logMsgs)
        self.logDir = os.path.realpath(logDir)
        self.logFile = os.path.join(self.logDir, logFile)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh = logging.FileHandler(self.logFile, mode="a", encoding="utf8")
        fh.setFormatter(formatter)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        root = logging.getLogger()
        if level != logging.NOTSET:
            root.setLevel(level)
        root.addHandler(fh)
        if logConsole:
            root.addHandler(sh)
        sys.excepthook = handleUncaughtException

    def _cleanupLogs(self, logDir, logRetention, logMsgs):
        """Cleanup older log files."""
        logMsgs.append(
            (logging.INFO,
             "Cleaning up log files older than {} days.".format(logRetention)))
        cutoff = time.time() - (logRetention * 86400)
        count = 0
        for f in os.listdir(logDir):
            f = os.path.join(logDir, f)
            if os.stat(f).st_mtime < cutoff:
                logMsgs.append(
                    (logging.DEBUG, "Removing log file: {}".format(f)))
                os.remove(f)
                count += 1
        logMsgs.append((logging.INFO, "Removed {} log files.".format(count)))

    def _initRunNumber(self, runNumberFile, runNumber, logMsgs):
        """Initialize the run number unique to this particular execution."""
        if runNumber is not None:
            self.runNumber = runNumber
            logMsgs.append(
                (logging.INFO, "Setting run number to {}.".format(runNumber)))
        else:
            self.runNumber = "1"
            self.runNumberFile = self.config.resolve(
                runNumberFile, default='.runNumber')
            self.runNumberFile = os.path.abspath(self.runNumberFile)
            if os.path.isfile(self.runNumberFile):
                logMsgs.append(
                    (logging.INFO, "Found run number "
                     "file: \"{}\"".format(self.runNumberFile)))
                with open(self.runNumberFile, "r") as f:
                    self.runNumber = f.readline()
                if self.runNumber is not None:
                    try:
                        self.runNumber = str(int(self.runNumber) + 1)
                    except:
                        logMsgs.append(
                            (logging.WARN, "Unable to increment run "
                             "number ({}) in {}. Resetting run number "
                             "to 1.".format(self.runNumber,
                                            self.runNumberFile)))
                        self.runNumber = "1"
                else:
                    logMsgs.append(
                        (logging.WARN, "No run number found in {}. Resetting "
                         "run number to 1.".format(self.runNumberFile)))
            else:
                logMsgs.append(
                    (logging.INFO, "No previous run number found as {} does "
                     "not exist. Initializing run number to 1".format(
                         self.runNumberFile)))
            with open(self.runNumberFile, 'w') as f:
                f.write(self.runNumber)
            self.runNumber = datetime.datetime.now().strftime(
                "%Y%m%d%H%M%S") + "-" + self.runNumber
        self.config['runNumber'] = self.runNumber

    def _initCheckpoint(self, checkpointFile):
        """Initialize the result checkpoint."""
        self.currentCheckpoint = None
        self.skip = False
        if checkpointFile:
            checkpointFile = self.config.resolve(
                checkpointFile, default=self.config['appName'] +
                ".checkpoint")
        if checkpointFile:
            checkpointFile = os.path.abspath(checkpointFile)
            self.checkpointManager = UdaExecCheckpointManagerFileImpl(
                checkpointFile)
            self.resumeFromCheckpoint = self.checkpointManager.loadCheckpoint()
            if self.resumeFromCheckpoint:
                logger.info(
                    "Resuming from checkpoint \"%s\".",
                    self.resumeFromCheckpoint)
                self.skip = True
            else:
                logger.info("No previous checkpoint found, executing "
                            "from beginning...")
                self.skip = False
        else:
            self.checkpointManager = None
            self.resumeFromCheckpoint = None
            logger.info("Checkpoint file disabled.")

    def _initVersion(self, version, gitPath):
        """Initialize the version and GIT revision."""
        if not gitPath:
            gitPath = "git"
            logger.debug("Git path not specified, using system path.")
        self.gitVersion = None
        self.gitRevision = None
        self.gitDirty = None
        try:
            self.gitVersion = subprocess.check_output(
                [gitPath, "--version"],
                stderr=subprocess.STDOUT).decode("utf-8").strip()
            self.gitRevision = subprocess.check_output(
                [gitPath, "describe", "--tags", "--always", "HEAD"],
                stderr=subprocess.STDOUT).decode("utf-8").strip()
            self.modifiedFiles = subprocess.check_output(
                [gitPath, "status", "--porcelain"],
                stderr=subprocess.STDOUT).decode("utf-8").splitlines()
            self.gitDirty = True if self.modifiedFiles else False
        except subprocess.CalledProcessError as e:
            logger.debug(
                "Git information is not available: %s.",
                e.output.decode("utf-8"))
        except Exception as e:
            logger.debug("Git is not available: %s", e)
        if not version:
            version = self.gitRevision
        if not version:
            raise api.InterfaceError(
                api.CONFIG_ERROR, "version is a required field, it must be "
                "passed in as a parameter, specified in a config file, "
                "or pulled from a git repository.")
        self.config['version'] = version

    def _initQueryBands(self, production):
        """Initialize the Query Band that will be set on future connections."""
        self.queryBands = collections.OrderedDict()
        self.queryBands['ApplicationName'] = self.config['appName']
        self.queryBands['Version'] = self.config['version']
        self.queryBands['JobID'] = self.runNumber
        self.queryBands['ClientUser'] = getpass.getuser()
        self.queryBands['Production'] = util.booleanValue(production)
        if self.configureLogging:
            self.queryBands['udaAppLogFile'] = self.logFile
        if self.gitRevision:
            self.queryBands['gitRevision'] = self.gitRevision
        if self.gitDirty is not None:
            self.queryBands['gitDirty'] = self.gitDirty
        self.queryBands['UtilityName'] = 'PyTd'
        self.queryBands['UtilityVersion'] = __version__

    def __str__(self):
        value = u"Execution Details:\n/"
        value += u'*' * 80
        value += u"\n"
        value += u" * Application Name: {}\n".format(
            toUnicode(self.config['appName']))
        value += u" *          Version: {}\n".format(
            toUnicode(self.config['version']))
        value += u" *       Run Number: {}\n".format(toUnicode(self.runNumber))
        value += u" *             Host: {}\n".format(
            toUnicode(platform.node()))
        value += u" *         Platform: {}\n".format(
            platform.platform(aliased=True))
        value += u" *          OS User: {}\n".format(
            toUnicode(getpass.getuser()))
        value += u" *   Python Version: {}\n".format(platform.python_version())
        value += u" *  Python Compiler: {}\n".format(
            platform.python_compiler())
        value += u" *     Python Build: {}\n".format(platform.python_build())
        value += u" *  UdaExec Version: {}\n".format(__version__)
        value += u" *     Program Name: {}\n".format(toUnicode(sys.argv[0]))
        value += u" *      Working Dir: {}\n".format(toUnicode(os.getcwd()))
        if self.gitRevision:
            value += u" *      Git Version: {}\n".format(self.gitVersion)
            value += u" *     Git Revision: {}\n".format(self.gitRevision)
            value += u" *        Git Dirty: {} {}\n".format(
                self.gitDirty, "" if not self.gitDirty else "[" +
                ",".join(self.modifiedFiles) + "]")
        if self.configureLogging:
            value += u" *          Log Dir: {}\n".format(
                toUnicode(self.logDir))
            value += u" *         Log File: {}\n".format(
                toUnicode(self.logFile))
        value += u" *     Config Files: {}\n".format(
            toUnicode(self.config.configFiles))
        value += u" *      Query Bands: {}\n".format(
            u";".join(u"{}={}".format(toUnicode(k), toUnicode(v))
                      for k, v in self.queryBands.items()))
        value += '*' * 80
        value += '/'
        return value


def _appendConfigFiles(configFiles, *args):
    for arg in args:
        if arg is None:
            continue
        if util.isString(arg):
            configFiles.append(arg)
        else:
            configFiles.extend(arg)


class UdaExecCheckpointManager:

    """ Manages the initialization and saving of checkpoints. """

    def loadCheckpoint(self):
        """ Return the checkpoint name that we should resume from. """
        raise NotImplementedError(
            "loadCheckpoint must be implemented by sub-class")

    def saveCheckpoint(self, checkpointName):
        """ Save the specified checkpoint """
        raise NotImplementedError(
            "raiseCheckpoint must be implemented by sub-class")

    def clearCheckpoint(self):
        """ Remove the checkpoint so that the application starts from beginning
         next time around. """
        raise NotImplementedError(
            "clearCheckpoint must be implemented by sub-class")


class UdaExecCheckpointManagerFileImpl (UdaExecCheckpointManager):

    """ Implementation of the UdaExecCheckpointMananer using a local file."""

    def __init__(self, f):
        self.file = f

    def loadCheckpoint(self):
        resumeFromCheckpoint = None
        if os.path.isfile(self.file):
            logger.info(u"Found checkpoint file: \"%s\"", toUnicode(self.file))
            with open(self.file, "r") as f:
                resumeFromCheckpoint = f.readline()
            if not resumeFromCheckpoint:
                logger.warn(
                    u"No checkpoint found in %s.", toUnicode(self.file))
        else:
            logger.info(u"Checkpoint file not found: %s", toUnicode(self.file))
        return resumeFromCheckpoint

    def saveCheckpoint(self, checkpointName):
        logger.info(
            "Saving checkpoint \"%s\" to %s.", checkpointName, self.file)
        with open(self.file, 'w') as f:
            f.write(checkpointName)

    def clearCheckpoint(self):
        logger.info("Removing checkpoint file %s.", self.file)
        if os.path.isfile(self.file):
            os.remove(self.file)


class UdaExecTemplate (string.Template):

    """Template used by UdaExec configuration and token replacement."""
    idpattern = r'[a-z][_a-z0-9\.]*'


class UdaExecConfig:

    """UdaExec configuration loader and resolver."""

    def __init__(self, configFiles, encoding, configSection, parseCmdLineArgs,
                 logMsgs):
        configParser = configparser.ConfigParser()
        configParser.optionxform = str
        configFiles = [os.path.expanduser(f) for f in configFiles]
        self.configFiles = [toUnicode(os.path.abspath(
            f)) + (": Found" if os.path.isfile(f) else ": Not Found")
            for f in configFiles]
        logMsgs.append(
            (logging.INFO,
             "Reading config files: {}".format(self.configFiles)))
        if sys.version_info[0] == 2:
            for f in configFiles:
                if os.path.isfile(f):
                    configParser.readfp(codecs.open(f, "r", encoding))
        else:
            configParser.read(configFiles, encoding)
        self.configSection = configSection
        self.sections = {configSection: {}}
        for section in configParser.sections():
            self.sections[section] = dict(configParser.items(section))
        if parseCmdLineArgs:
            for arg in sys.argv:
                if arg.startswith('--') and '=' in arg:
                    (key, val) = arg.split("=", 1)
                    key = key[2:]
                    logMsgs.append(
                        (logging.DEBUG, u"Configuration value was set via "
                         "command line: {}={}".format(toUnicode(key),
                                                      toUnicode(val))))
                    self.sections[configSection][key] = val

    def __iter__(self):
        return iter(self.sections[self.configSection])

    def contains(self, option):
        return option in self.sections[self.configSection]

    def resolveDict(self, d, sections=None):
        if sections is None:
            sections = [self.configSection]
        for key, value in d.items():
            if util.isString(value):
                d[key] = self._resolve(value, sections, None, None)
        return d

    def resolve(self, value, sections=None, default=None, errorMsg=None):
        if value is None:
            if errorMsg is not None:
                raise api.InterfaceError(api.CONFIG_ERROR, errorMsg)
            else:
                util.raiseIfNone("value", value)
        if not util.isString(value):
            return value
        if sections is None:
            sections = [self.configSection]
        return self._resolve(value, sections, default, errorMsg)

    def _resolve(self, value, sections, default, errorMsg):
        error = None
        for section in sections:
            try:
                s = self.sections[section]
                newValue = UdaExecTemplate(
                    value.replace("$$", "$$$$")).substitute(**s)
                if value != newValue:
                    value = self._resolve(newValue, sections, None, errorMsg)
                else:
                    value = value.replace("$$", "$")
                error = None
                break
            except (ValueError, KeyError) as e:
                error = e
        if error is not None:
            if default is not None:
                return default
            if errorMsg is not None:
                raise api.InterfaceError(api.CONFIG_ERROR, errorMsg)
            else:
                raise api.InterfaceError(
                    api.CONFIG_ERROR, "Unable to resolve \"{}\".  "
                    "Parameter not found: {}.  "
                    "If parameter substitution is not intended, "
                    "escape '$' by adding another '$'.".format(value, error))
        return value

    def section(self, section):
        try:
            return self.resolveDict(self.sections[section].copy(),
                                    (section, self.configSection))
        except KeyError:
            return None

    def __getitem__(self, key):
        return self.resolve(self.sections[self.configSection][key])

    def __setitem__(self, key, value):
        self.sections[self.configSection][key] = value

    def __str__(self):
        length = 0
        for key in self.sections[self.configSection]:
            keyLength = len(key)
            if keyLength > length:
                length = keyLength
        value = u"Configuration Details:\n/"
        value += u'*' * 80
        value += u"\n"
        for key in sorted(self.sections[self.configSection]):
            value += u" * {}: {}\n".format(toUnicode(key.rjust(length)),
                                           toUnicode(
                                               self.resolve("${" + key + "}"))
                                           if 'password' not in key.lower()
                                           else u'XXXX')
        value += '*' * 80
        value += '/'
        return value


class UdaExecConnection:

    """A UdaExec connection wrapper for ODBC or REST connections."""

    def __init__(self, udaexec, conn):
        self.udaexec = udaexec
        self.conn = conn
        self.internalCursor = self.cursor()

    def close(self):
        self.internalCursor.close()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def cursor(self):
        return UdaExecCursor(self.udaexec, self.conn.cursor())

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        self.close()

    def callproc(self, procname, params, **kwargs):
        return self.internalCursor.callproc(procname, params, **kwargs)

    def execute(self, query=None, params=None, **kwargs):
        self.internalCursor.execute(query, params, **kwargs)
        return self.internalCursor

    def executemany(self, query, params, **kwargs):
        self.internalCursor.executemany(query, params, **kwargs)
        return self.internalCursor


class UdaExecCursor:

    """A UdaExec cursor wrapper for ODBC or REST cursors."""

    def __init__(self, udaexec, cursor):
        self.udaexec = udaexec
        self.cursor = cursor
        self.skip = False
        self.description = None
        self.types = None
        self.arraysize = 1
        self.rowcount = -1
        self.error = None

    def __setattr__(self, name, value):
        self.__dict__[name] = value
        if name == "arraysize":
            self.cursor.arraysize = value

    def callproc(self, procname, params, runAlways=False,
                 continueOnError=False, ignoreErrors=[], **kwargs):
        self.error = None
        self.skip = self.udaexec.skip and not runAlways
        if not self.skip:
            start = time.time()
            try:
                procname = self.udaexec.config.resolve(procname)
                outparams = self.cursor.callproc(procname, params, **kwargs)
                duration = time.time() - start
                logger.info(
                    "Procedure Successful. Duration: %.3f seconds, "
                    "Procedure: %s, Params: %s", duration, procname, params)
                return outparams
            except Exception as e:
                duration = time.time() - start
                self.error = e
                if isinstance(e, api.DatabaseError) and e.code in ignoreErrors:
                    logger.error(
                        "Procedure Failed! Duration: %.3f seconds, "
                        "Procedure: %s, Params: %s, Error Ignored: ",
                        duration, procname, params, e)
                else:
                    logger.exception(
                        "Procedure Failed! Duration: %.3f seconds, "
                        "Procedure: %s, Params: %s", duration,
                        procname, params)
                    if not continueOnError:
                        raise
        else:
            logger.info(
                "Skipping procedure, haven't reached resume checkpoint yet. "
                "Procedure:  %s", procname)

    def close(self):
        self.cursor.close()

    def execute(self, query=None, params=None, file=None, fileType=None,
                delimiter=";", **kwargs):
        if file is None:
            util.raiseIfNone("query", query)
        if query is not None:
            if util.isString(query):
                self._execute(self.cursor.execute, query, params, **kwargs)
            else:
                for q in query:
                    self._execute(self.cursor.execute, q, params, **kwargs)
        if file is not None:
            self._executeFile(file, params, fileType, delimiter, **kwargs)
        return self

    def executemany(self, query, params, **kwargs):
        self._execute(self.cursor.executemany, query, params, **kwargs)
        return self

    def _executeFile(self, file, params, fileType, delimiter, runAlways=False,
                     **kwargs):
        self.skip = self.udaexec.skip and not runAlways
        if not self.skip:
            file = self.udaexec.config.resolve(file)
            if fileType is None:
                script = util.SqlScript(file, delimiter)
            elif fileType == "bteq":
                script = util.BteqScript(file)
            else:
                raise api.InterfaceError(
                    "UNKNOWN_FILE_TYPE",
                    "The file type '{}' is not unknown".format(fileType))
            for query in script:
                self._execute(
                    self.cursor.execute, query, params, runAlways, **kwargs)
        else:
            logger.info(
                "Skipping file, haven't reached resume checkpoint yet. "
                "File:  %s", file)

    def _execute(self, func, query, params, runAlways=False,
                 continueOnError=False, logParamFrequency=1,
                 logParamCharLimit=80, ignoreErrors=[],
                 **kwargs):
        self.error = None
        self.skip = self.udaexec.skip and not runAlways
        if not self.skip:
            start = time.time()
            paramStr = _getParamsString(params, logParamFrequency,
                                        logParamCharLimit)
            try:
                query = self.udaexec.config.resolve(query)
                func(query, params, **kwargs)
                self.description = self.cursor.description
                self.types = self.cursor.types
                self.rowcount = self.cursor.rowcount
                duration = time.time() - start
                rowsStr = " " if self.cursor.rowcount < 0 else \
                    " Rows: %s, " % self.cursor.rowcount
                logger.info(
                    "Query Successful. Duration: %.3f seconds,%sQuery: %s%s",
                    duration, rowsStr, query, paramStr)
            except Exception as e:
                self.description = None
                self.types = None
                self.rowcount = -1
                duration = time.time() - start
                self.error = e
                if isinstance(e, api.DatabaseError) and e.code in ignoreErrors:
                    logger.error(
                        "Query Failed! Duration: %.3f seconds, Query: %s%s, "
                        "Error Ignored: %s", duration, query, paramStr, e)
                else:
                    logger.exception(
                        "Query Failed! Duration: %.3f seconds, Query: %s%s",
                        duration, query, paramStr)
                    if not continueOnError:
                        raise
        else:
            logger.info(
                "Skipping query, haven't reached resume checkpoint yet.  "
                "Query:  %s", query)

    def fetchone(self):
        if self.skip:
            return None
        return self.cursor.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        if self.skip:
            return []
        return self.cursor.fetchmany(size)

    def fetchall(self):
        if self.skip:
            return []
        return self.cursor.fetchall()

    def nextset(self):
        if self.skip:
            return None
        return self.cursor.nextset()

    def setinputsizes(self, sizes):
        self.cursor.setinputsizes(self, sizes)

    def setoutputsize(self, size, column=None):
        self.cursor.setoutputsizes(self, size)

    def __iter__(self):
        return self

    def __next__(self):
        if self.skip:
            raise StopIteration()
        return self.cursor.__next__()

    def next(self):
        return self.__next__()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()


def _getParamsString(params, logParamFrequency=1, logParamCharLimit=80):
    paramsStr = ""
    if params and logParamFrequency > 0:
        if isinstance(params[0], (list, tuple)):
            index = 0
            paramsStr = []
            for p in params:
                index += 1
                if index == 1 or index % logParamFrequency == 0:
                    paramsStr.append(_getParamString(p, logParamCharLimit,
                                                     index))
            if index != 1 and index % logParamFrequency != 0:
                paramsStr.append(_getParamString(p, logParamCharLimit, index))
            paramsStr = u", Params: {}".format(
                u"\n".join(paramsStr))
        else:
            paramsStr = u", Params: {}".format(_getParamString(
                params, logParamCharLimit))
    return paramsStr


def _getParamString(params, logParamCharLimit=80, index=None):
    paramsStr = []
    for p in params:
        p = repr(p)
        if logParamCharLimit > 0 and len(p) > logParamCharLimit:
            p = (p[:(logParamCharLimit)] + '...')
        paramsStr.append(p)
    prefix = u"["
    if index is not None:
        prefix = u"%s:[" % index
    return prefix + u",".join(paramsStr) + u"]"
