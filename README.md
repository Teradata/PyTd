
## Teradata Python Module

<div>

The Teradata Python Module is a freely available, open source, library
for the Python programming language, whose aim is to make it easy to
script powerful interactions with Teradata Database. It adopts the
philosophy of
[udaSQL](/tools/articles/udasql-a-devops-focused-sql-execution-engine),
providing a DevOps focused SQL Execution Engine that allows developers
to focus on their SQL and procedural logic without worrying about
Operational requirements such as external configuration, query banding,
and logging.

This package requires 64-bit Python 3.4 or later, and runs on Windows, macOS, and Linux. 32-bit Python is not supported.

The Teradata Python Module is released under an MIT license. The source
is available on [GitHub](https://github.com/Teradata/PyTd) and the
package is available for download and install from
[PyPI](https://pypi.python.org/pypi/teradata). This module is open
source and therefore uses the [Community
Support](https://support.teradata.com/community) model.

For Teradata customer support, please visit [Teradata Access](https://access.teradata.com/).

</div>

### Table of Contents
---
* [1.0 Getting Started](#GettingStarted)
  + [1.1 Documentation](#Documentation)
  + [1.2 Installing the Teradata Python Module](#Installing)
  + [1.3 Connectivity Options](#Connectivity)
  + [1.4 Hello World Example](#HelloWorld)
* [2.0 DevOps Features](#DevOps)
  + [2.1 External Configuration](#ExternalConfiguraton)
  + [2.2 Logging](#Logging)
  + [2.3 Checkpoints](#Checkpoints)
  + [2.4 Query Banding](#QueryBands)
* [3.0 Database Interactions](#DatabaseInteractions)
  + [3.1 Cursors](#Cursors)
  + [3.2 Parameterized SQL](#ParameterizedSQL)
  + [3.3 Stored Procedures](#StoredProcedures)
  + [3.4 Transactions](#Transactions)
  + [3.5 Data Types](#DataTypes)
  + [3.6 Unicode](#Unicode)
  + [3.7 Ignoring Errors](#IgnoringErrors)
  + [3.8 Password Protection](#PasswordProtection)
  + [3.9 External SQL Scripts](#ExternalConfiguraton)
* [4.0 Reference](#References)
  + [4.1 UdaExec Parameters](#UdaExec%20Parameters)
  + [4.2 Connect Parameters](#ConnectParametrs)
  + [4.3 Execute Parameters](#ExecuteParameters)
* [5.0 Running Unit Tests](#RunningTests)
* [6.0 Migration Guide](#Migration)
  + [6.1 Setup](#MGSetup)
  + [6.2 Database Interactions](#MGDatabase)
  + [6.3 Reference](#MGReferences)

Table of Contents links do not work on PyPI due to a [PyPI limitation](https://github.com/pypa/warehouse/issues/4064).

---
<a name="GettingStarted"></a>

### **1.0 Getting Started**

The following sections run through documentation, installation, connectivity options,
and a simple Hello World example.

<a name="Documentation"></a>

#### **1.1 Documentation**
---
When the Teradata Python Module is installed, the `README.md` file is placed in the `teradata` directory under your Python installation directory. This permits you to view the documentation offline, when you are not connected to the internes.

The `README.md` file is a plain text file containing the documentation for the Teradata Python Module. While the file can be viewed with any text file viewer or editor, your viewing experience will be best with an editor that understands Markdown format.

<a name="Installing"></a>

#### **1.2 Installing the Teradata Python Module**
---
The Teradata Python Module depends on the `teradatasql` package which is available from PyPI.

Use `pip install` to download and install the Teradata Python Module and its dependencies automatically.

Platform       | Command
-------------- | ---
macOS or Linux | `pip install teradata`
Windows        | `py -3 -m pip install teradata`

When upgrading to a new version of the Teradata Python Module, you may need to use pip install's `--no-cache-dir` option to force the download of the new version.

Platform       | Command
-------------- | ---
macOS or Linux | `pip install --no-cache-dir -U teradata`
Windows        | `py -3 -m pip install --no-cache-dir -U teradata`

If you don't have pip installed, you can download the package from
[PyPI](https://pypi.python.org/pypi/teradata), unzip the folder, then
double click the setup.py file or run `setup.py install`.

<a name="Connectivity"></a>

#### **1.3 Connectivity Options**
---
The Teradata Python Module uses Teradata SQL Driver for Python to connect to the Teradata Database.

<a name="HelloWorld"></a>

#### **1.4 Hello World Example**
---
In this example, we will connect to a Teradata Database and run a simple
query to fetch the Query Band information for the session that we
create.

**Example 1 - HelloWorld.py**

<span style="font-family:Consolas; font-size:.75em;">

``` {.brush:python;}
import teradata

udaExec = teradata.UdaExec (appName="HelloWorld", version="1.0",
        logConsole=False)

session = udaExec.connect(system="tdprod",
        username="xxx", password="xxx");

for row in session.execute("SELECT GetQueryBand()"):
    print(row)
```
</span>

Let's break the example down line by line. The first line, "import
teradata", imports the Teradata Python Module for use in the script.

The second line initializes the "UdaExec" framework that provides DevOps
support features such as configuration and logging. We tell UdaExec the
name and version of our application during initialization so that we can
get feedback about our application in DBQL and Teradata Viewpoint as
this information is included in the QueryBand of all Database sessions
created by our script. We also tell UdaExec not to log to the console
(e.g. logConsole=False) so that our print statement is easier to read.

The third line creates a connection to a Teradata system named "tdprod".
The last line executes the "SELECT
GetQueryBand()" SQL statement and iterates over the results, printing
each row returned. Since "SELECT GetQueryBand()" statement only returns
one row, only one row is printed.

Let's go ahead and run the script by executing "python HelloWorld.py".
Below is the result:

<span style="font-family:Consolas; font-size:.7em;">

    Row 1: [=S> ApplicationName=HelloWorld;Version=1.0;JobID=20201208153806-1;ClientUser=example;Production=False;udaAppLogFile=/home/hd121024/devel/pydbapi-81-pytd/logs/HelloWorld.20201208153806-1.log;UtilityName=PyTd;UtilityVersion=15.10.0.22;]

</span>

From the output, we see that one row was returned with a single string
column. We also see quite a bit of information was added to the
QueryBand of the session we created. We can see the application name and
version we specified when initializing UdaExec. If we look at this location
on the file system we can see the log file that was generated:

<span style="font-family:Consolas; font-size:.7em;">

``` {.brush:bash;}
2020-12-08 15:38:06,204 - teradata.udaexec - INFO - Initializing UdaExec...
2020-12-08 15:38:06,204 - teradata.udaexec - INFO - Reading config files: ['/etc/udaexec.ini: Not Found', '/home/example/udaexec.ini: Not Found', '/home/example/pytd/udaexec.ini: Not Found']
2020-12-08 15:38:06,204 - teradata.udaexec - INFO - No previous run number found as /home/example/pytd/.runNumber does not exist. Initializing run number to 1
2020-12-08 15:38:06,204 - teradata.udaexec - INFO - Cleaning up log files older than 90 days.
2020-12-08 15:38:06,204 - teradata.udaexec - INFO - Removed 0 log files.
2020-12-08 15:38:06,237 - teradata.udaexec - INFO - Checkpoint file not found: /home/example/pytd/HelloWorld.checkpoint
2020-12-08 15:38:06,237 - teradata.udaexec - INFO - No previous checkpoint found, executing from beginning...
2020-12-08 15:38:06,237 - teradata.udaexec - INFO - Execution Details:
/********************************************************************************
 * Application Name: HelloWorld
 *          Version: 1.0
 *       Run Number: 20201208153806-1
 *             Host: sdl52261
 *         Platform: Linux-4.4.73-5-default-x86_64-with-SuSE-12-x86_64
 *          OS User: example
 *   Python Version: 3.4.6
 *  Python Compiler: GCC
 *     Python Build: ('default', 'Mar 01 2017 16:52:22')
 *  UdaExec Version: 15.10.0.22
 *     Program Name:
 *      Working Dir: /home/example/pytd
 *          Log Dir: /home/example/pytd/logs
 *         Log File: /home/example/pytd/logs/HelloWorld.20201208153806-1.log
 *     Config Files: ['/etc/udaexec.ini: Not Found', '/home/example/udaexec.ini: Not Found', '/home/example/pytd/udaexec.ini: Not Found']
 *      Query Bands: ApplicationName=HelloWorld;Version=1.0;JobID=20201208153806-1;ClientUser=example;Production=False;udaAppLogFile=/home/example/pytd/logs/HelloWorld.20201208153806-1.log;UtilityName=PyTd;UtilityVersion=15.10.0.22
********************************************************************************/
2020-12-08 15:38:35,290 - teradata.udaexec - INFO - Creating connection: {'password': 'XXXXXX', 'username': 'guest', 'system': 'tdprod'}
2020-12-08 15:38:35,498 - teradata.udaexec - INFO - Connection successful. Duration: 0.208 seconds. Details: {'password': 'XXXXXX', 'us
ername': 'guest', 'system': 'tdprod'}
2020-12-08 15:39:02,167 - teradata.udaexec - INFO - Query Successful. Duration: 0.007 seconds, Rows: 1, Query: SELECT GetQueryBand()
2020-12-08 15:40:43,349 - teradata.udaexec - INFO - UdaExec exiting.
```

</span>

In the logs, you can see connection information and all the SQL
statements submitted along with their durations. If any errors had
occurred, those would have been logged too.

Explicitly closing resources when done is always a good idea. In the
next sections, we show how this can be done automatically using the "with"
statement.

---

<a name="DevOps"></a>

### **2.0 DevOps Features**

The following sections discuss the DevOps oriented features provided by
the Teradata Python Module. These features help simplify development and
provide the feedback developers need once their applications are put
into QA and production.

<a name="ExternalConfiguraton"></a>

#### **2.1 External Configuration**
---
In the first "Hello World" example, we depended on no external
configuration information for our script to run. What if we now wanted
to run our HelloWorld.py script against a different database system? We
would need to modify the source of our script, which is somewhat
inconvenient and error prone. Luckily the UdaExec framework makes it
easy to maintain configuration information outside of our source code.

**Example 2 -- PrintTableRows.py**

``` {.brush:python;}
import teradata

udaExec = teradata.UdaExec ()

with udaExec.connect("${dataSourceName}") as session:
    for row in session.execute("SELECT * FROM ${table}"):
        print(row)
```

In this example, we remove all the hard coded configuration data and
instead load our configuration parameters from external configuration
files. We also call connect using the "with" statement so that the
connection is closed after use even when exceptions are raised.

You may be wondering what *\${dataSourceName}* means above. Well, a
dollar sign followed by optional curly braces means replace
*\${whatever}* with the value of the external configuration variable
named "whatever". In this example, we make a connection to a data source
whose name and configuration is defined outside of our script. We then
perform a SELECT on a table whose name is also configured outside of our
script.

UdaExec allows any SQL statement to make reference to an external
configuration parameter using the dollar sign/curly brace syntax. When
actually wanting to include a "\$" literal in a SQL statement that isn't
a parameter substitution, you must escape the dollar sign with another
dollar sign (e.g. "\$\$").

Here is our external configuration file that we name "udaexec.ini" and
place in the same directory as our python script.

**Example 2 - udaexec.ini**

``` {.brush:bash;}
# Application Configuration
[CONFIG]
appName=PrintTableRows
version=2
logConsole=False
dataSourceName=TDPROD
table=DBC.DBCInfo

# Default Data Source Configuration
[DEFAULT]
system=tdprod
username=xxx
password=xxx

# Data Source Definition
[TDPROD]
username=xxx
password=xxx
```

An external configuration file should contain one section named "CONFIG"
that contains application configuration name/value pairs, a section
named "DEFAULT" that contains default data source name/value pairs, and
one or more user defined sections that contain data source name/value
pairs.

In this example, we are connecting to *\${dataSourceName}*, which
resolves to "TDPROD" as dataSourceName and is a property in the CONFIG
section. The TDPROD data source is defined in our configuration file and
provides the name of the
username and password. It also inherits the properties in the DEFAULT
section, which in this case, defines system, username and password. The
username and password in the TDPROD section will override the username
and password defined in the DEFAULT section.

You'll notice in this example we didn't specify the "appName" and
"version" when initializing UdaExec. If you look at the method signature
for UdaExec, you'll see that the default values for appName and version
are "\${appName}" and "\${version}". When not specified as method
arguments, these values are looked up in the external configuration.
This is true for almost all configuration parameters that can be passed
to the UdaExec constructor so that any setting can be set or changed
without changing your code.

If we run the example script above using "python PrintTableRows.py", we
get the following output:

    Row 1: [VERSION, 17.10c.00.35]
    Row 2: [LANGUAGE SUPPORT MODE, Standard]
    Row 3: [RELEASE, 17.10c.00.30]

Looking at the generated log file, we see the following log entry:

    2020-12-08 17:21:32,307 - teradata.udaexec - INFO - Reading config files: ['C:\\etc\\udaexec.ini: Not Found', 'C:\\Users\\example\\udaexec.ini: Found', 'C:\\Users\\example\\udaexec.ini: Found']

As you can see, UdaExec is attempting to load external configuration
from multiple files. By default, UdaExec looks for a system specific
configuration file, a user specific configuration file, and an
application specific configuration file. The location of these files can
be specified as arguments to the UdaExec constructor. Below are the
argument names along with their default values.

**Table 1 -- Config File Locations**

  | **Name**               | **Description**                                                   | **Default Value** |
  |------------------------|-------------------------------------------------------------------|-------------------|
  | **systemConfigFile**   | The system wide configuration file(s). Can be a single value or a list.             | *\"/etc/udaexec.ini\"*|
  | **userConfigFile**     | The user specific configuration file(s). Can be a single value or a list.           | *\"\~/udaexec.ini\" or \"%HOMEPATH%/udaexec.ini\"*|
  | **appConfigFile**      | The application specific configuration file (s). Can be a single value or a list.   | *\"udaexec.ini\"*|
  ---------------------- ----------------------------------------------------------------------------------- ----------------------------------------------------

Configuration data is loaded in the order shown above, from least
specific to most specific, with later configuration files overriding the
values specified by earlier configuration files when conflicts occur.

If we had wanted to name our configuration file in this example
"PrintTableRows.ini" instead of "udaexec.ini", then we could've
specified that when creating the UdaExec object. E.g.

``` {.brush:python;}
udaExec = teradata.UdaExec (appConfigFile="PrintTableRows.ini")
```

If we wanted to have multiple application configuration files, then we
could've specified a list of file names instead. E.g.

``` {.brush:python;}
udaExec = teradata.UdaExec (appConfigFile=["PrintTableRows.ini", "PrintTableRows2.ini"])
```

If you find that even that isn't flexible enough, you can always
override the external configuration file list used by UdaExec by passing
it in the "configFiles" argument. When the "configFiles" list is
specified, systemConfigFile, userConfigFile, and appConfigFile values
are ignored.

In addition to using external configuration files, application
configuration options can also be specified via the command line. If we
wanted to change the table name we select from in the example above, we
can specify the table value on the command line e.g. "python
PrintTableRows.py \--table=ExampleTable" which would instead print the
rows of a table named "ExampleTable". Configuration options specified on
the command line override those in external configuration files. UdaExec
has a parameter named "parseCmdLineArgs" that is True by default. You
can set this value to False to prevent command line arguments from being
included as part of the UdaExec configuration.

Sometimes it may be necessary to get or set UdaExec application
configuration parameters in the code directly. You can do this by using
the "config" dictionary-like object on the UdaExec instance. E.g.

``` {.brush:sql;}
udaExec = teradata.UdaExec ()
print(udaExec.config["table"])
udaExec.config["table"] = "ExampleTable"
```

As you can see, using external configuration makes it easy to write
scripts that are reasonably generic and that can execute in a variety of
environments. The same script can be executed against a Dev, Test, and
Prod environment with no changes, making it easier to adopt and automate
a DevOps workflow.

<a name="Checkpoints"></a>

#### **2.2 Logging**
---
The UdaExec object automatically enables logging when it is initialized.
Logging is implemented using Python's standard logging module. If you
create a logger in your script, your custom log messages will also be
logged along with the UdaExec log messages.

By default, each execution of a script that creates the UdaExec object
gets its own unique log file. This has the potential to generate quite a
few files. For this reason, UdaExec also automatically removes log files
that are older than a configurable number of days.

Below is a list of the different logging options and their default
values. Logging options can be specified in the UdaExec constructor, in
the application section of external configuration files, or on the
command line.

**Table 2 -- Logging Options**

  | **Name**            |  **Description**       |              **Default Value**|
  |---------------------|------------------------|-------------------------------|
  | **configureLogging**|  Flags if UdaExec will configure logging. |True        |
  | **logDir**          |  The directory that contains log files.   |*\"logs\"*  |
  | **logFile**         |  The log file name. | *\"\${appName}.\${runNumber}.log\"*|
  | **logLevel**        |  The level that determines what log messages are logged (i.e. CRITICAL, ERROR, WARNING, INFO, TRACE, DEBUG) | *\"INFO\"*|
  | **logConsole**      |  Flags if logs should be written to stdout in addition to the log file.  | True|
  | **logRetention**    |  The number of days to retain log files. Files in the log directory older than the specified number of days are deleted. |  90 |
  ---
If the logging features of UdaExec don't meet the requirements of your
application, then you can configure UdaExec not to configure logging and
instead configure it yourself.

Log messages generated at INFO level contain all the status of all
submitted SQL statements and their durations. If there are problems
during script execution, the log files provide the insight needed to
diagnose any issues. If more information is needed, the log level can be
increased to *\TRACE\"* or *\"DEBUG\"*.

<a name="Checkpoints"></a>

#### **2.3 Checkpoints**
---
When an error occurs during script execution, exceptions get raised that
typically cause the script to exit. Let's suppose you have a script that
performs 4 tasks but it is only able to complete 2 of them before an
unrecoverable exception is raised. In some cases, it would be nice to be
able to re-run the script when the error condition is resolved and have
it automatically resume execution of the 2 remaining tasks. This is
exactly the reason UdaExec includes support for checkpoints.

A checkpoint is simply a string that denotes some point during script
execution. When a checkpoint is reached, UdaExec saves the checkpoint
string off to a file. UdaExec checks for this file during
initialization. If it finds a previous checkpoint, it will ignore all
execute statements until the checkpoint specified in the file is
reached.

**Example 3 - CheckpointExample.py**

``` {.brush:python;}
import teradata

udaExec = teradata.UdaExec()
with udaExec.connect("${dataSourceName}") as session:
    session.execute("-- Task 1")
    udaExec.checkpoint("Task 1 Complete")

    session.execute("-- Task 2")
    udaExec.checkpoint("Task 2 Complete")

    session.execute("-- Task 3")
    udaExec.checkpoint("Task 3 Complete")

    session.execute("-- Task 4")
    udaExec.checkpoint("Task 4 Complete")


# Script completed successfully, clear checkpoint
# so it executes from the beginning next time
udaExec.checkpoint()
```

In the example above, we are calling execute 4 different times and
setting a checkpoint after each call. If we were to re-run the script
after the 3rd execute failed, the first two calls to execute would be
ignored. Below are the related log entries when re-running our
CheckpointExample.py script after the 3rd execute failed.

``` {.brush:bash;}
2015-06-25 14:15:29,017 - teradata.udaexec - INFO - Initializing UdaExec...
2015-06-25 14:15:29,026 - teradata.udaexec - INFO - Found checkpoint file: "/home/example/PyTd/Example3/CheckpointExample.checkpoint"
2015-06-25 14:15:29,027 - teradata.udaexec - INFO - Resuming from checkpoint "Task 2 Complete".
2015-06-25 14:15:29,028 - teradata.udaexec - INFO - Creating connection: {'system': 'tdprod', 'username': 'xxx', 'password': 'XXXXXX', 'dsn': 'TDPROD'}
2015-06-25 14:15:29,250 - teradata.udaexec - INFO - Connection successful. Duration: 0.222 seconds. Details: {'system': 'tdprod', 'username': 'xxx', 'password': 'XXXXXX', 'dsn': 'TDPROD'}
2015-06-25 14:15:29,250 - teradata.udaexec - INFO - Skipping query, haven't reached resume checkpoint yet.  Query:  -- Task 1
2015-06-25 14:15:29,250 - teradata.udaexec - INFO - Skipping query, haven't reached resume checkpoint yet.  Query:  -- Task 2
2015-06-25 14:15:29,250 - teradata.udaexec - INFO - Reached resume checkpoint: "Task 2 Complete".  Resuming execution...
2015-06-25 14:15:29,252 - teradata.udaexec - INFO - Query Successful. Duration: 0.001 seconds, Rows: 0, Query: -- Task 3
2015-06-25 14:15:29,252 - teradata.udaexec - INFO - Reached checkpoint: "Task 3 Complete"
2015-06-25 14:15:29,252 - teradata.udaexec - INFO - Saving checkpoint "Task 3 Complete" to /home/example/PyTd/Example3/CheckpointExample.checkpoint.
2015-06-25 14:15:29,253 - teradata.udaexec - INFO - Query Successful. Duration: 0.001 seconds, Rows: 0, Query: -- Task 4
2015-06-25 14:15:29,254 - teradata.udaexec - INFO - Reached checkpoint: "Task 4 Complete"
2015-06-25 14:15:29,254 - teradata.udaexec - INFO - Saving checkpoint "Task 4 Complete" to /home/example/PyTd/Example3/CheckpointExample.checkpoint.
2015-06-25 14:15:29,328 - teradata.udaexec - INFO - Clearing checkpoint....
2015-06-25 14:15:29,329 - teradata.udaexec - INFO - Removing checkpoint file /home/example/PyTd/Example3/CheckpointExample.checkpoint.
2015-06-25 14:15:29,329 - teradata.udaexec - INFO - UdaExec exiting.
```

As you can see from the logs, all calls to execute are skipped until the
"Task 2 Complete" checkpoint is reached. At the end of our script we
call "udaExec.checkpoint()" without a checkpoint string. This call
clears the checkpoint file so that the next time we run our script, it
will execute from the beginning.

While skipping calls to execute help to resume after an error, there are
situations where this alone will not always work. If the results of a
query are necessary for program execution, then the script may hit
additional errors when being resumed. For example, let's assume our
script now loads a configuration parameter from a table.

``` {.brush:python;}
udaExec.config["mysetting"] = session.execute("SELECT mysetting FROM
    MyConfigTable").fetchone()[0]
```

A call to execute returns a Cursor into a result set, so we call
fetchone()\[0\] to get the first column of the first row in the result
set. If the execute call is skipped, then fetchone() will return None
and the lookup of the first column will fail. There are several ways we
can workaround this problem. The first way is to force execute to run
regardless of checkpoints by specifying the parameter runAlways=True.
E.g.

``` {.brush:python;}
udaExec.config["mysetting"] = session.execute("SELECT mysetting FROM
    MyConfigTable", runAlways=True).fetchone()[0]
```

This is a good approach if we want to set "mysetting" even on resume. If
"mysetting" is not necessary for resume though, then another way to
prevent errors is to check the UdaExec "skip" attribute. E.g.

``` {.brush:python;}
if not udaExec.skip:
    udaExec.config["mysetting"] = session.execute("SELECT mysetting FROM
    MyConfigTable").fetchone()[0]
```

With this approach, we only access the "mysetting" column if execute
will not be skipped.

UdaExec saves checkpoints to a file named *\"\${appName}.checkpoint\"*
located in the same directory the script is executed by default. The
checkpoint file can be changed by specifying the "checkpointFile"
parameter in the UdaExec constructor, in an external configuration file,
or on the command line. To disable file-based checkpoints,
"checkpointFile" can be set to None in the UdaExec constructor or it can
be set to an empty string in an external configuration file.

If it is desirable to load checkpoints from and save checkpoints to a
place other than a local file (e.g. a database table), then a custom
checkpoint manager implementation can be used to handle loading, saving,
and clearing checkpoint details. Below is an example of a custom
checkpoint manager that loads and saves checkpoints to a database table.

``` {.brush:python;}
class MyCheckpointManager (teradata.UdaExecCheckpointManager):
    def __init__(self, session):
    self.session = session
    def loadCheckpoint(self):
        for row in self.session.execute("""SELECT * FROM ${checkPointTable}
                                           WHERE appName = '${appName}'"""):
            return row.checkpointName
    def saveCheckpoint(self, checkpointName):
        self.session.execute("""UPDATE ${checkPointTable} SET checkpointName = ?
                                WHERE appName = '${appName}' ELSE
                                INSERT INTO ${checkPointTable} VALUES ('${appName}', ?)""",
                             (checkpointName, checkpointName))
    def clearCheckpoint(self):
        self.session.execute("""DELETE FROM ${checkPointTable}
                                WHERE appName = '${appName}'""",
                             ignoreErrors=[3802])
```

To use this custom checkpoint manager, you can disable the
checkpointFile and call the setCheckpointManager method on UdaExec. E.g.

``` {.brush:python;}
udaexec = teradata.UdaExec(checkpointFile=None)
with udaexec.connect("${dsn}") as session:
    udaexec.setCheckpointManager(MyCheckpointManager(session))
    # The rest of my program logic.
```

<a name="QueryBands"></a>

#### **2.4 Query Banding**
---
UdaExec automatically sets session Query Bands for any connections you
create so that the runtime characteristics of your application can be
monitored in DBQL and Teradata Viewpoint. Reviewing application log
files along with the associated log entries in DBQL are great ways to
get feedback on the overall execution of your application. The table
below lists the name and descriptions of the Query Bands that are set.

**Table 3 - Query Bands**

  ----------------- -----------------------------------------------------------
  | **Name**          | **Description**                                       |
  |-------------------| ------------------------------------------------------|
  | ApplicationName   | The name of your application                          |
  | Version           | The version of your application                       |
  | JobID             | The run number of this particular execution           |
  | ClientUser        | The OS user name.                                     |
  | Production        | True if a production App, else False                  |
  | udaAppLogFile     | Path of the generated log file                        |
  | gitRevision       | The GIT revision of the application.                  |
  | gitDirty          | True if files have been modified since last commit to GIT|
  | UtilityName       | The nickname of the Teradata Python Module - PyTd     |
  | UtilityVersion    | The version of the Teradata Python Module             |
  ----------------- -----------------------------------------------------------

Additional custom Query Bands can be set by passing a map (dict) as the
queryBand argument to UdaExec.connect().

---
<a name="DatabaseInteractions"></a>

### **3.0 Database Interactions**

UdaExec implements the Python Database API Specification v2.0 while
adding additional convenience on top. The only deviation from this
specification is that UdaExec enables auto commit by default. It is
recommended to review the Python Database API Specification v2.0 first
and then review the following sections for more details.

<a name="Cursors"></a>

#### **3.1 Cursors**
---
Since only a single Cursor is needed most of the time, UdaExec creates
an internal cursor for each call to connect() and allows execute,
executemany, and callproc to be called directly on the connection
object. Calls to these methods on the Connection object simply invoke
those same methods on the internal cursor. The internal cursor is closed
when the connection is closed.

Calls to execute, executemany, and callproc return the Cursor for
convenience. Cursors act as iterators, so the results of an execute call
can easily be iterated over in a "for" loop. Rows act like tuples or
dictionaries, and even allow columns to be accessed by name similar to
attributes on an object. Below is an example. All 3 print statements
print the same thing for each row.

``` {.brush:python;}
import teradata
udaExec = teradata.UdaExec()
with udaExec.connect("${dataSourceName}") as session:
    for row in session.execute("""SELECT InfoKey AS name, InfoData as val
           FROM DBC.DBCInfo"""):
        print(row[0] + ": " + row[1])
        print(row["name"] + ": " + row["val"])
        print(row.name + ": " + row.val)
```

There are situations where it may be necessary to use a separate cursor
in addition to the one created by default. A good example of this is
when wanting to perform queries while iterating over the results of
another query. To accomplish this, two cursors must be used, one to
iterate and one to invoke the additional queries. Below is an example.

``` {.brush:python;}
import teradata
udaExec = teradata.UdaExec()
with udaExec.connect("${dataSourceName}") as session:
    with session.cursor() as cursor:
        for row in cursor.execute("SELECT * from ${tableName}"):
            session.execute("DELETE FROM ${tableName} WHERE id = ?", (row.id, )):
```

Like connections, cursors should be closed when you\'re finished using
them. This is best accomplished using the "with" statement.

<a name="ParameterizedSQL"></a>

#### **3.2 Parameterized SQL**
---
You can pass parameters to SQL statements using the question mark
notation. The following example inserts a row into an employee table.

``` {.brush:python;}
session.execute("""INSERT INTO employee (id, firstName, lastName, dob)
                   VALUES (?, ?, ?, ?)""", (1,"James", "Kirk", "2233-03-22"))
```

To insert multiple rows, executemany can be used. To insert them using
batch mode, pass in the parameter batch=True (default is True). To insert
them one at a time, pass in the parameter batch=False. E.g.

``` {.brush:python;}
session.executemany("""INSERT INTO employee (id, firstName, lastName, dob)
                       VALUES (?, ?, ?, ?)""",
                    ((1,"James", "Kirk", "2233-03-22"),
                     (2,"Jean-Luc", "Picard", "2305-07-13")),
                    batch=True)
```

Batch mode sends all the parameter sequences to the database in a single
"batch" and is much faster than sending the parameter sequences
individually.

<a name="StoredProcedures"></a>

#### **3.3 Stored Procedures**
---
Stored procedures can be invoked using the "callproc" method. OUT
parameters should be specified as teradata.OutParam instances. INOUT
parameters should be specified as teradata.InOutParam instances. IN
parameters can be specified as teradata.InParam instances. An
optional name can be specified with output parameters that can be used
to access the returned parameter by name. E.g.

``` {.brush:python;}
results = session.callproc("MyProcedure", (teradata.InOutParam("inputValue", "inoutVar1"), teradata.OutParam(), teradata.OutParam("outVar2", dataType="PERIOD")))
print(results.inoutVar1)
print(results.outVar1)
```
Additionally, a Teradata data type can be specified for the IN and INOUT
parameters, so that the input parameter is converted to the proper Teradata
data type. A size can be set for OUT and INOUT parameters so that
the output parameter is truncated to the specified size. The size option will only
work for string and byte types and will be ignored for all other types.

<a name="Transactions"></a>

#### **3.4 Transactions**
---
UdaExec enables auto commit by default. To disable auto commit and
instead commit transactions manually, set autoCommit=False on the call
to connect or in the data source's external configuration.

Transactions can be manually committed or rolled back using the commit()
and rollback() methods on the Connection object. E.g.

``` {.brush:python;}
import teradata
udaExec = teradata.UdaExec()
with udaExec.connect("${dataSourceName}", autoCommit=False) as session:
    session.execute("CREATE TABLE ${tableName} (${columns})")
    session.commit()
```

<a name="DataTypes"></a>

#### **3.5 Data Types**
---
The interface that UdaExec uses to perform conversion on the data type
values is called teradata.datatypes.DataTypeConverter with the default
implementation being teradata.datatypes.DefaultDataTypeConverter. If you
would like to customize how data gets converted to Python objects,
you can specify a custom DataTypeConverter during connect. E.g.

``` {.brush:python;}
udaExec.connect("${dataSourceName}", dataTypeConverter=MyDataTypeConverter())
```

It is recommended to derive your custom DataTypeConverter from
DefaultDataTypeConverter so that you can perform conversion for the data
types you're interested in while delegating to the default
implementation for any of the remaining ones.

The table below specifies the data types that get converted by the
DefaultDataTypeConverter. Any data types not in the table below are
returned as a Python Unicode string (e.g. VARCHAR, CLOB, UDT, ARRAY,
etc.)

<a name="DataTypeConversion"></a>
**Table 4 - Data Type Conversions**

  --------------------------------- -----------------------------------
  | **Data Type**                  |  **Python Object**                |
  | -------------------------------|-----------------------------------|
  | BYTE                           |  bytearray                        |
  | VARBYTE                        |  bytearray                        |
  | BYTEINT                        |  decimal.Decimal                  |
  | SMALLINT                       |  decimal.Decimal                  |
  | INTEGER                        |  decimal.Decimal                  |
  | BIGINT                         |  decimal.Decimal                  |
  | REAL, FLOAT, DOUBLE PRECISION  |  decimal.Decimal                  |
  | DECIMAL, NUMERIC               |  decimal.Decimal                  |
  | NUMBER                         |  decimal.Decimal                  |
  | DATE                           |  datetime.date                    |
  | TIME                           |  datetime.time                    |
  | TIME WITH TIME ZONE            |  datetime.time                    |
  | TIMESTAMP                      |  datetime.datetime                |
  | TIMESTAMP WITH TIME ZONE       |  datetime.datetime                |
  | INTERVAL                       |  teradata.datatypes.Interval      |
  | BLOB                           |  bytearray                        |
  | JSON                           |  dict or list, result of json.loads() |
  | PERIOD                         |  teradata.datatypes.Period        |
  ------------------------------- --------------------------------------

<a name="Unicode"></a>

#### **3.6 Unicode**
---
The Teradata Python Module supports the unicode character data transfer
via the UTF8 session character set.

<a name="IgnoringErrors"></a>

#### **3.7 Ignoring Errors**
---
Sometimes it is necessary to execute a SQL statement even though there
is a chance it may fail. For example, if your script depends on a table
that may or may not already exist, the simple thing to do is to try to
create the table and ignore the "table already exists" error. UdaExec
makes it easy to do this by allowing clients to specify error codes that
can safely be ignored. For example, the following execute statement will
not raise an error even if the checkpoints table already exists.

``` {.brush:python;}
session.execute("""CREATE TABLE ${dbname}.checkpoints (
    appName VARCHAR(1024) CHARACTER SET UNICODE,
    checkpointName VARCHAR(1024) CHARACTER SET UNICODE)
    UNIQUE PRIMARY INDEX(appName)""",
    ignoreErrors=[3803])
```

If you want to ignore all errors regardless of the error code, you can
include the "continueOnError=True" parameter to execute. This will cause
any errors to be caught and logged and not raised up to your
application.

<a name="PasswordProtection"></a>

#### **3.8 Password Protection**
---
Teradata SQL Driver for Python supports stored password protection. Please
refer to the [Stored Password Protection Section](https://github.com/Teradata/python-driver#StoredPasswordProtection) in the `README.md` for details.

<a name="ExternalScripts"></a>

#### **3.9 External SQL Scripts**
---
UdaExec can be used to execute SQL statements that are stored in files
external to your Python script. To execute the SQL statements in an
external file, simply pass the execute method the location of the file
to execute. E.g.

``` {.brush:python;}
session.execute(file="myqueries.sql")
```

A semi-colon is used as the default delimiter when specifying multiple
SQL statements. Any occurrence of a semi-colon outside of a SQL string
literal or comments is treated as a delimiter. When SQL scripts contain
SQL stored procedures that contain semi-colons internal to the
procedure, the delimiter should be change to something other than the
default. To use a different character sequence as the delimiter, the
delimiter parameter can be used. E.g.

``` {.brush:python;}
session.execute(file="myqueries.sql", delimiter=";;")
```

UdaExec also has limited support for executing BTEQ scripts. Any BTEQ
commands starting with a "." are simply ignored, while everything else
is treated as a SQL statement and executed. To execute a BTEQ script,
pass in a fileType=*\"bteq\"* parameter. E.g.

``` {.brush:python;}
session.execute(file="myqueries.bteq", fileType="bteq")
```

SQL statements in external files can reference external configuration
values using the *\${keyname}* syntax. Therefore, any use of "\$" in an
external SQL file must be escaped if it is not intended to reference an
external configuration value.

Any parameters passed to execute will be passed as parameters to the SQL
statements in the external file. Execute will still return a cursor when
executing a SQL script, the cursor will point to the results of the last
SQL statement in the file.

Comments can be included in SQL files. Multi-line comments start with
\"/\*\" and end with \"\*/\". Single line comments start with \"\--\".
Comments are submitted to the database along with the individual SQL
statements.

---
<a name="References"></a>

### **4.0 Reference**

This section defines the full set of method parameters supported by the
API.

<a name="UdaExec%20%Parameters"></a>

#### **4.1 UdaExec Parameters**
---
UdaExec accepts the following list of parameters during initialization.
The column labeled "E" flags if a parameter can be specified in an
external configuration file.

  ---
  |**Name**              |   **Description** |**E**| **Default Value**     |
  |----------------------|-------------------|-----|-----------------------|
  | **appName**          | The name of our application    |  Y  | None - Required field |
  | **version**          | The version of our application |  Y  | None - Required field |
  | **checkpointFile**   | The location of the checkpoint file. Can be None to disable file-based checkpoints. |Y |   *\${appName}.checkpoint*|
  | **runNumberFile**    | The path of the file containing the previous runNumber. |   Y  | *.runNumber* |
  | **runNumber**        | A string that represents this particular execution of the python script. Used in the log file name as well as included in the Session QueryBand.   | Y  | *YYYYmmddHHMMSS-X* |
  | **configureLogging** | Flags if UdaExec will configure logging. | Y  | True |
  | **logDir**           | The directory that contains log files.   | Y  | *\"logs\"* |
  | **logFile**          | The log file name.             | Y   | *\"\${appName}.\${runNumber}.log\"* |
  | **logLevel**         | The level that determines what log messages are logged (i.e. CRITICAL, ERROR, WARNING, INFO, DEBUG, TRACE) |  Y  | *\"INFO\"* |
  | **logConsole**       | Flags if logs should be written to stdout in addition to the log file.  |  Y  |  True |
  | **logRetention**     | The number of days to retain log files. Files in the log directory older than the specified number of days are deleted. |  Y  | 90 |
  | **systemConfigFile** | The system wide configuration file(s). Can be a single value or a list.  |   N  | *\"/etc/udaexec.ini\"* |
  | **userConfigFile**   | The user specific configuration file(s). Can be a single value or a list. |  N  |  *\"\~/udaexec.ini\"* or *\"%HOMEPATH%/udaexec.ini\"* |
  | **appConfigFile**    | The application specific configuration file (s). Can be a single value or a list. |  N  | *\"udaexec.ini\"* |
  | **configFiles**      | The full list of external configuration files. Overrides any values in systemConfigFile, userConfigFile, appConfigFile. |  N  | None |
  | **configSection**    | The name of the application config section in external configuration files.|  N  | *CONFIG* |
  | **parseCmdLineArgs** | Flags whether or not to include command line arguments as part of the external configuration variables. |   N   | True |
  | **gitPath**          | The path to the GIT executable to use to include GIT information in the session QueryBand. |   Y  | Defaults to system path |
  | **production**       | Flags if this app is a production application, applies this value to session QueryBand. |   Y  | False |
  | **dataTypeConverter**| The DataTypeConverter implementation to use to convert data types from their string representation to python objects. |  N | datatypes.DefaultDataTypeConverter() |
  -----------------------

<a name="ConnectParametrs"></a>

#### **4.2 Connect Parameters**
---
The following table lists the parameters that the UdaExec.connect()
method accepts. With the exception of the "externalDSN" parameter, all
the parameters below can be specified in the DEFAULT or named data
source sections of external configuration files. While the externalDSN
parameter cannot be specified directly in an external configuration
file, it can reference the name of an external configuration variable
using *\${keyname}* syntax. The "Type" column indicates if a parameter
is specific to a connectivity option, if it is blank it applies to all
types.

Any parameters passed to the connect method or specified in an external
configuration that are not listed below will be automatically be appened
to the connect string passed to the teradatasql driver. If the parameter
is not supported by the teradatasql driver, an error will be returned.

  ---
  | **Name**              |  **Description**                                      | **Default Value**|
  |-----------------------|-------------------------------------------------------|------------------|
  | **externalDSN**       | The name of the data source defined in external configuration files. | None - Optional |
  | **system**            | The Database name of the system to connect.           | None |
  | **username**          | The Database username to use to connect.              | None |
  | **password**          | The Database password to use to connect.              | None |
  | **database**          | The default database name to apply to the session     | None |
  | **autoCommit**        | Enables or disables auto commit mode. When auto commit mode is disabled, transactions must be committed manually.                                        | True |
  | **transactionMode**   | The transaction mode to use i.e. "Teradata" or "ANSI" | *Teradata* |
  | **queryBands**        | A map (dict) of query band key/value pairs to include the session's QueryBand. | None |
  | **dataTypeConverter** | The DataTypeConverter implementation to use to convert data types from their string representation to python objects. | datatypes.DefaultDataTypeConverter() |
  | **\*\*kwargs**        | A variable number of name/value pairs to append to the ConnectString passed to the Teradata SQL Driver for Python. For a full list of connection parameters offered, refer to the [Connection Parameters Section](https://github.com/Teradata/python-driver#ConnectionParameters) in the Teradata SQL Driver for Python `README.md`. | None |
  ---

<a name="ExecuteParameters"></a>

#### **4.3 Execute Parameters**
---
The following table lists the parameters that the execute method
accepts.

  ---
  | **Name**              |  **Description**                                 | **Default Value**      |
  |-----------------------|--------------------------------------------------|------------------------|
  | **query**             | The query to execute.                            | None, required if file is None |
  | **params**            | The list or tuple containing the parameters to pass in to replace question mark placeholders. | None |
  | **file**              | The path of an external script to execute.       | None |
  | **fileType**          | The type of file to execute if different than a standard delimited SQL script (i.e. bteq) | None |
  | **delimiter**         | The delimiter character to use for SQL scripts.  |  *;* |
  | **runAlways**         | When True, the query or script will be executed regardless if the previous checkpoint has been reached. | False |
  | **continueOnError**   | When True, all errors will be caught and logged but not raised up to the application.            | False |
  | **ignoreErrors**      | The list or sequence of error codes to ignore.   | None |
  | **logParamCharLimit** | The maximum number of characters to log per query parameter. When a parameter exceeds the limit it is truncated in the logs and an ellipsis (\"\...\") is appended. | 80 characters per parameter |
  | **logParamFrequency** | The amount of parameter sets to log when executemany is invoked. Setting this value to X means that every Xth parameter set will be logged in addition to the first and last parameter set. When this value is set to zero, no parameters are logged.               | 1 - all parameters sets are logged. |

---
<a name="RunningTests"></a>

### **5.0 Running Unit Tests**

To execute the unit tests, you can run the following command at the root of the project checkout.

    python -m unittest discover -s test

The unit tests use the connection information specified in test/udaexec.ini.  The unit tests depend on Teradata SQL Driver for Python being installed.

---
<a name="Migration"></a>

### **6.0 Migration Guide**

The Teradata Python Module is now a fully supported Teradata product. This module has been updated to use the Teradata SQL Driver for Python to connect to the Teradata Database. The Teradata ODBC Driver and the Query Service REST API for Teradata have been dropped.

This section highlights the modifications that may be useful to migrate your code to function with the updated module.

<a name="MGSetup"></a>

#### **6.1 Setup**
---
**Requirements and Limitations**

* Python 2.7 support dropped.
* Requires 64-bit Python 3.4 or later.
* Runs on Windows, macOS and Linux.
* 32-bit Python is not supported.
* Supported for use with Teradata Database 14.10 and later releases.

**Installation**

The Teradata Python Module now depends on the teradatasql package which is available from PyPI.

Continue to use pip install to download and install the Teradata Python module and its dependencies as described in the [Installation Section](#Installing).

If a different version of the teradatasql package is required, refer to the [Installation Section](https://github.com/Teradata/python-driver#Installation) of the teradatasql `README.md`.

<a name="MGDatabase"></a>

### **6.2 Database Interactions**
---
**Parameterized SQL**

To insert multiple rows, executemany can be used. The default behavior for this method has changed to send all the parameter sequences to the database in a single "batch". This is much faster than sending the parameter sequences individually. To insert the statements individually, pass in the parameter batch=False.

``` {.brush:python;}
session.executemany("""INSERT INTO employee (id, firstName, lastName, dob)
                       VALUES (?, ?, ?, ?)""",
                    ((1,"James", "Kirk", "2233-03-22"),
                     (2,"Jean-Luc", "Picard", "2305-07-13")),
                    batch=False)
```

**Stored Procedures**

Stored procedures can be invoked using the "callproc" method. Following are some changes made to the parameters:

* The teradata.InParam is new and can be used to specify the Teradata data type to bind the input parameter. E.g.

      teradata.InParam (None, dataType='PERIOD (DATE)')

* The teradata.InOutParam can also be used to specify the Teradata data type to bind the input parameter. E.g.

      teradata.InOutParam("2000-12-22,2008-10-27", "p2", dataType='PERIOD (DATE)')

The supported Teradata Database bind data types are limited to the ones supported by the teradatasql package. For these limitations, refer to the [Limitations Section](https://github.com/Teradata/python-driver#Limitations) in the teradatasql `README.md`.

**Data Types**

The returned data type values from the Teradata Database are no longer limited to their string representation. For a list of how the data types are returned, refer to the [Data Type Section](https://github.com/Teradata/python-driver#DataTypes) in the teradatasql `README.md`.

The teradata.datatypes.DefaultDataTypeConverter will still perform the same conversions as displayed in the [Data Type Conversion](#DataTypeConversion) table. If a custom DataTypeConverter is being used, adjustments may be needed.

**Unicode**

The UTF8 session character set is always used. The charset connection parameter is no longer supported.

**Password Protection**

Stored password protection is still supported through the Teradata SQL Driver for Python. For details, see the [Stored Password Protection](https://github.com/Teradata/python-driver#StoredPasswordProtection) section in the teradatasql `README.md`.

**Query Timeouts**

Query timeouts are not currently supported.


<a name="MGReference"></a>

### **6.3 Reference**
---
**UdaExec Parameters**

The odbcLibPath parameter has been removed.

**Connect Parameters**

The following table lists removed connect parameters.

---
  | **Removed Parameter**|  **Description**  | **Reason**|
  |---------------------------|-------------------|-------------|
  | **charset**      | The session character set. | The UTF8 session character set is always used so the charset connection parameter is not needed. |
  | **dbType**       | The type of system being connected to.| The only supported option is “Teradata” so this parameter is not needed.   |
  | **host**         | The host name of the server hosting the REST service. | REST is no longer supported. |
  | **method**       | The type of connection to make. | The only supported option is teradatasql so this parameter is not needed |
  | **port**         | The port number of REST Service. | REST is no longer supported. |
  | **protocol**     | The protocol to use for REST connections | REST is no longer supported. |
  | **sslContext**   | The ssl.SSLContext to use to establish SSL connections.| REST is no longer supported. |
  | **webContext**   | The web context of the REST service | REST is no longer supported. |
  | **verifyCerts**  | Flags if REST SSL certificate should be verified, ignored if sslContext is not None. | REST is no longer supported. |
  ---

The following table lists modified connect parameters.

  | **Modified Parameter**|  **Description**  | **Reason**|
  |---------------------------|-------------------|-------------|
  | **\*\*kwargs**   | A variable number of name/value pairs to append to the ConnectString passed to the Teradata SQL Driver for Python.  | For a full list of connection parameters offered, refer to the [Connection Parameters Section](https://github.com/Teradata/python-driver#ConnectionParameters) in the Teradata SQL Driver for Python `README.md`. |
  ---

**Execute Parameters**

The following table lists the execute parameters not currently supported.

 | **Name**   |  **Description**                         | **Reason**      |
  |---------------------|--------------------------------------------------|------------------------|
  | **queryTimeout**    | The number of seconds to wait for a response before aborting the query and returning.   | This feature is not currently supported in the teradatasql package but will be offered at a future date. |
  ---