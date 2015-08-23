Teradata Python DevOps Module
=============================

The Teradata Python Module is a freely available, open source, library for the Python programming language, whose aim is to make it easy to script powerful interactions with Teradata Database. It adopts the philosophy of <a href="https://developer.teradata.com/tools/articles/udasql-a-devops-focused-sql-execution-engine">udaSQL</a>, providing a DevOps focused SQL Execution Engine that allows developers to focus on their SQL and procedural logic without worrying about Operational requirements such as external configuration, query banding, and logging.

INSTALLATION
------------

    [sudo] pip install teradata

The module is hosted on PyPi: https://pypi.python.org/pypi/teradata

DOCUMENTATION
-------------

Documentation for the Teradata Python Module is available on the <a href="https://developer.teradata.com/tools/reference/teradata-python-module">Teradata Developer Exchange</a>.

UNIT TESTS
----------

To execute the unit tests, you can run the following command at the root of the project checkout.  

python -m unittest discover -s test

The unit tests use the connection information specified in test/udaexec.ini.  The unit tests depend on Teradata ODBC being installed and also on access to Teradata REST Services.
