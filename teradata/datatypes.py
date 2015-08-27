"""Data types and converters."""

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

import re
import logging
import json
from . import util
from .api import *  # @UnusedWildImport # noqa

logger = logging.getLogger(__name__)

SECS_IN_MILLISECS = MILLISECS_IN_MICROSECS = 1000
dateRegExStr = r"(\d{4})-(\d{2})-(\d{2})"
timeRegExStr = r"(\d{2}):(\d{2}):(\d{2})(\.(\d{1,6}))?(([-+])(\d{2}):(\d{2}))?"
dateRegEx = re.compile("^{}$".format(dateRegExStr))
timeRegEx = re.compile("^{}$".format(timeRegExStr))
timestampRegEx = re.compile("^{} {}$".format(dateRegExStr, timeRegExStr))
scalarIntervalRegEx = re.compile("^(-?)(\d+)$")
yearToMonthIntervalRegEx = re.compile("^(-?)(\d+)-(\d+)$")
dayToHourIntervalRegEx = re.compile("^(-?)(\d+) (\d+)$")
dayToMinuteIntervalRegEx = re.compile("^(-?)(\d+) (\d+):(\d+)$")
dayToSecondIntervalRegEx = re.compile("^(-?)(\d+) (\d+):(\d+):(\d+\.?\d*)$")
hourToMinuteIntervalRegEx = re.compile("^(-?)(\d+):(\d+)$")
hourToSecondIntervalRegEx = re.compile("^(-?)(\d+):(\d+):(\d+\.?\d*)$")
minuteToSecondIntervalRegEx = re.compile("^(-?)(\d+):(\d+\.?\d*)$")
secondIntervalRegEx = re.compile("^(-?)(\d+\.?\d*)$")
periodRegEx = re.compile("\('(.*)',\s*'(.*)'\)")

NUMBER_TYPES = ("BYTEINT", "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE PRECISION",
                "INTEGER", "NUMBER", "SMALLINT", "FLOAT", "INT", "NUMERIC",
                "REAL")

FLOAT_TYPES = ("FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL")

BINARY_TYPES = (
    "BLOB", "BYTE", "GRAPHIC", "LONG VARGRAPHIC", "VARBYTE", "VARGRAPHIC")


def _getMs(m, num):
    ms = m.group(num)
    if ms:
        ms = int(ms.ljust(6, "0"))
    else:
        ms = 0
    return ms


def _getInt(m, num):
    return int(m.group(num))


def _getFloat(m, num):
    return float(m.group(num))


def convertDate(value):
    m = dateRegEx.match(value)
    if m:
        return datetime.date(_getInt(m, 1), _getInt(m, 2), _getInt(m, 3))
    else:
        raise InterfaceError(
            "INVALID_DATE", "Date format invalid: {}".format(value))


def convertTime(value):
    m = timeRegEx.match(value)
    if m:
        tz = None
        if m.group(7):
            tz = TimeZone(m.group(7), _getInt(m, 8), _getInt(m, 9))
        return datetime.time(_getInt(m, 1), _getInt(m, 2), _getInt(m, 3),
                             _getMs(m, 5), tz)
    else:
        raise InterfaceError(
            "INVALID_TIME", "Time format invalid: {}".format(value))


def convertTimestamp(value):
    m = timestampRegEx.match(value)
    if m:
        tz = None
        if m.group(10):
            tz = TimeZone(m.group(10), _getInt(m, 11), _getInt(m, 12))
        return datetime.datetime(_getInt(m, 1), _getInt(m, 2), _getInt(m, 3),
                                 _getInt(m, 4), _getInt(m, 5), _getInt(m, 6),
                                 _getMs(m, 8), tz)
    else:
        raise InterfaceError(
            "INVALID_TIMESTAMP", "Timestamp format invalid: {}".format(value))


def _convertScalarInterval(dataType, value, *args):
    return _convertInterval(dataType, value, scalarIntervalRegEx, *args)


def _convertInterval(dataType, value, regEx, *args):
    m = regEx.match(value)
    if m:
        kwargs = {}
        index = 2
        for field in args:
            if field != "seconds":
                kwargs[field] = _getInt(m, index)
            else:
                kwargs[field] = _getFloat(m, index)
            index += 1
        return Interval(negative=True if m.group(1) else False, **kwargs)
    else:
        raise InterfaceError("INVALID_INTERVAL",
                             "{} format invalid: {}".format(dataType, value))


def convertInterval(dataType, value):
    value = value.strip()
    if dataType == "INTERVAL YEAR":
        return _convertScalarInterval(dataType, value, "years")
    elif dataType == "INTERVAL YEAR TO MONTH":
        return _convertInterval(dataType, value, yearToMonthIntervalRegEx,
                                "years", "months")
    elif dataType == "INTERVAL MONTH":
        return _convertScalarInterval(dataType, value, "months")
    elif dataType == "INTERVAL DAY":
        return _convertScalarInterval(dataType, value, "days")
    elif dataType == "INTERVAL DAY TO HOUR":
        return _convertInterval(dataType, value, dayToHourIntervalRegEx,
                                "days", "hours")
    elif dataType == "INTERVAL DAY TO MINUTE":
        return _convertInterval(dataType, value, dayToMinuteIntervalRegEx,
                                "days", "hours", "minutes")
    elif dataType == "INTERVAL DAY TO SECOND":
        return _convertInterval(dataType, value, dayToSecondIntervalRegEx,
                                "days", "hours", "minutes", "seconds")
    elif dataType == "INTERVAL HOUR":
        return _convertScalarInterval(dataType, value, "hours")
    elif dataType == "INTERVAL HOUR TO MINUTE":
        return _convertInterval(dataType, value, hourToMinuteIntervalRegEx,
                                "hours", "minutes")
    elif dataType == "INTERVAL HOUR TO SECOND":
        return _convertInterval(dataType, value, hourToSecondIntervalRegEx,
                                "hours", "minutes", "seconds")
    elif dataType == "INTERVAL MINUTE":
        return _convertScalarInterval(dataType, value, "minutes")
    elif dataType == "INTERVAL MINUTE TO SECOND":
        return _convertInterval(dataType, value, minuteToSecondIntervalRegEx,
                                "minutes", "seconds")
    elif dataType == "INTERVAL SECOND":
        return _convertInterval(dataType, value, secondIntervalRegEx,
                                "seconds")
    return value


def convertPeriod(dataType, value):
    m = periodRegEx.match(value)
    if m:
        if "TIMESTAMP" in dataType:
            start = convertTimestamp(m.group(1))
            end = convertTimestamp(m.group(2))
        elif "TIME" in dataType:
            start = convertTime(m.group(1))
            end = convertTime(m.group(2))
        elif "DATE" in dataType:
            start = convertDate(m.group(1))
            end = convertDate(m.group(2))
        else:
            raise InterfaceError("INVALID_PERIOD",
                                 "Unknown PERIOD data type: {}".format(
                                     dataType, value))
    else:
        raise InterfaceError(
            "INVALID_PERIOD", "{} format invalid: {}".format(dataType, value))
    return Period(start, end)


def zeroIfNone(value):
    if value is None:
        value = 0
    return value


class DataTypeConverter:

    """Handles conversion of result set data types into python objects."""

    def convertValue(self, dbType, dataType, typeCode, value):
        """Converts the value returned by the database into the desired
         python object."""
        raise NotImplementedError(
            "convertValue must be implemented by sub-class")

    def convertType(self, dbType, dataType):
        """Converts the data type to a python type code."""
        raise NotImplementedError(
            "convertType must be implemented by sub-class")


class DefaultDataTypeConverter (DataTypeConverter):

    """Handles conversion of result set data types into python objects."""

    def __init__(self, useFloat=False):
        self.useFloat = useFloat

    def convertValue(self, dbType, dataType, typeCode, value):
        """Converts the value returned by the database into the desired
         python object."""
        logger.trace(
            "Converting \"%s\" to (%s, %s).", value, dataType, typeCode)
        if value is not None:
            if typeCode == NUMBER:
                try:
                    return NUMBER(value)
                except:
                    # Handle infinity and NaN for older ODBC drivers.
                    if value == "1.#INF":
                        return NUMBER('Infinity')
                    elif value == "-1.#INF":
                        return NUMBER('-Infinity')
                    else:
                        return NUMBER('NaN')
            elif typeCode == float:
                return value if not util.isString else float(value)
            elif typeCode == Timestamp:
                if util.isString(value):
                    return convertTimestamp(value)
                else:
                    return datetime.datetime.fromtimestamp(
                        value // SECS_IN_MILLISECS).replace(
                        microsecond=value % SECS_IN_MILLISECS *
                        MILLISECS_IN_MICROSECS)
            elif typeCode == Time:
                if util.isString(value):
                    return convertTime(value)
                else:
                    return datetime.datetime.fromtimestamp(
                        value // SECS_IN_MILLISECS).replace(
                        microsecond=value % SECS_IN_MILLISECS *
                        MILLISECS_IN_MICROSECS).time()
            elif typeCode == Date:
                if util.isString(value):
                    return convertDate(value)
                else:
                    return datetime.datetime.fromtimestamp(
                        value // SECS_IN_MILLISECS).replace(
                        microsecond=value % SECS_IN_MILLISECS *
                        MILLISECS_IN_MICROSECS).date()
            elif typeCode == BINARY:
                if util.isString(value):
                    return bytearray.fromhex(value)
            elif dataType.startswith("INTERVAL"):
                return convertInterval(dataType, value)
            elif dataType.startswith("JSON") and util.isString(value):
                return json.loads(value, parse_int=decimal.Decimal,
                                  parse_float=decimal.Decimal)
            elif dataType.startswith("PERIOD"):
                return convertPeriod(dataType, value)
        return value

    def convertType(self, dbType, dataType):
        """Converts the data type to a python type code."""
        typeCode = STRING
        if dataType in NUMBER_TYPES:
            typeCode = NUMBER
            if self.useFloat and dataType in FLOAT_TYPES:
                typeCode = float
        elif dataType in BINARY_TYPES:
            typeCode = BINARY
        elif dataType.startswith("DATE"):
            typeCode = Date
        elif dataType.startswith("TIMESTAMP"):
            typeCode = Timestamp
        elif dataType.startswith("TIME"):
            typeCode = Time
        return typeCode


class TimeZone (datetime.tzinfo):

    """Represents a Fixed Time Zone offset from UTC."""

    def __init__(self, sign, hours, minutes):
        self.offset = datetime.timedelta(hours=hours, minutes=minutes)
        if sign == "-":
            self.offset = -self.offset

    def utcoffset(self, dt):
        return self.offset

    def tzname(self, dt):
        return "TeradataTimestamp"

    def dst(self, dt):
        return 0


def _appendInterval(arr, value, padding=2, separator=" "):
    if value is not None:
        if arr and separator:
            arr.append(separator)
        s = (("%0" + str(padding + 7) + ".6f") % value).rstrip("0").rstrip(".")
        arr.append(s)


class Interval:

    """Represents a SQL date/time interval."""

    def __init__(self, negative=False, years=None, months=None, days=None,
                 hours=None, minutes=None, seconds=None):
        self.negative = negative
        self.years = years
        self.months = months
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.type = None
        if years is not None:
            if months is not None:
                self.type = "YEAR TO MONTH"
            else:
                self.type = "YEAR"
            if days or hours or minutes or seconds:
                raise InterfaceError(
                    "INVALID INTERVAL",
                    "A year/month interval cannot be "
                    "shared with a day/hour/minute/second interval.")
        elif months is not None:
            self.type = "MONTH"
            if days or hours or minutes or seconds:
                raise InterfaceError(
                    "INVALID INTERVAL",
                    "A year/month interval cannot be shared "
                    "with a day/hour/minute/second interval.")
        elif days is not None:
            if seconds is not None:
                self.type = "DAY TO SECOND"
                self.hours = zeroIfNone(hours)
                self.minutes = zeroIfNone(minutes)
            elif minutes is not None:
                self.type = "DAY TO MINUTE"
                self.hours = zeroIfNone(hours)
            elif hours is not None:
                self.type = "DAY TO HOUR"
            else:
                self.type = "DAY"
        elif hours is not None:
            if seconds is not None:
                self.type = "HOUR TO SECOND"
                self.minutes = zeroIfNone(minutes)
            elif minutes is not None:
                self.type = "HOUR TO MINUTE"
            elif hours is not None:
                self.type = "DAY TO HOUR"
            else:
                self.type = "HOUR"
        elif minutes is not None:
            if seconds is not None:
                self.type = "MINUTE TO SECOND"
            else:
                self.type = "MINUTE"
        elif seconds is not None:
            self.type = "SECOND"
        else:
            raise InterfaceError(
                "INVALID INTERVAL",
                "One of years, months, days, hours, minutes, "
                "seconds must not be None.")

    def timedelta(self):
        if self.years or self.months:
            raise InterfaceError(
                "UNSUPPORTED_INTERVAL",
                "timedelta() is not supported for Year "
                "and Month interval types. %s" % repr(self))
        delta = datetime.timedelta(days=zeroIfNone(self.days),
                                   hours=zeroIfNone(self.hours),
                                   minutes=zeroIfNone(self.minutes),
                                   seconds=zeroIfNone(self.seconds))
        if self.negative:
            delta = -delta
        return delta

    def __str__(self):
        s = []
        _appendInterval(s, self.years, padding=1)
        _appendInterval(s, self.months, separator="-")
        _appendInterval(s, self.days, padding=1)
        _appendInterval(s, self.hours)
        _appendInterval(s, self.minutes, separator=":")
        _appendInterval(s, self.seconds, separator=":")
        if self.negative:
            s.insert(0, "-")
        return "".join(s)

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        try:
            return self.__dict__ == other.__dict__
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other


class Period:

    """ Represents a PERIOD data type. """

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __str__(self):
        return "('" + str(self.start) + "', '" + str(self.end) + "')"

    def __eq__(self, other):
        try:
            return self.__dict__ == other.__dict__
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other
