#!/usr/bin/env python

import struct
from ctypes import Structure, c_int, c_longlong, c_float, c_ulonglong, c_long
import os
from time import strftime, localtime
import re


DEBUG = True
DEFAULTS = None

pg_control_file = "global/pg_control"

__all__ = ["ControlFile", "ControlFileData"]


class ControlFile(object):
    major_version = "0.0"
    XLogRecPtr = "2i"
    Checkpoint = ""
    uint64 = "L"
    pg_time_t = "i"

    def __init__(self, datadir):
        self.datadir = datadir
        self._check_version()
        self.process_controlfile()

    def _check_version(self):
        datadir = self.datadir

        try:
            if not os.path.isdir(datadir):
                raise Exception("datadir {0} aren't acessible".format(datadir))
            elif not self._is_valid_datadir(datadir):
                raise Exception("Invalid $PGDATA directory")

            with open(datadir + "/PG_VERSION", "r") as version:
                self.major_version = version.read(3).strip()
                return True
        except IOError:
            """ @todo use logger """
            print "Version file not found"
            return False

    def _enum_state(self, code_state):
        msg_state = [
            "starting up",
            "shut down",
            "shutdown in recovering",
            "shutting down",
            "in crash recovery",
            "in archive recovery",
            "in production"
        ]
        return msg_state[code_state]

    def _format_time(self, time):
        strformattime = "%c"
        return strftime(strformattime, localtime(time))

    def __getattr__(self, name):
        row = self.control

        try:
            attr = row[name]

        except KeyError:
            attr = None

        return attr

    def process_controlfile(self):
        dat = self._get_data_file()

        if self._check_version():
            cfd = ControlFileData(self.major_version)

            for i in cfd._fields_:
                nmc = i[0]
                fmt = cfd.map_format_type(nmc)
                ofs = int(cfd.members_offset(nmc))
                siz = int(cfd.member_size(nmc))
                
#                if isinstance(i, XLogRecPtr):
#                    pass

                setattr(cfd, nmc, struct.unpack(fmt, dat[ofs:ofs+siz])[0])

            self.control = cfd.__dict__

    def _extract_member(self, offset, size):
        pass

    def _is_valid_datadir(self, datadir):
        pgdata = os.listdir(datadir)
        pgdirs = ['base', 'global', 'pg_clog']
        # check to see pgdirs in $PGDATA
        r = list(set(pgdirs).intersection(set(pgdata))).sort() == pgdirs.sort()
        return r

    def _get_data_file(self):
        datadir = self.datadir
        pg_control = datadir + pg_control_file
        data = ()
        try:
            with open(pg_control, "rb") as controlfile:
                data = controlfile.read()
        except IOError as e:
            print "Fail to get controlfile:", e

        return data
    """ @todo check crc algorithm perhaps using zlib.crc32 """
    def _check_crc(self, controlfile):
        raise NotImplementedError()

    def __str__(self):
        str_c = """
        ------------- Control File Data ----------------
        Database state: {0}
        Last pg_control update: {1}\n

        """.format(self._enum_state(self.state),
                self._format_time(self.time))
        return str_c.strip()


# Base class that represents a typical xlog record
# see subclasses for more details
class XLog(Structure):

    def members_size(self, member=None):
#        size = struct.calcsize(self.format_type())
        am = self.all_members()
        zf = map(lambda x: getattr(type(self), str(x[0])).size, am)
        size = sum(zf)
        return size

    def member_size(self, member):
        size = struct.calcsize(self.map_format_type(member))
        return size

    def all_members(self):
        return self._fields_

    def members_offset(self, member):
        if member:
            first_member = getattr(type(self), str(member))
        else:
            first_member = getattr(type(self), self._fields_[0][0])

        ofs = re.search("type=(.*), ofs=(.*)(,.*)", str(first_member)).group(2)
        return ofs

    def member_type(self, attr):
        member = getattr(type(self), str(attr))
        attr_type = re.search("type=(.*)(,.*)(,.*)", str(member)).group(1)

        return attr_type

    def format_type(self):
        return ''.join(map(self.map_format_type, self.all_members()))

    def map_format_type(self, attr):
        # mapping from ctype API to struct format string
        fmt_str = {
            "c_longlong": "@q",
            "c_ulonglong": "Q",
            "c_int": "i",
            "c_ulong": "L",
            "c_long": "l",
            "c_float": "f"
        }

        try:
            fmt = fmt_str[self.member_type(attr)]
        except KeyError:
            fmt = "i"
        return fmt


class XLogRecPtr(XLog):
    _fields_ = [
        ('xlogid', c_int),
        ('xrecoff', c_int)
    ]

    def __init__(self, offset):
        self.offset = offset


class Checkpoint(object):

    def __init__(self):
        self.redo = XLogRecPtr()
        self.ThisTimeLineId
        self.nextXidEpoch
        self.nextXid
        self.nextOid
        self.nextMulti


# Dummy class representing a pointer to xlog
# Another ways to make the same container would be using
# struct.Structure. Needs more discuss around it.
# see pgcontrol.h
class ControlFileData(XLog):
    #@todo Handling other members of ControlData
    _fields_ = [
        ('system_identifier', c_longlong),
        ('pg_control_version', c_int),
        ('catalog_version_no', c_int),
        ('state', c_int),
        ('time', c_int),
#        ('checkPoint', XLogRecPtr)
    ]

    def __init__(self, version):
        pass
