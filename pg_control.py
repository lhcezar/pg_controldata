#!/usr/bin/env python

import struct
import os
from time import strftime, localtime
from collections import namedtuple

# enabled only at development time
DEBUG = True
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
        control_file_data = ControlFileData(self.major_version)
        self.control = control_file_data.__dict__
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
            # argh!
            attr = eval("row."+name)
        except KeyError:
            attr = None

        return attr

    def process_controlfile(self):
        records = self._get_data_file()

        if self._check_version():
            control_file_data = ControlFileData(self.major_version)

            def get_str_fmt(cf):
                for key, member in cf.iteritems():
                    if type(member) is not dict:
                        yield member, key

            pg_control_members = control_file_data.__dict__
            # @todo needs refactoring..
            members = tuple(i[1] for i in get_str_fmt(pg_control_members))
            struct_keys = (i[0] for i in get_str_fmt(pg_control_members))

            fmt = ''.join(struct_keys)

            size = struct.calcsize(fmt)
            Container = namedtuple("Container", members)

            self.control = Container._make(struct.unpack(fmt, records[0:size]))

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


# dummy class
# see pgcontrol.h
class ControlFileData(object):
    def __init__(self, version):
        self.system_identifier = "Q"
        self.pg_control_version = "i"
        self.catalog_version_no = "i"
        self.state = "i"
        self.time = "i"
        self.checkPoint = {
            "xlogid": "i",
            "xrecoff": "i"
        }
        self.prevCheckPoint = {
            "xlogid": "i",
            "xrecoff": "i"
        }
