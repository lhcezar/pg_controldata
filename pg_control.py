#!/usr/bin/env python

import struct
import os
from time import strftime, localtime
from collections import namedtuple

# from format import Struct, Type

# enabled only at development time
DEBUG = True
pg_control_file = "global/pg_control"

__all__ = ["ControlFile"]

(DB_STARTUP, DB_SHUTDOWNED, DB_SHUTDOWNED_IN_RECOVERY,
    DB_SHUTDOWNING, DB_IN_CRASH_RECOVERY, DB_IN_ARCHIVE_RECOVERY,
    DB_IN_PRODUCTION
        ) = range(0, 7)

Checkpoint = namedtuple('Checkpoint', "xrecid \
        xrecoff lastcheck_xrecid lastcheck_xrecoff")


class ControlFile(object):
    control = {}
    major_version = "0.0"
    XLogRecPtr = "2i"
    Checkpoint = ""
    uint64 = "L"
    pg_time_t = "i"

    # see pg_control.h
    format_char = {
        "9.3": ("@QiiQQiiii",
        ('system_identifier', 'pg_control_version',
            'catalog_version_no', 'state', 'time'
                ,'checkpoint_xrecid'
                ,'checkpoint_xrecoff'
                ,'lastcheck_xrecid'
                ,'lastcheck_xrecoff'
                ))
        ,"9.1": "xxxx"
    }
    def __init__(self, datadir):
        self.datadir = datadir
        self.process_controlfile()

    def _check_version(self):
        datadir = self.datadir

        if not os.path.isdir(datadir):
            raise Exception("datadir {0} doesn't exist".format(datadir))
            return False
        try:
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
        records = self._get_data_file()

        if self._check_version():
            fchar = self.format_char[self.major_version]
            format = fchar[0]
            size = struct.calcsize(format)

            # @todo little-endians could mess up it
            values = struct.unpack(format, records[0:size])
            self.control = dict(zip(fchar[1], values))
            if DEBUG:
                print self.control

    def _extract_member(self, offset, size):
        """ @todo extract members from ControlFile struct by offset """
        pass

    def _get_data_file(self):
        datadir = self.datadir
        pg_control = datadir + pg_control_file
        data = ()
        try:
            with open(pg_control, "rb") as controlfile:
                data = controlfile.read()
        except:
            print "Fail to get controlfile"

        return data
    """ @todo check crc algorithm perhaps using zlib.crc32 """
    def _check_crc(self, controlfile):
        raise NotImplementedError()

    def __str__(self):
        str_c = """
        ------------- Control File Data ----------------
        Database state: {0}
        Last pg_control update: {1}\n

        """.format(self._enum_state(self.state)
                , self._format_time(self.time)
            )
        return str_c.strip()
