import unittest
import os
from pg_control import ControlFile, ControlFileData


class ControlFileTestCase(unittest.TestCase):

    def setUp(self):
        self.config_pg_data()
        self.cf = ControlFile(self.pgdata)
        self.data = ControlFileData(self.cf.major_version)

    def config_pg_data(self):
        """$PGDATA variable should be set or give me a valid instance dir """
        if os.environ.get("PGDATA"):
            self.pgdata = os.environ.get("PGDATA")
        else:
            # remove to ini file
            self.pgdata = "/Users/lhcezar/postgres-9.2/"

    def test_size_of_system_identifier(self):
        pass

    def test_is_valid_datadir(self):
        self.assertTrue(self.cf._is_valid_datadir(self.pgdata))

    def test_is_not_valid_datadir(self):
        # of course nobody would put PGDATA into filesystem root
        self.assertTrue(self.cf._is_valid_datadir(os.sep))

    def test_valid_state(self):
        valid_states = range(0, 7)
        print self.data.state
        self.assertIn(self.data.state, valid_states)

    def test_startup_state(self):
        self.assertEqual(self.cf._enum_state(0), "starting up")

    def test_cluster_version(self):
        version = self.cf.major_version
        # support to next versions: 9.4, 10.0, &ca
        version_regexp = '[8-9]{1,2}\.[0-9]'
        self.assertRegexpMatches(version, version_regexp, 'Version failures')
