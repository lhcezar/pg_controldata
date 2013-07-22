import unittest
import os
from pg_control import ControlFile


class PgControlTestCase(unittest.TestCase):

    def setUp(self):
        self.config_pg_data()

    def config_pg_data(self):
        if os.environ.get("PGDATA"):
            self.pgdata = os.environ.get("PGDATA")
        else:
            # remove to ini file
            self.pgdata = "/Users/lhcezar/postgres-9.2/"

    def test_is_valid_datadir(self):
        pass

    def test_cluster_version(self):
        c = ControlFile(self.pgdata)
#        assert '9.3' == '9.3', "Falha na versao"
        assert self.assertEqual(9.3, str(c.major_version), 'Version failures')
