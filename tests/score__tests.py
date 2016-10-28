# -*- coding: UTF-8 -*-
# NOTE: Launch all tests with `nosetests` command from git repo root dir.

import unittest
import json
import os

from StringIO import StringIO
from tempfile import NamedTemporaryFile
from textwrap import dedent

from mock import patch
from nose.plugins.attrib import attr
import psycopg2


import main

env = json.load(open('.env.test.json'))


connection = psycopg2.connect(
    database=env['REDSHIFT_DBNAME'],
    user=env['REDSHIFT_USER'],
    password=env['REDSHIFT_PASSWORD'],
    host=env['REDSHIFT_HOST'],
    port=env['REDSHIFT_PORT']
)


class ScoresTestCase(unittest.TestCase):
    # Test scores calculation.

    def setUp(self):
        # Patch main connection with the test one.
        patch('main.connection', connection).start()

        # Set isolation level to run CREATE DATABASE statement outside of transactions.
        main.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        self.cursor = main.connection.cursor()

        # Recreate logentry table
        try:
            self.cursor.execute("DROP TABLE %s" % (main.tablename))
        except:
            pass
        main.create_table()

    def test_scores5(self):
        # GIVEN 5 entries from different hosts, of the same risk and week, 2 from US, and 3 from DE
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,71.3.0.1,2,4444,US
        2016-09-29T00:00:01+00:00,71.3.0.9,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.1,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.2,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.3,2,3333,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(ntp_scan_csv))

        # WHEN grouped entries get created
        main.create_count()
        main.create_count_by_country()
        main.create_count_by_risk()
        # AND score calcualted
        main.update_with_scores()

        # THEN count_by_country table should have proper scores
        self.cursor.execute('select * from count_by_country;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (2, 'DE', '2016-09-26', 3L, 100.0, 0),   # 100% score
                (2, 'US', '2016-09-26', 2L, 63.093, 0)   # 63% score
            ])

    def test_scores6(self):
        # GIVEN 6 entries from different hosts, of the same risk and week, 2 from US, and 4 from DE
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,71.3.0.1,2,4444,US
        2016-09-29T00:00:01+00:00,71.3.0.9,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.1,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.2,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.3,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.4,2,3333,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(ntp_scan_csv))

        # WHEN grouped entries get created
        main.create_count()
        main.create_count_by_country()
        main.create_count_by_risk()
        # AND score calcualted
        main.update_with_scores()

        # THEN count_by_country table should have proper scores
        self.cursor.execute('select * from count_by_country;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (2, 'DE', '2016-09-26', 4L, 100.0, 0),   # 100% score
                (2, 'US', '2016-09-26', 2L, 50.0, 0)   # 50% score
            ])

    def test_scores7(self):
        # GIVEN 7 entries from different hosts, of the same risk and week, 2 from US, and 5 from DE
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,71.3.0.1,2,4444,US
        2016-09-29T00:00:01+00:00,71.3.0.9,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.1,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.2,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.3,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.4,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.5,2,3333,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(ntp_scan_csv))

        # WHEN grouped entries get created
        main.create_count()
        main.create_count_by_country()
        main.create_count_by_risk()
        # AND score calcualted
        main.update_with_scores()

        # THEN count_by_country table should have proper scores
        self.cursor.execute('select * from count_by_country;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (2, 'DE', '2016-09-26', 5L, 100.0, 0),   # 100% score
                (2, 'US', '2016-09-26', 2L, 43.0677, 0)   # 43% score
            ])


