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

os.chdir('load')

from load import main

env = json.load(open('../.env.test.json'))


connection = psycopg2.connect(
    database=env['REDSHIFT_DBNAME'],
    user=env['REDSHIFT_USER'],
    password=env['REDSHIFT_PASSWORD'],
    host=env['REDSHIFT_HOST'],
    port=env['REDSHIFT_PORT']
)


class AggregationTestCase(unittest.TestCase):
    # Test aggregation functions by week, place, risk, and scores calculation.

    def setUp(self):
        # Patch main connection with the test one.
        patch('load.main.connection', connection).start()

        # Set isolation level to run CREATE DATABASE statement outside of transactions.
        main.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        self.cursor = main.connection.cursor()

        # Recreate logentry table
        try:
            self.cursor.execute("DROP TABLE %s" % (main.tablename))
        except:
            pass
        main.create_table()

    def test_group_by_week(self):
        # GIVEN 3 entries of the same asn, risk and country, two of which within one week
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-20T00:00:01+00:00,71.3.0.0,2,12252,US
        2016-09-28T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.135.11,2,12252,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(ntp_scan_csv))

        # WHEN grouped entries get created
        main.create_count()

        # THEN count table should have 2 entries which get grouped, and one entry which stands alone
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (1L, 2, 'US', 12252L, '2016-09-19', 'monthly', 1),
                (2L, 2, 'US', 12252L, '2016-09-26', 'monthly', 2)  # grouped two entries
            ])
    
    def test_group_by_distinct_ip(self):
        # GIVEN 7 entries of the same asn, risk and country from hostA (71.3.0.0) and hostB (190.81.134)
        # First week: 2 hostA entries, 1 hostB entry
        # Second week: 2 hostA entries, 2 hostB entries
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-20T00:00:01+00:00,71.3.0.0,2,12252,US
        2016-09-20T00:00:01+00:00,71.3.0.0,2,12252,US
        2016-09-20T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-27T00:00:01+00:00,71.3.0.0,2,12252,US
        2016-09-28T00:00:01+00:00,71.3.0.0,2,12252,US
        2016-09-28T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(ntp_scan_csv))

        # WHEN grouped entries get created
        main.create_count()

        # THEN count table should have 2 entries which get grouped, and one entry which stands alone
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                # First week: 2 entries from hostA count as one
                (1L, 2, 'US', 12252L, '2016-09-19', 'monthly', 2),

                # Second week: duplicated entries for hostA and hostB will merge to single one for each host
                (2L, 2, 'US', 12252L, '2016-09-26', 'monthly', 2)
            ])

    def test_group_by_country(self):
        # GIVEN 3 entries of the same risk and week, two of which are from one country, but different asn
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,190.81.134.82,2,4444,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,3333,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(ntp_scan_csv))

        # WHEN grouped entries get created
        main.create_count()
        main.create_count_by_country()

        # THEN count_by_country table should have 2 entries which get grouped, and one entry which stands alone
        self.cursor.execute('select * from count_by_country;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (2, 'DE', '2016-09-26', 1L, 0.0, 0),
                (2, 'US', '2016-09-26', 2L, 0.0, 0)   # 2 entries grouped by country
            ])

    def test_group_by_risk(self):
        # GIVEN 3 entries, of the same week, two of which have same risk type, but different countries
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,190.81.134.82,7,4444,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,3333,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(ntp_scan_csv))

        # WHEN grouped entries get created
        main.create_count()
        main.create_count_by_country()
        main.create_count_by_risk()

        # THEN count_by_risk table should have 2 entries which get grouped, and one entry which stands alone
        self.cursor.execute('select * from count_by_risk;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (7, '2016-09-26', 1L, 1L),
                (2, '2016-09-26', 2L, 1L)   # 2 enreies grouped by risk
            ])

    def test_scores(self):
        # GIVEN 5 entries, of the same risk and week, two from US, and three from DE
        ntp_scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,190.81.134.82,2,4444,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.33,2,3333,DE
        2016-09-29T00:00:01+00:00,190.81.134.35,2,3333,DE
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




