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
from sqlalchemy import create_engine
import datetime

import main

connection = create_engine('postgres://cg_test_user:secret@localhost/cg_test_db')


class AggregationTestCase(unittest.TestCase):
    # Test aggregation functions by week, ip, country and risk.

    def setUp(self):
        # Patch main connection with the test one.
        patch('main.connRedshift', connection).start()

        # Set isolation level to run CREATE DATABASE statement outside of transactions.
        # main.connRedshift.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        self.cursor = main.connRedshift.raw_connection().cursor()

        # Recreate logentry table
        main.create_redshift_tables()
        main.load_ref_data()


    def test_group_by_day(self):
        '''
        Cheks if entries with same dates are grouped and summed up
        '''
        # GIVEN 3 entries of the same asn, risk and country,
        # two of which within same day, but different IP's
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-20T00:00:01+00:00,71.3.0.1,2,12252,US
        2016-09-20T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.135.11,2,12252,US
        ''')
        self.cursor.copy_expert('COPY logentry from STDIN csv header', StringIO(scan_csv))

        main.aggregate()

        # count table should have 2 entries - 1 with cout of 2 and other with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252L, 1, 0.0),  
                (datetime.datetime(2016, 9, 20, 0, 0), 2, 'US', 12252L, 2, 0.0) # grouped two entries
            ])


    def test_group_by_distinct_ip(self):
        '''
        Cheks if entries with same IP within same risk and date are ignored
        '''
        # GIVEN 3 entries of the same asn, risk, country an IP,
        # two of which within same day
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-20T00:00:01+00:00,190.81.135.11,2,12252,US
        2016-09-20T00:00:01+00:00,190.81.135.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.135.11,2,12252,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        main.aggregate()

        # count table should have 2 entries with 1 count each
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252L, 1, 0.0),  
                (datetime.datetime(2016, 9, 20, 0, 0), 2, 'US', 12252L, 1, 0.0)
            ])


    def test_group_by_risk(self):
        '''
        Cheks if entries with same risks are grouped and summed up
        '''
        # GIVEN 3 entries of the same asn, day and country from hostA (71.3.0.1) and hostB (190.81.134)
        # hostA: 1 entry with risk type matching with hostB
        # hostB: 2 entries of different risk type
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-29T00:00:01+00:00,71.3.0.1,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,1,12252,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        main.aggregate()

        #count table should have 2 rows corresponding to different risks, with properly grouped entries
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'US', 12252L, 1, 0.0),  
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252L, 2, 0.0)
            ])


    def test_group_by_country(self):
        '''
        Cheks if entries with same county are gurped and summed up
        '''
        # GIVEN 3 entries of the same risk and day, two of which are from one country
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-29T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.34,2,3333,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        main.aggregate()

        # count table should have 2 entries - US with count of 2, and DE with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'DE', 3333L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252L, 2, 0.0)  
            ])


    def test_group_by_asn(self):
        '''
        Cheks if entries with same asn are gurped and summed up
        '''
        # GIVEN 3 entries of the same risk, country and day, two of which have the same asn
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-29T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.34,2,3333,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        main.aggregate()

        # count table should have 2 entries - AS 12252 with count of 2, and AS 3333 with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 3333L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252L, 2, 0.0)
            ])


    def test_end_to_end(self):
        '''
        Ckeks if entries same date, risk, country, asn are grouped seperately
        and summed up correctly
        '''
        # GIVEN 17 entries of the 2 different risk, day, country and asn.
        # Also includes one entry with duplicated IP within one day, risk, country and asn
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-29T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.82,2,122,US
        2016-09-29T00:00:01+00:00,190.81.134.83,2,1225,DE
        2016-09-29T00:00:01+00:00,190.81.134.83,2,1224,DE
        2016-09-29T00:00:01+00:00,190.81.134.82,1,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.82,1,122,US
        2016-09-29T00:00:01+00:00,190.81.134.83,1,1225,DE
        2016-09-29T00:00:01+00:00,190.81.134.83,1,1224,DE
        2016-09-28T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-28T00:00:01+00:00,190.81.134.82,2,122,US
        2016-09-28T00:00:01+00:00,190.81.134.83,2,1225,DE
        2016-09-28T00:00:01+00:00,190.81.134.83,2,1224,DE
        2016-09-28T00:00:01+00:00,190.81.134.82,1,12252,US
        2016-09-28T00:00:01+00:00,190.81.134.82,1,122,US
        2016-09-28T00:00:01+00:00,190.81.134.83,1,1225,DE
        2016-09-28T00:00:01+00:00,190.81.134.83,1,1224,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        main.aggregate()

        # count table should have 2 entries - US with count of 2, and DE with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'DE', 1224L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'DE', 1224L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'DE', 1225L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'DE', 1225L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'US', 122L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 122L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'US', 12252L, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'DE', 1224L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'DE', 1224L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'DE', 1225L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'DE', 1225L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 122L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 122L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 12252L, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 12252L, 1, 0.0)
            ])


class CalculationTestCase(unittest.TestCase):


    def setUp(self):
        # Patch main connection with the test one.
        patch('main.connRedshift', connection).start()
        # Set isolation level to run CREATE DATABASE statement outside of transactions.
        # main.connRedshift.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = main.connRedshift.raw_connection().cursor()
        main.create_redshift_tables()
        # import amlificatin counts
        scan_csv = dedent('''\
        id,slug,title,amplification_factor,description
        1,,,41,
        2,,,556.9,
        4,,,6.3,
        5,,,30.8,
        ''')
        self.cursor.copy_expert("COPY dim_risk from STDIN csv header", StringIO(scan_csv))

    def test_aplified_count(self):
        # GIVEN 4 entries of the same day, country, ASN an IP but different risks
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,71.3.0.1,1,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,2,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,4,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,5,4444,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        # WHEN grouped entries get created
        main.aggregate()
        main.update_amplified_count()
        self.maxDiff = None

        # THEN count_by_country table should have proper scores
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 4444L, 1, 41),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 4444L, 1, 556.9),
                (datetime.datetime(2016, 9, 28, 0, 0), 4, 'US', 4444L, 1, 6.3),
                (datetime.datetime(2016, 9, 28, 0, 0), 5, 'US', 4444L, 1, 30.8),
            ])


    def test_aplified_count_when_grouped(self):
        # GIVEN 4 entries of the same day, country, ASN, but different risks
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,71.3.0.1,1,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.2,1,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.3,1,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,2,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.2,2,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,4,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.2,4,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.3,4,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.4,4,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,5,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,5,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.2,5,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.3,5,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.4,5,4444,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        # WHEN grouped entries get created
        main.aggregate()
        main.update_amplified_count()
        self.maxDiff = None

        # THEN count_by_country table should have proper scores
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 4444L, 3, 41*3),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 4444L, 2, 556.9*2),
                (datetime.datetime(2016, 9, 28, 0, 0), 4, 'US', 4444L, 4, 6.3*4),
                (datetime.datetime(2016, 9, 28, 0, 0), 5, 'US', 4444L, 4, 30.8*4),
            ])


class MetadataTestCase(unittest.TestCase):

    def test_create_manifest(self):
        datapackage = dedent('''{"resources":[
        {"path": ["ntp-scan/ntp-scan.20000101.csv.gz"],
        "schema": {"fields": []}, "name": "openntp", "compression": "gz", "format": "csv"},
        {"path": ["ssdp-data/ssdp-data.20000101.csv.gz"],
        "schema": {"fields": []}, "name": "openssdp", "compression": "gz", "format": "csv"},
        {"path": [],
        "schema": {"fields": []}, "name": "spam", "compression": "gz", "format": "csv"},
        {"path": ["snmp-data/snmp-data.20000101.csv.gz"],
        "schema": {"fields": []}, "name": "opensnmp", "compression": "gz", "format": "csv"},
        {"path": ["dns-scan/dns-scan.20000101.csv.gz"],
        "schema": {"fields": []}, "name": "opendns", "compression": "gz", "format": "csv"}],
        "name": "cybergreen_enriched_data",
        "title": "CyberGreen Enriched Data"}''')
        expected_manifest = {'entries': [
            {'url': u's3://test.bucket/test/key/ntp-scan/ntp-scan.20000101.csv.gz',
             'mandatory': True},
            {'url': u's3://test.bucket/test/key/ssdp-data/ssdp-data.20000101.csv.gz',
             'mandatory': True},
            {'url': u's3://test.bucket/test/key/snmp-data/snmp-data.20000101.csv.gz',
             'mandatory': True},
            {'url': u's3://test.bucket/test/key/dns-scan/dns-scan.20000101.csv.gz',
             'mandatory': True}
            ]}
        manifest = main.create_manifest(datapackage, 's3://test.bucket/test/key')
        self.assertEquals(manifest,expected_manifest)
