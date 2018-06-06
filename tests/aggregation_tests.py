# -*- coding: UTF-8 -*-
# NOTE: Launch all tests with `nosetests` command from git repo root dir.

import unittest
import tempfile
import datetime
import json
import os

from io import StringIO
from textwrap import dedent

from psycopg2.extensions import AsIs
from sqlalchemy import create_engine

from aggregator.main import Aggregator, LoadToRDS

config = json.loads(open('tests/config.test.json').read())

class RedshiftFunctionsTestCase(unittest.TestCase):
    # Test aggregation functions by week, ip, country and risk.

    def setUp(self):
        self.aggregator = Aggregator(config=config)

        # reference data with amplification factors
        self.scan_csv = dedent(u'''\
        id,slug,title,is_archived,amplification_factor,description
        1,,,false,,,41,
        2,,,false,,,556.9,
        4,,,false,,,6.3,
        5,,,false,,,30.8,
        ''')
        # set configurations
        self.cursor = self.aggregator.connRedshift.raw_connection().cursor()
        # create tables
        self.aggregator.create_tables()


    def test_all_tables_created(self):
        '''
        Checks if all necessary tables are created for redshift
        '''
        self.aggregator.create_tables()
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'logentry'})
        self.assertEqual(self.cursor.fetchone()[0], True)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'count'})
        self.assertEqual(self.cursor.fetchone()[0], True)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'dim_risk'})
        self.assertEqual(self.cursor.fetchone()[0], True)


    def test_drop_tables(self):
        '''
        Checks if tables are dropped
        '''
        self.aggregator.drop_tables(
            self.aggregator.connRedshift,
            ['logentry', 'count', 'dim_risk']
        )
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'logentry'}
        )
        self.assertEqual(self.cursor.fetchone()[0], False)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'count'}
        )
        self.assertEqual(self.cursor.fetchone()[0], False)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'dim_risk'}
        )
        self.assertEqual(self.cursor.fetchone()[0], False)


    def test_referenece_data_loaded(self):
        '''
        Checks if reference data is loaded from given url
        '''
        # load data
        self.aggregator.load_ref_data()
        self.cursor.execute('SELECT * FROM dim_risk')
        self.assertEqual(self.cursor.fetchone(), (0, u'test-risk', u'Test Risk', False, 'Testable','count', 0.13456, u''))


    def test_group_by_day(self):
        '''
        Checks if entries with same dates are grouped and summed up
        '''
        # GIVEN 3 entries of the same asn, risk and country,
        # two of which within same day, but different IP's
        scan_csv = dedent(u'''\
        ts,ip,risk_id,asn,cc
        2016-09-20T00:00:01+00:00,71.3.0.1,2,12252,US
        2016-09-20T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.135.11,2,12252,US
        ''')
        self.cursor.copy_expert('COPY logentry from STDIN csv header', StringIO(scan_csv))

        self.aggregator.aggregate()

        # count table should have 2 entries - 1 with cout of 2 and other with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252, 1, 0.0),
                (datetime.datetime(2016, 9, 20, 0, 0), 2, 'US', 12252, 2, 0.0) # grouped two entries
            ])


    def test_group_by_distinct_ip(self):
        '''
        Checks if entries with same IP within same risk and date are ignored
        '''
        # GIVEN 3 entries of the same asn, risk, country an IP,
        # two of which within same day
        scan_csv = dedent(u'''\
        ts,ip,risk_id,asn,cc
        2016-09-20T00:00:01+00:00,190.81.135.11,2,12252,US
        2016-09-20T00:00:01+00:00,190.81.135.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.135.11,2,12252,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        self.aggregator.aggregate()

        # count table should have 2 entries with 1 count each
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252, 1, 0.0),
                (datetime.datetime(2016, 9, 20, 0, 0), 2, 'US', 12252, 1, 0.0)
            ])


    def test_group_by_risk(self):
        '''
        Checks if entries with same risks are grouped and summed up
        '''
        # GIVEN 3 entries of the same asn, day and country from hostA (71.3.0.1) and hostB (190.81.134)
        # hostA: 1 entry with risk type matching with hostB
        # hostB: 2 entries of different risk type
        scan_csv = dedent(u'''\
        ts,ip,risk_id,asn,cc
        2016-09-29T00:00:01+00:00,71.3.0.1,0,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.12,0,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,1,12252,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        self.aggregator.aggregate()

        #count table should have 2 rows corresponding to different risks, with properly grouped entries
        self.cursor.execute('select * from count ORDER BY risk;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 0, u'US', 12252L, 2, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 1, u'US', 12252L, 1, 0.0)
            ])


    def test_group_by_country(self):
        '''
        Checks if entries with same county are grouped and summed up
        '''
        # GIVEN 3 entries of the same risk and day, two of which are from one country
        scan_csv = dedent(u'''\
        ts,ip,risk_id,asn,cc
        2016-09-29T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.34,2,3333,DE
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        self.aggregator.aggregate()

        # count table should have 2 entries - US with count of 2, and DE with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'DE', 3333, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252, 2, 0.0)
            ])


    def test_group_by_asn(self):
        '''
        Checks if entries with same asn are grouped and summed up
        '''
        # GIVEN 3 entries of the same risk, country and day, two of which have the same asn
        scan_csv = dedent(u'''\
        ts,ip,risk_id,asn,cc
        2016-09-29T00:00:01+00:00,190.81.134.82,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.11,2,12252,US
        2016-09-29T00:00:01+00:00,190.81.134.34,2,3333,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))

        self.aggregator.aggregate()

        # count table should have 2 entries - AS 12252 with count of 2, and AS 3333 with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 3333, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252, 2, 0.0)
            ])


    def test_end_to_end_aggregaton(self):
        '''
        Checks if entries same date, risk, country, asn are grouped separately
        and summed up correctly
        '''
        # GIVEN 17 entries of the 2 different risk, day, country and asn.
        # Also includes one entry with duplicated IP within one day, risk, country and asn
        scan_csv = dedent(u'''\
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

        self.aggregator.aggregate()

        # count table should have 2 entries - US with count of 2, and DE with count of 1
        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'DE', 1224, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'DE', 1224, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'DE', 1225, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'DE', 1225, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'US', 122, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 122, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 1, 'US', 12252, 1, 0.0),
                (datetime.datetime(2016, 9, 29, 0, 0), 2, 'US', 12252, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'DE', 1224, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'DE', 1224, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'DE', 1225, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'DE', 1225, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 122, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 122, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 12252, 1, 0.0),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 12252, 1, 0.0)
            ])


    def test_aplified_count(self):
        '''
        Checks if amplified counts are calculated correctly for each risk
        '''
        # recreate tables
        self.aggregator.create_tables()
        # GIVEN 4 entries of the same day, country, ASN an IP but different risks
        scan_csv = dedent(u'''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,71.3.0.1,1,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,2,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,4,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,5,4444,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))
        # import ref data
        self.cursor.copy_expert("COPY dim_risk from STDIN csv header", StringIO(self.scan_csv))

        self.aggregator.aggregate()
        self.aggregator.update_amplified_count()
        self.maxDiff = None

        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 4444, 1, 41),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 4444, 1, 556.9),
                (datetime.datetime(2016, 9, 28, 0, 0), 4, 'US', 4444, 1, 6.3),
                (datetime.datetime(2016, 9, 28, 0, 0), 5, 'US', 4444, 1, 30.8),
            ])


    def test_aplified_count_when_grouped(self):
        '''
        Checks if amplified counts are calculated correctly for each risk when grouped
        '''
        # recreate tables
        self.aggregator.create_tables()
        # GIVEN 4 entries of the same day, country, ASN, but different risks
        scan_csv = dedent(u'''\
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
        # import ref data
        self.cursor.copy_expert("COPY dim_risk from STDIN csv header", StringIO(self.scan_csv))

        self.aggregator.aggregate()
        self.aggregator.update_amplified_count()
        self.maxDiff = None

        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 4444, 3, 41*3),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 4444, 2, 556.9*2),
                (datetime.datetime(2016, 9, 28, 0, 0), 4, 'US', 4444, 4, 6.3*4),
                (datetime.datetime(2016, 9, 28, 0, 0), 5, 'US', 4444, 4, 30.8*4),
            ])


    def tearDown(self):
        self.aggregator.drop_tables(self.aggregator.connRedshift, ['logentry', 'count', 'dim_risk'])



class RDSFunctionsTestCase(unittest.TestCase):
    # Test aggregation functions by week, ip, country and risk.

    def setUp(self):
        self.loader = LoadToRDS(config=config)
        self.cursor = self.loader.connRDS.raw_connection().cursor()
        self.tablenames = [
            'fact_count', 'agg_risk_country_week',
            'agg_risk_country_month', 'agg_risk_country_quarter',
            'agg_risk_country_year', 'dim_risk', 'dim_country',
            'dim_asn', 'dim_date'
        ]
        # snipet for fact_count table
        self.counts = dedent(u'''
        2016-09-03,0,AA,111111,1,30.8
        2016-11-13,0,ZZ,999999,33,1353
        2016-05-22,0,AA,111111,10,410
        2014-10-21,0,ZZ,999999,4,25.2
        2014-10-03,0,AA,111111,2,1113.8
        ''')
        # drops temporay tables for ref data if exsists
        self.loader.drop_tables(self.tablenames)
        self.loader.drop_tables(['data__risk___risk',
                                        'data__country___country',
                                        'data__asn___asn'])
        self.loader.load_ref_data_rds()


    def test_reference_tables_created(self):
        '''
        Checks if temporary tables for ref data are created
        '''
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'data__risk___risk'})
        self.assertEqual(self.cursor.fetchone()[0], True)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'data__country___country'})
        self.assertEqual(self.cursor.fetchone()[0], True)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'data__asn___asn'})
        self.assertEqual(self.cursor.fetchone()[0], True)


    def test_reference_data_loaded_in_temptables(self):
        '''
        Checks if ref data is loaded in temp tables
        '''
        self.cursor.execute('SELECT * FROM data__risk___risk')
        self.assertEqual(self.cursor.fetchone(),
                         (0.0, u'test-risk', u'Test Risk', False, 'Testable','count', 0.13456, u'Nice\nSmall\nDescription'))
        self.cursor.execute('SELECT * FROM data__country___country')
        self.assertEqual(self.cursor.fetchone(),
                         (u'AA', u'Test country', u'test-country', u'test-regiton', u'test-continent'))
        self.cursor.execute('SELECT * FROM data__asn___asn')
        self.assertEqual(self.cursor.fetchone(), (111111.0, u'Test title', u'AA'))


    def test_rds_tables_created(self):
        '''
        Checks if all rds tables are created and temptables renamed
        '''
        self.loader.create_tables()
        message='Table %(table) is not created'
        for table in self.tablenames:
            self.cursor.execute(
                'select table_name from information_schema.tables where table_name=%(table)s',
                {'table': table})
            # If there is no table cursor.fetchone() will return None and fail
            self.assertEqual(self.cursor.fetchone()[0], table, msg=message.format(table=table))


    def test_populate_rds_tables(self):
        '''
        Checks if rds tables are populated
        '''
        message = 'Table {table} is empty'
        # Create tables and pouplate fact_cunt
        self.loader.create_tables()
        self.loader.connRDS.connect().execute(dedent("""
            INSERT INTO fact_count
            VALUES
                ('2016-09-03',0,'AA',111111,1,30.8),
                ('2016-11-13',0,'ZZ',999999,33,1353),
                ('2016-05-22',0,'AA',111111,10,410),
                ('2014-10-21',0,'ZZ',999999,4,25.2),
                ('2014-10-03',0,'AA',111111,2,1113.8)
            """))
        self.loader.connRDS.connect().close()
        self.loader.populate_tables()
        # After running populate tables
        # If there is no entry cursor.fetchone() will return None and fail
        for table in self.tablenames:
            self.cursor.execute('SELECT * FROM %(table)s LIMIT 1',{"table": AsIs(table)})
            self.assertNotEqual(self.cursor.fetchone(), None, msg=message.format(table=table))


    def test_create_constraints(self):
        '''
        Checks if all constraints are created
        '''
        c_names = [
            'dim_risk_pkey', 'dim_country_pkey', 'dim_asn_pkey', 'dim_date_pkey',
            'fk_country_asn', 'fk_count_risk', 'fk_count_country', 'fk_count_asn',
            'fk_count_time', 'fk_cube_risk_week','fk_cube_risk_month', 'fk_cube_risk_quarter',
            'fk_cube_risk_year', 'fk_cube_country_week', 'fk_cube_country_month',
            'fk_cube_country_quarter','fk_cube_country_year']
        message = 'Constraint {c_name} is not created'
        # Create tables and pouplate them
        self.loader.create_tables()
        self.loader.connRDS.connect().execute(dedent("""
            INSERT INTO fact_count
            VALUES
                ('2016-09-03',0,'AA',111111,1,30.8),
                ('2016-11-13',0,'ZZ',999999,33,1353),
                ('2016-05-22',0,'AA',111111,10,410),
                ('2014-10-21',0,'ZZ',999999,4,25.2),
                ('2014-10-03',0,'AA',111111,2,1113.8)
            """))
        self.loader.populate_tables()
        self.loader.create_constraints()
        for c_name in c_names:
            self.cursor.execute("select constraint_name from information_schema.table_constraints WHERE constraint_name = %(c_name)s",
                                {'c_name': c_name})
            # If there is no constraint cursor.fetchone() will return None and fail
            self.assertEqual(self.cursor.fetchone()[0], c_name, msg=message.format(c_name=c_name))

        # TODO: check indexes created

    def tearDown(self):
        self.loader.drop_tables(self.tablenames)


class MetadataTestCase(unittest.TestCase):
    def setUp(self):
        self.aggregator = Aggregator(config=config)


    def test_create_manifest(self):
        '''
        Checks if manifest is created according to AWS specifications
        '''
        datapackage = dedent(u'''{"resources":[
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
        manifest = self.aggregator.create_manifest(datapackage, 's3://test.bucket/test/key')
        self.assertEquals(manifest,expected_manifest)
