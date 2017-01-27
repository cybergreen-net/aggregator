# -*- coding: UTF-8 -*-
# NOTE: Launch all tests with `nosetests` command from git repo root dir.

import unittest
import tempfile
import datetime
import json
import os

from StringIO import StringIO
from tempfile import NamedTemporaryFile
from textwrap import dedent

from psycopg2.extensions import AsIs
from sqlalchemy import create_engine
from nose.plugins.attrib import attr
from mock import patch

# seting env variables for testing
os.environ["CYBERGREEN_BUILD_ENV"] = ''
os.environ["RDS_PASSWORD"] = ''
os.environ["REDSHIFT_PASSWORD"] = ''
os.environ["CYBERGREEN_SOURCE_ROOT"] = ''
os.environ["CYBERGREEN_DEST_ROOT"] = ''
os.environ["CYBERGREEN_BUILD_ENV"] = ''
os.environ["AWS_ACCESS_KEY_ID"] = ''
os.environ["AWS_SECRET_ACCESS_KEY"] = ''

import main

connRedshift = create_engine('postgres://cg_test_user:secret@localhost/cg_test_db')
connRDS = create_engine('postgres://cg_test_user:secret@localhost/cg_test_db')

class RedshiftFunctionsTestCase(unittest.TestCase):
    # Test aggregation functions by week, ip, country and risk.

    def setUp(self):
        # Patch main connection with the test one.
        patch('main.connRedshift', connRedshift).start()

        # reference data with amplification factors
        self.scan_csv = dedent('''\
        id,slug,title,amplification_factor,description
        1,,,41,
        2,,,556.9,
        4,,,6.3,
        5,,,30.8,
        ''')
        # set configurations
        self.config = {"inventory": [{
            "name": 'risk', "url": "tests/fixtures/risk-datapackage.json"
            }]}
        self.cursor = main.connRedshift.raw_connection().cursor()
        # create tables
        main.create_redshift_tables()


    def test_all_tables_created(self):
        '''
        Checks if all necessary tables are created for redshift
        '''
        main.create_redshift_tables()
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
        Checks if tebles ar dropped
        '''
        main.drop_tables(main.connRedshift, ['logentry', 'count', 'dim_risk'])
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'logentry'})
        self.assertEqual(self.cursor.fetchone()[0], False)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'count'})
        self.assertEqual(self.cursor.fetchone()[0], False)
        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%(table)s)',
            {'table': 'dim_risk'})
        self.assertEqual(self.cursor.fetchone()[0], False)


    def test_referenece_data_loaded(self):
        '''
        Checks if reference data is loaded from given url
        '''
        # load data
        main.load_ref_data(self.config)
        self.cursor.execute('SELECT * FROM dim_risk')
        self.assertEqual(self.cursor.fetchone(), (0, u'test-risk', u'Test Risk', 0.13456, u''))


    def test_group_by_day(self):
        '''
        Checks if entries with same dates are grouped and summed up
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
        Checks if entries with same IP within same risk and date are ignored
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
        Checks if entries with same risks are grouped and summed up
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
        Checks if entries with same county are gurped and summed up
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
        Checks if entries with same asn are gurped and summed up
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


    def test_end_to_end_aggregaton(self):
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


    def test_aplified_count(self):
        '''
        Checks if amplified counts are calculated correctly for each risk
        '''
        # recreate tables
        main.create_redshift_tables()
        # GIVEN 4 entries of the same day, country, ASN an IP but different risks
        scan_csv = dedent('''\
        ts,ip,risk_id,asn,cc
        2016-09-28T00:00:01+00:00,71.3.0.1,1,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,2,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,4,4444,US
        2016-09-28T00:00:01+00:00,71.3.0.1,5,4444,US
        ''')
        self.cursor.copy_expert("COPY logentry from STDIN csv header", StringIO(scan_csv))
        # import ref data
        self.cursor.copy_expert("COPY dim_risk from STDIN csv header", StringIO(self.scan_csv))

        main.aggregate()
        main.update_amplified_count()
        self.maxDiff = None

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
        '''
        Checks if amplified counts are calculated correctly for each risk when grouped
        '''
        # recreate tables
        main.create_redshift_tables()
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
        # import ref data
        self.cursor.copy_expert("COPY dim_risk from STDIN csv header", StringIO(self.scan_csv))

        main.aggregate()
        main.update_amplified_count()
        self.maxDiff = None

        self.cursor.execute('select * from count;')
        self.assertEqual(
            self.cursor.fetchall(),
            [
                (datetime.datetime(2016, 9, 28, 0, 0), 1, 'US', 4444L, 3, 41*3),
                (datetime.datetime(2016, 9, 28, 0, 0), 2, 'US', 4444L, 2, 556.9*2),
                (datetime.datetime(2016, 9, 28, 0, 0), 4, 'US', 4444L, 4, 6.3*4),
                (datetime.datetime(2016, 9, 28, 0, 0), 5, 'US', 4444L, 4, 30.8*4),
            ])


    def tearDown(self):
        main.drop_tables(main.connRedshift, ['logentry', 'count', 'dim_risk','tmp_count'])



class RDSFunctionsTestCase(unittest.TestCase):
    # Test aggregation functions by week, ip, country and risk.

    def setUp(self):
        # Patch main connection with the test one.
        patch('main.connRDS', connRDS).start()

        self.cursor = main.connRDS.raw_connection().cursor()
        self.tmpdir = tempfile.mkdtemp()

        self.tablenames = [
            'fact_count', 'agg_risk_country_week',
            'agg_risk_country_month', 'agg_risk_country_quarter',
            'agg_risk_country_year', 'dim_risk', 'dim_country', 
            'dim_asn', 'dim_time'
        ]
        # snipet for fact_count table
        self.counts = dedent('''
        2016-09-03,0,AA,111111,1,30.8
        2016-11-13,0,ZZ,999999,33,1353
        2016-05-22,0,AA,111111,10,410
        2014-10-21,0,ZZ,999999,4,25.2
        2014-10-03,0,AA,111111,2,1113.8
        ''')
        # set configurations
        self.urls = ['tests/fixtures/risk-datapackage.json',
                     'tests/fixtures/country-datapackage.json',
                     'tests/fixtures/asn-datapackage.json']
        # drops temporay tables for ref data if exsists
        main.drop_tables(main.connRDS, self.tablenames)
        main.drop_tables(main.connRDS, ['data__risk___risk',
                                        'data__country___country',
                                        'data__asn___asn'])
        self.uri = 'postgres://cg_test_user:secret@localhost/cg_test_db'
        main.load_ref_data_rds(self.urls, main.connRDS, self.tmpdir, self.uri)


    def test_reference_tables_created(self):
        '''
        Ckecks if temporary tables for ref data are created
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
        Ckecks if ref data is loaded in tem tables
        '''
        self.cursor.execute('SELECT * FROM data__risk___risk')
        self.assertEqual(self.cursor.fetchone(),
                         (0.0, u'test-risk', u'Test Risk', 0.13456, u'Nice\nSmall\nDescription'))
        self.cursor.execute('SELECT * FROM data__country___country')
        self.assertEqual(self.cursor.fetchone(),
                         (u'AA', u'Test country', u'test-country', u'test-regiton', u'test-continent'))
        self.cursor.execute('SELECT * FROM data__asn___asn')
        self.assertEqual(self.cursor.fetchone(), (111111.0, u'Test title', u'AA'))
    
    
    def test_rds_tables_created(self):
        '''
        Checks if all rds tables are created and temptables renamed
        '''
        main.create_rds_tables()
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
        main.create_rds_tables()
        self.cursor.copy_expert("COPY fact_count from STDIN csv header", StringIO(self.counts))
        # After running populate tables
        main.populate_tables()
        # If there is no entry cursor.fetchone() will return None and fail
        for table in self.tablenames:
            self.cursor.execute('SELECT * FROM %(table)s LIMIT 1',{"table": AsIs(table)})
            self.assertNotEqual(self.cursor.fetchone(), None, msg=message.format(table=table))
    
    
    def test_create_constraints(self):
        '''
        Checks if all constraints are created
        '''
        c_names = [
            'dim_risk_pkey', 'dim_country_pkey', 'dim_asn_pkey', 'dim_time_pkey',
            'fk_country_asn', 'fk_count_risk', 'fk_count_country', 'fk_count_asn',
            'fk_count_time', 'fk_cube_risk_week','fk_cube_risk_month', 'fk_cube_risk_quarter',
            'fk_cube_risk_year', 'fk_cube_country_week', 'fk_cube_country_month',
            'fk_cube_country_quarter','fk_cube_country_year']
        message = 'Constraint {c_name} is not created'
        # Create tables and pouplate them
        main.create_rds_tables()
        self.cursor.copy_expert("COPY fact_count from STDIN csv header", StringIO(self.counts))
        main.populate_tables()
        main.create_constraints()
        for c_name in c_names:
            self.cursor.execute("select constraint_name from information_schema.table_constraints WHERE constraint_name = %(c_name)s",
                                {'c_name': c_name})
            # If there is no constraint cursor.fetchone() will return None and fail
            self.assertEqual(self.cursor.fetchone()[0], c_name, msg=message.format(c_name=c_name))
        
        ## TODO: check indexes created
    
    def tearDown(self):
        main.drop_tables(main.connRDS, self.tablenames)


class MetadataTestCase(unittest.TestCase):

    def test_create_manifest(self):
        '''
        Checks if manifest is created according to AWS specifications
        '''
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
