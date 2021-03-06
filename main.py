from __future__ import print_function

from datapackage import push_datapackage
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine
from os.path import dirname, join
from string import Template
from textwrap import dedent

import logging
import datapackage
import tempfile
import shutil
import requests
import boto3
import json
import csv
import os

#utils
def rpath(*args):
    return join(dirname(__file__), *args)


def load_config(config_path):
    '''
    Load the regular config file
    '''
    template = open(config_path).read()
    try:
        config_str = Template(template).substitute(os.environ)
    except KeyError as e:
        raise ValueError(
            "A missing environment variable: {}".format(e))
    config = json.loads(config_str)
    logging.info("COnfig file {}".format(config))

    return config


def is_s3_path(str):
    return str.startswith("s3://")


def split_s3_path(s3_address):
    if not is_s3_path(s3_address):
        raise ValueError("{} is not an S3 address".format(s3_address))
    else:
        (s3_bucket, s3_path) = s3_address[5:].split('/', 1)
    return (s3_bucket, s3_path)


class Aggregator(object):
    def __init__(self, config):
        self.config = config
        self.tmpdir = tempfile.mkdtemp()
        self.connRedshift = create_engine(
            config.get('redshift_uri'),
            isolation_level='AUTOCOMMIT'
        )
        self.conns3 = boto3.resource(
            's3'
        )
        # only include country risk data for those with at least this many
        # results
        self.country_count_threshold = (
            self.config.get("country_count_threshold", 100))
        logging.info("Using country count threshold: {}".format(
            self.country_count_threshold))

    def run(self):
        table_name = 'count'
        self.upload_manifest()
        self.create_tables()
        self.load_ref_data()
        self.load_data()
        self.count_data()
        self.aggregate()
        self.update_amplified_count()
        self.unload(table_name)
        self.drop_tables(self.connRedshift.connect(), [
            'dim_risk', 'logentry', 'count'
        ])
        shutil.rmtree(self.tmpdir)


    def drop_tables(self, cursor, tables):
        for tablename in tables:
            cursor.execute(
                "DROP TABLE IF EXISTS %(table)s CASCADE",
                {"table": AsIs(tablename)}
            )


    def create_manifest(self, datapackage_string, source):
        datapackage = json.loads(datapackage_string)
        manifest = {"entries": []}
        keys = (p['path'] for p in datapackage.get('resources'))
        for key_list in keys:
            for key in key_list:
                logging.info("Adding {} to manifest".format(key))
                manifest['entries'].append({"url": join(source,key), "mandatory": True})
        return manifest


    def upload_manifest(self):
        tmp_manifest = join(self.tmpdir, 'clean.manifest')
        s3bucket, key = split_s3_path(self.config.get('dest_path'))
        dp_key = join(key, 'datapackage.json')
        logging.info("dest_path from config is {}".format(self.config.get('dest_path')))
        logging.info("s3bucket is {}".format(s3bucket))
        logging.info("key is {}".format(key))
        logging.info("dp_key is {}".format(dp_key))
        obj = self.conns3.Object(s3bucket, dp_key)
        dp = obj.get()['Body'].read().decode()
        manifest = self.create_manifest(dp, self.config.get('dest_path'))

        f = open(tmp_manifest, 'w')
        json.dump(manifest, f)
        f.close()

        key = join(key, 'clean.manifest')
        obj = self.conns3.Object(s3bucket, key)
        obj.put(Body=open(tmp_manifest,"rb"))
        logging.info('Manifest Updated')


    def create_tables(self):
        conn = self.connRedshift.connect()
        tablenames = [
            'dim_risk', 'logentry', 'count'
        ]
        self.drop_tables(conn, tablenames)
        create_logentry = dedent('''
        CREATE TABLE logentry(
        date TIMESTAMP, ip VARCHAR(32), risk INT,
        asn BIGINT, country VARCHAR(2)
        )
        ''')
        create_risk = dedent('''
        CREATE TABLE dim_risk(
        id INT, slug VARCHAR(32), title VARCHAR(32),
        is_archived BOOLEAN,
        taxonomy VARCHAR(16), measurement_units VARCHAR(32),
        amplification_factor FLOAT, description TEXT
        )
        ''')
        create_count = dedent('''
        CREATE TABLE count(
        date TIMESTAMP, risk INT, country VARCHAR(2),
        asn BIGINT, count INT, count_amplified FLOAT
        )
        ''')
        conn.execute(create_risk)
        conn.execute(create_logentry)
        conn.execute(create_count)
        conn.close()
        logging.info('Redshift tables created')


    def load_data(self):
        conn = self.connRedshift.connect()
        manifest = join(self.config.get('dest_path'), 'clean.manifest')
        copycmd = dedent('''
        COPY logentry FROM '%s'
        CREDENTIALS 'aws_iam_role=%s'
        IGNOREHEADER AS 1
        DELIMITER ',' gzip
        TIMEFORMAT AS 'auto'
        MANIFEST
        ''')
        logging.info('Loading data into db ... ')
        conn.execute(copycmd%(manifest, self.config.get('role_arn_redshift')))
        conn.close()
        logging.info('Data Loaded')


    def load_ref_data(self):
        conn = self.connRedshift.connect()
        url = ''
        for inv in self.config.get('inventory'):
            if inv.get('name') == 'risk':
                url = inv.get('url')
        dp = datapackage.DataPackage(url)
        risks = dp.resources[0].data
        query = dedent('''
        INSERT INTO dim_risk
        VALUES (%(id)s, %(slug)s, %(title)s, %(is_archived)s, %(taxonomy)s, %(measurement_units)s, %(amplification_factor)s, %(description)s)''')
        for risk in risks:
            # description is too long and not needed here
            risk['description']=''
            conn.execute(query,risk)
        conn.close()


    def count_data(self):
        cmd = 'SELECT count(*) FROM logentry'
        cursor = self.connRedshift.raw_connection().cursor()
        cursor.execute(cmd)
        logging.info(cursor.fetchone())


    def aggregate(self):
        conn = self.connRedshift.connect()
        logging.info('Aggregating ...')
        query = dedent('''
        INSERT INTO count
        (SELECT
            date, risk, country, asn, count(*) as count, 0 as count_amplified
        FROM(
            SELECT DISTINCT (ip), date_trunc('day', date) AS date, risk, asn, country FROM logentry
        ) AS foo
        GROUP BY date, asn, risk, country HAVING count(*) > %(threshold)s ORDER BY date DESC, country ASC, asn ASC, risk ASC)
        ''')
        conn.execute(query, {'threshold': self.country_count_threshold})
        conn.close()


    def update_amplified_count(self):
        conn = self.connRedshift.connect()
        logging.info('Calculating Amplificated Counts ...')
        query = dedent('''
        UPDATE count
        SET count_amplified = count*amplification_factor
        FROM dim_risk WHERE risk=id
        ''')
        conn.execute(query)
        conn.close()
        logging.info('Aggregation Finished!')


    def unload(self, table):
        conn = self.connRedshift.connect()
        conn.execute(dedent('''
        UNLOAD('SELECT * FROM count')
        TO '%s'
        iam_role '%s'
        DELIMITER AS ','
        ALLOWOVERWRITE
        PARALLEL OFF
        ''')%(join(self.config.get('agg_path'), table), self.config['role_arn_redshift'] ))
        conn.close()

        bucket, key = split_s3_path(self.config.get('agg_path'))
        self.add_extention(bucket, '%s000'%(join(key, table)))
        self.delete_key(bucket, '%s000'%(join(key, table)))
        logging.info('Data Unloaded To s3')


    def add_extention(self, bucket, key):
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        new_key = '%s.csv'%(key.split('0')[0])
        self.conns3.meta.client.copy(copy_source, bucket, new_key)


    def delete_key(self, bucket, key):
        self.conns3.Object(bucket, key).delete()


class LoadToRDS(object):
    def __init__(self, config):
        self.config = config
        self.tmpdir = tempfile.mkdtemp()
        self.ref_data_urls = [inventory.get('url') for inventory in config.get('inventory')]
        self.connRDS = create_engine(config.get('rds_uri'))
        self.conns3 = boto3.resource(
            's3'
        )
        self.tablenames = [
            'fact_count', 'agg_risk_country_week',
            'agg_risk_country_month', 'agg_risk_country_quarter', 'dim_asn',
            'agg_risk_country_year', 'dim_risk', 'dim_country', 'dim_date'
        ]


    def run(self):
        self.load_ref_data_rds()
        self.create_tables()
        self.download_and_load()
        self.populate_tables()
        self.update_dim_country_if_entry_does_not_present()
        self.update_dim_asn_if_entry_does_not_present()
        self.create_constraints()
        self.create_indexes()
        shutil.rmtree(self.tmpdir)


    def drop_tables(self, tables):
        for tablename in tables:
            self.connRDS.execute("DROP TABLE IF EXISTS %(table)s CASCADE",{"table": AsIs(tablename)})


    def download_and_load(self):
        logging.info('Downloading csv file ...')
        bucket, key = split_s3_path(self.config.get('agg_path'))
        s3paths = [(join(self.tmpdir,'count.csv'),join(key,'count.csv'))]
        bucket = self.conns3.Bucket(bucket)
        for path in s3paths:
            bucket.download_file(path[1], path[0])

        logging.info('Loading into RDS ...')
        # TODO: replace shelling out to psql
        copy_command = dedent('''
        psql {uri} -c "\COPY fact_count FROM {tmp}/count.csv WITH delimiter as ',' null '' csv;"
        ''')
        os.system(copy_command.format(tmp=self.tmpdir,uri=self.config.get('rds_uri')))


    def load_ref_data_rds(self):
        logging.info('Loading reference_data to RDS ...')
        conn = self.connRDS.connect()
        # creating dim_asn table here with other ref data
        conn.execute('DROP TABLE IF EXISTS data__asn___asn CASCADE')
        create_asn = 'CREATE TABLE data__asn___asn(number BIGINT, title TEXT, country TEXT)'
        conn.execute(create_asn)

        for url in self.ref_data_urls:
            # Loading of asn with push_datapackage takes more then 2 hours
            # So have to download locally and save (takes ~5 seconds)
            if 'asn' not in url:
                push_datapackage(descriptor=url, backend='sql', engine=conn)
            else:
                dp = datapackage.DataPackage(url)
                # local path will be returned if not found remote one (for tests)
                if dp.resources[0].remote_data_path:
                    r = requests.get(dp.resources[0].remote_data_path)
                    with open(join(self.tmpdir, 'asn.csv'),"wb") as fp:
                        fp.write(r.content)
                else:
                    shutil.copy(dp.resources[0].local_data_path,join(self.tmpdir, 'asn.csv'))
                # TODO: replace shelling out
                copy_command = dedent('''
                psql {uri} -c "\COPY data__asn___asn FROM {tmp}/asn.csv WITH delimiter as ',' csv header;"
                ''')
                os.system(copy_command.format(tmp=self.tmpdir,uri=self.config.get('rds_uri')))
        conn.close()


    def create_tables(self):
        conn=self.connRDS.connect()
        self.drop_tables(self.tablenames)
        create_risk ='ALTER TABLE data__risk___risk RENAME TO dim_risk'
        create_country = 'ALTER TABLE data__country___country RENAME TO dim_country'
        create_asn = 'ALTER TABLE data__asn___asn RENAME TO dim_asn'
        create_time = dedent('''
        CREATE TABLE dim_date(
            date DATE, month INT,
            year INT, quarter INT,
            week INT, week_start DATE,
            week_end DATE
            )''')
        create_count = dedent('''
        CREATE TABLE fact_count(
            date DATE, risk INT,
            country VARCHAR(2),
            asn BIGINT, count BIGINT,
            count_amplified FLOAT
            )''')
        create_cube = dedent('''
        CREATE TABLE agg_risk_country_{time}(
            date DATE, risk INT,
            country VARCHAR(2),
            count BIGINT,
            count_amplified FLOAT
            )''')

        conn.execute(create_risk)
        conn.execute(create_country)
        conn.execute(create_asn)
        conn.execute(create_time)
        conn.execute(create_count)
        self.create_or_update_cubes(conn, create_cube)
        conn.close()


    def create_or_update_cubes(self, conn, cmd):
        time_granularities = [
            'week', 'month', 'quarter', 'year'
        ]
        for time in time_granularities:
            conn.execute(cmd.format(time=time))


    def populate_tables(self):
        logging.info('Populating cubes')
        conn=self.connRDS.connect()
        update_date = dedent('''
        INSERT INTO dim_date
        (SELECT
            date,
            EXTRACT(MONTH FROM date) as month,
            EXTRACT(YEAR FROM date) as year,
            EXTRACT(QUARTER FROM date) as quarter,
            EXTRACT(WEEK FROM date) as week,
            date_trunc('week', date) as week_start,
            (date_trunc('week', date)+'6 days') as week_end
        FROM fact_count GROUP BY date)
        ''')
        populate_cube = dedent('''
        INSERT INTO agg_risk_country_{time}
            (SELECT date_trunc('{time}', date) AS date, risk, country,
            SUM(count) AS count, SUM(count_amplified) FROM fact_count
        GROUP BY CUBE(date_trunc('{time}', date), country, risk) ORDER BY date DESC, country)
        ''')
        update_cube_risk = dedent('''
        UPDATE agg_risk_country_{time}
        SET risk=100
        WHERE risk IS null;
        ''')
        update_cube_country = dedent('''
        UPDATE agg_risk_country_{time}
        SET country='T'
        WHERE country IS null;
        ''')
        conn.execute(update_date)
        self.create_or_update_cubes(conn, populate_cube)
        self.create_or_update_cubes(conn, update_cube_risk)
        self.create_or_update_cubes(conn, update_cube_country)
        conn.close()


    def update_dim_country_if_entry_does_not_present(self):
        '''
        Checks if there is new country in fact table that does not present in dim_country
        and updates if so. Inserts values like so: (country_id, uknown, unknown, unknown, unknown)
        '''

        conn = self.connRDS.connect()
        cmd = dedent('''
        SELECT DISTINCT country FROM fact_count fc WHERE NOT EXISTS (
            SELECT 1 FROM dim_country dc WHERE fc.country=dc.id
        ) AND country IS NOT NULL;
        ''')
        results = conn.execute(cmd).fetchall()
        if not len(results):
            conn.close()
            return
        logging.info (dedent('''
        Warning: New country ID(s) in fact table that do not present in country dimension.
        Updating dim_country as follows:
        id | name    | slug    | region  | continent
        ---+---------+---------+---------+----------'''))
        for country in results:

            cmd = dedent('''
            INSERT INTO dim_country VALUES('%s','unknown','unknown','unknown','unknown')
            ''' % country[0])
            conn.execute(cmd)
            logging.info ('%s | unknown | unknown | unknown | unknown' % country[0])
        logging.info ('\nUpdate reference data for counrtry with new entries here: https://github.com/cybergreen-net/refdata-country')
        conn.close()


    def update_dim_asn_if_entry_does_not_present(self):
        '''
        Checks if there is new ASN in fact table that does not present in dim_asn
        and updates if so. Inserts values like so: (AS_number, uknown, country_id)
        '''

        conn = self.connRDS.connect()
        cmd = dedent('''
        SELECT DISTINCT asn, country FROM fact_count fc WHERE NOT EXISTS (
            SELECT 1 FROM dim_asn da WHERE fc.asn=da.number
        ) AND asn IS NOT NULL;
        ''')
        results = conn.execute(cmd).fetchall()
        if not len(results):
            conn.close()
            return
        logging.info (dedent('''
        Warning: New AS numbers in fact table that do not present in asn dimension.
        Updating dim_asn as follows:

        number    | title   | country
        ----------+---------+--------'''))
        inserted = []
        duplicates = []
        for asn in results:
            if asn[0] in inserted:
                duplicates.append(asn)
                continue
            inserted.append(asn[0])
            cmd = dedent('''
            INSERT INTO dim_asn VALUES(%d,'unknown','%s')
            ''' % (asn[0], asn[1] or 'XY'))
            conn.execute(cmd)
            space = ' '*(10-len(str(asn[0])))
            logging.info ('%s%s| unknown | %s' % (asn[0], space, asn[1] or 'XY'))
        logging.info ('\nThis AS numbers were ignored as they already exists with different country ID:')
        for num in duplicates:
            logging.info ('%s | %s'%(num[0], num[1]))
        logging.info ('\nUpdate reference data for counrtry with new entries here: https://github.com/cybergreen-net/refdata-asn')
        conn.close()


    def create_constraints(self):
        conn = self.connRDS.connect()
        risk_constraints = 'ALTER TABLE dim_risk ADD PRIMARY KEY (id);'
        country_constraints = 'ALTER TABLE dim_country ADD PRIMARY KEY (id);'
        asn_constraints = '''
        ALTER TABLE dim_asn
        ADD PRIMARY KEY (number),
        ADD CONSTRAINT fk_country_asn FOREIGN KEY (country) REFERENCES dim_country(id)
        '''
        time_constraints = 'ALTER TABLE dim_date ADD PRIMARY KEY (date)'
        count_counstraints = dedent('''
        ALTER TABLE fact_count
        ADD CONSTRAINT fk_count_risk FOREIGN KEY (risk) REFERENCES dim_risk(id),
        ADD CONSTRAINT fk_count_country FOREIGN KEY (country) REFERENCES dim_country(id),
        ADD CONSTRAINT fk_count_asn FOREIGN KEY (asn) REFERENCES dim_asn(number),
        ADD CONSTRAINT fk_count_time FOREIGN KEY (date) REFERENCES dim_date(date);
        ''')
        cube_counstraints = dedent('''
        ALTER TABLE agg_risk_country_{time}
        ADD CONSTRAINT fk_cube_risk_{time} FOREIGN KEY (risk) REFERENCES dim_risk(id),
        ADD CONSTRAINT fk_cube_country_{time} FOREIGN KEY (country) REFERENCES dim_country(id)
        ''')
        conn.execute(risk_constraints)
        conn.execute(country_constraints)
        conn.execute(asn_constraints)
        conn.execute(time_constraints)
        conn.execute(count_counstraints)
        self.create_or_update_cubes(conn, cube_counstraints)
        conn.close()


    def create_indexes(self):
        conn = self.connRDS.connect()
        idx_dict = {
            # Index to speedup /api/v1/count
            'idx_date_country': 'CREATE INDEX idx_date_country ON fact_count(date DESC, country);',
            "idx_all": "CREATE INDEX idx_all ON fact_count(date, country, risk, asn);",
            "idx_all_desc": "CREATE INDEX idx_all_desc ON fact_count(date DESC, country, risk, asn);",
            "idx_risk": "CREATE INDEX idx_risk ON fact_count(risk);",
            "idx_asn": "CREATE INDEX idx_asn ON fact_count(asn);",
            "idx_country": "CREATE INDEX idx_country ON fact_count(country);",
            "idx_date": "CREATE INDEX idx_date ON fact_count(date);",
            "idx_all_cube": "CREATE INDEX idx_all_cube_{time} ON agg_risk_country_{time}(date, country, risk);",
            "idx_all_desc_cube": "CREATE INDEX idx_all_desc_cube_{time} ON agg_risk_country_{time}(date DESC, country, risk);",
            "idx_risk_cube": "CREATE INDEX idx_risk_cube_{time} ON agg_risk_country_{time}(risk);",
            "idx_country_cube": "CREATE INDEX idx_country_cube_{time} ON agg_risk_country_{time}(country);",
            "idx_date_cube": "CREATE INDEX idx_date_cube_{time} ON agg_risk_country_{time}(date);"
        }
        for idx in idx_dict:
            if 'cube' not in idx:
                conn.execute(idx_dict[idx])
            else:
                self.create_or_update_cubes(conn, idx_dict[idx])
        conn.close()


if __name__ == '__main__':
    config = load_config(rpath('config.json'))
    aggregator = Aggregator(config)
    aggregator.run()
    loader = LoadToRDS(config)
    loader.run()
