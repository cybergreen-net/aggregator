from __future__ import print_function

from datapackage import push_datapackage
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine
from os.path import dirname, join
from string import Template
from textwrap import dedent

import datapackage
import tempfile
import shutil
import urllib
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

    return config


def is_s3_path(str):
    return str.startswith("s3://")


def split_s3_path(s3_address):
    if not is_s3_path(s3_address):
        raise ValueError("{} is not an S3 address".format(s3_address))
    else:
        (s3_bucket, s3_path) = s3_address[5:].split('/', 1)
    return (s3_bucket, s3_path)


config = load_config(rpath('config.json'))

CYBERGREEN_SOURCE_ROOT = config['source_path']
CYBERGREEN_DEST_ROOT = config['dest_path']
REDSHIFT_ROLE_ARN = config['role_arn']
REDSHIFT_URI = config['redshift_uri']
RDS_URI = config['rds_uri']
REF_DATA_URLS = [inventory['url'] for inventory in config['inventory']]
# AWS credentials
AWS_ACCESS_KEY = config['access_key']
AWS_ACCESS_SECRET_KEY = config['secret_key']
# set connections
connRedshift = create_engine(REDSHIFT_URI).connect()
connRDS = create_engine(RDS_URI).connect()
conns3 = boto3.resource('s3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_ACCESS_SECRET_KEY
)


def create_manifest(datapackage,source):
    datapackage = json.loads(datapackage)
    manifest = {"entries": []}
    keys = (p['path'] for p in datapackage['resources'])
    for key_list in keys:
        for key in key_list:
            manifest['entries'].append({"url": join(source,key), "mandatory": True})
    return manifest


def upload_manifest(tmp_dir):
    tmp_manifest = join(tmp_dir,'clean.manifest')
    s3bucket, key = split_s3_path(CYBERGREEN_SOURCE_ROOT)
    dp_key = join(key, 'datapackage.json')
    obj = conns3.Object(s3bucket, dp_key)
    dp = obj.get()['Body'].read()
    manifest = create_manifest(dp,CYBERGREEN_SOURCE_ROOT)

    f = open(tmp_manifest, 'w')
    json.dump(manifest, f)
    f.close()

    key = join(key, 'clean.manifest')
    obj = conns3.Object(s3bucket, key)
    obj.put(Body=open(tmp_manifest))
    print('Manifest Updated')


### LOAD, AGGREGATION, UNLOAD
def create_redshift_tables():
    tablenames = [
        'dim_risk', 'logentry', 'count'
    ]
    drop_tables(connRedshift, tablenames)
    create_logentry = dedent('''
    CREATE TABLE logentry(
    date TIMESTAMP, ip VARCHAR(32), risk INT,
    asn BIGINT, country VARCHAR(2)
    )
    ''')
    create_risk = dedent('''
    CREATE TABLE dim_risk(
    id INT, slug VARCHAR(32), title VARCHAR(32),
    amplification_factor FLOAT, description TEXT
    )
    ''')
    create_count = dedent('''
    CREATE TABLE count(
    date TIMESTAMP, risk INT, country VARCHAR(2),
    asn BIGINT, count INT, count_amplified FLOAT
    )
    ''')
    connRedshift.execute(create_risk)
    connRedshift.execute(create_logentry)
    connRedshift.execute(create_count)
    print('Redshift tables created')


def load_data():
    manifest = join(CYBERGREEN_SOURCE_ROOT, 'clean.manifest')
    copycmd = dedent('''
    COPY logentry FROM '%s'
    CREDENTIALS 'aws_iam_role=%s'
    IGNOREHEADER AS 1
    DELIMITER ',' gzip
    TIMEFORMAT AS 'auto'
    MANIFEST
    ''')
    print('Loading data into db ... ')
    connRedshift.execute(copycmd%(manifest, REDSHIFT_ROLE_ARN))
    print('Data Loaded')


def load_ref_data():
    url = ''
    for inv in config['inventory']:
        if inv['name'] == 'risk':
            url = inv['url']
    dp = datapackage.DataPackage(url)
    risks = dp.resources[0].data
    query = dedent('''
    INSERT INTO dim_risk
    VALUES (%(id)s, %(slug)s, %(title)s, %(amplification_factor)s, %(description)s)''')
    for risk in risks:
        # description is too long and not needed here
        risk['description']=''
        connRedshift.execute(query,risk)
    

def aggregate():
    print('Aggregating ...')
    query = dedent('''
    INSERT INTO count
    (SELECT
        date, risk, country, asn, count(*) as count, 0 as count_amplified
    FROM(
    SELECT DISTINCT (ip), date_trunc('day', date) AS date, risk, asn, country FROM logentry) AS foo
    GROUP BY date, asn, risk, country ORDER BY date DESC, country ASC, asn ASC, risk ASC)
    ''')
    connRedshift.execute(query)


def update_amplified_count():
    print('Calculating Amplificated Counts ...')
    query = dedent('''
    UPDATE count
    SET count_amplified = count*amplification_factor
    FROM dim_risk WHERE risk=id
    ''')
    connRedshift.execute(query)
    print('Aggregation Finished!')


def unload(table):
    aws_auth_args = 'aws_access_key_id=%s;aws_secret_access_key=%s'%\
        (AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY)
    connRedshift.execute(dedent('''
    UNLOAD('SELECT * FROM count')
    TO '%s'
    CREDENTIALS '%s'
    DELIMITER AS ','
    ALLOWOVERWRITE
    PARALLEL OFF
    ''')%(join(CYBERGREEN_DEST_ROOT, table), aws_auth_args))

    bucket, key = split_s3_path(CYBERGREEN_DEST_ROOT)
    add_extention(bucket, '%s000'%(join(key, table)))
    delete_key(bucket, '%s000'%(join(key, table)))
    print('Data Unloaded To s3')


def add_extention(bucket, key):
    copy_source = {
        'Bucket': bucket,
        'Key': key
    }
    new_key = '%s.csv'%(key.split('0')[0])
    conns3.meta.client.copy(copy_source, bucket, new_key)


def delete_key(bucket, key):
    conns3.Object(bucket, key).delete()


### LOAD FROM S3 TO RDS
def download(tmp):
    print('Downloading csv file ...')
    bucket, key = split_s3_path(CYBERGREEN_DEST_ROOT)
    s3paths = [
        (join(tmp,'count.csv'),join(key,'count.csv'))
    ]
    bucket = conns3.Bucket(bucket)
    for path in s3paths: 
        bucket.download_file(path[1], path[0])


def load_ref_data_rds(urls, engine):
    print('Loading reference_data')
    for url in urls:
        push_datapackage(descriptor=url,backend='sql',engine=engine)

def create_rds_tables():
    tablenames = [
        'fact_count', 'agg_risk_country_week',
        'agg_risk_country_month', 'agg_risk_country_quarter',
        'agg_risk_country_year', 'dim_risk', 'dim_country', 
        'dim_asn', 'dim_time'
    ]
    drop_tables(connRDS, tablenames)

    create_risk ='ALTER TABLE data__risk___risk RENAME TO dim_risk'
    create_country = 'ALTER TABLE data__country___country RENAME TO dim_country'
    create_asn = 'ALTER TABLE data__asn___asn RENAME TO dim_asn'
    create_time = dedent('''
    CREATE TABLE dim_time(
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

    connRDS.execute(create_risk)
    connRDS.execute(create_country)
    connRDS.execute(create_asn)
    connRDS.execute(create_time)
    connRDS.execute(create_count)
    create_or_update_cubes(create_cube)


def populate_tables(tmpdir):
    update_time = dedent('''
    INSERT INTO dim_time
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
    GROUP BY CUBE(date, country, risk) ORDER BY date DESC, country)
    ''')
    copy_command = dedent('''
    psql {uri} -c "\COPY fact_count FROM {tmp}/count.csv WITH delimiter as ',' null '' csv;"
    ''')
    download(tmpdir)
    os.system(copy_command.format(tmp=tmpdir,uri=config['rds_uri']))
    connRDS.execute(update_time)
    create_or_update_cubes(populate_cube)


def create_constraints():
    risk_constraints = 'ALTER TABLE dim_risk ADD PRIMARY KEY (id);'
    country_constraints = 'ALTER TABLE dim_country ADD PRIMARY KEY (id);'
    asn_constraints = 'ALTER TABLE dim_asn ADD PRIMARY KEY (number)'
    time_constraints = 'ALTER TABLE dim_time ADD PRIMARY KEY (date)'
    count_counstraints = dedent('''
    ALTER TABLE fact_count
    ADD CONSTRAINT fk_count_risk FOREIGN KEY (risk) REFERENCES dim_risk(id),
    ADD CONSTRAINT fk_count_country FOREIGN KEY (country) REFERENCES dim_country(id),
    ADD CONSTRAINT fk_count_asn FOREIGN KEY (asn) REFERENCES dim_asn(number),
    ADD CONSTRAINT fk_count_time FOREIGN KEY (date) REFERENCES dim_time(date);
    ''')
    cube_counstraints = dedent('''
    ALTER TABLE agg_risk_country_{time}
    ADD CONSTRAINT fk_cube_risk FOREIGN KEY (risk) REFERENCES dim_risk(id),
    ADD CONSTRAINT fk_cube_country FOREIGN KEY (country) REFERENCES dim_country(id),
    ADD CONSTRAINT fk_cube_time FOREIGN KEY (date) REFERENCES dim_time(date);
    ''')
    connRDS.execute(risk_constraints)
    connRDS.execute(country_constraints)
    connRDS.execute(asn_constraints)
    connRDS.execute(time_constraints)
    connRDS.execute(count_counstraints)
    create_or_update_cubes(cube_counstraints)


def create_indexes():
    connRDS = connRDS.curesor()
    idx_dict = {
        # Index to speedup /api/v1/count
        "idx_all": "CREATE INDEX idx_all ON fact_count(date, country, risk, asn);",
        "idx_all_desc": "CREATE INDEX idx_all_desc ON fact_count(date DESC, country, risk, asn);",
        "idx_risk": "CREATE INDEX idx_risk ON fact_count(risk);",
        "idx_asn": "CREATE INDEX idx_asn ON fact_count(asn);",
        "idx_country": "CREATE INDEX idx_country ON fact_count(country);",
        "idx_date": "CREATE INDEX idx_date ON fact_count(date);",
        "idx_all_cube": "CREATE INDEX idx_all_cube ON agg_risk_country_date(date, country, risk);",
        "idx_all_desc_cube": "CREATE INDEX idx_all_desc_cube ON agg_risk_country_date(date DESC, country, risk);",
        "idx_risk_cube": "CREATE INDEX idx_risk_cube ON agg_risk_country_date(risk);",
        "idx_country_cube": "CREATE INDEX idx_country_cube ON agg_risk_country_date(country);",
        "idx_date_cube": "CREATE INDEX idx_date_cube ON agg_risk_country_date(date);"
    }
    for idx in idx_dict:
        cursor.execute(idx_dict[idx])


def drop_tables(cursor, tables):
    for tablename in tables:
        cursor.execute("DROP TABLE IF EXISTS %(table)s CASCADE",{"table": AsIs(tablename)})


def create_or_update_cubes(cmd):
    time_granularities = [
        'week', 'month', 'quarter', 'year'
    ]
    for time in time_granularities:
        connRDS.execute(cmd.format(time=time))


def run_redshift(tmpdir):
    table_name = 'count'
    upload_manifest(tmpdir)
    create_redshift_tables()
    load_data()
    load_ref_data()
    aggregate()
    update_amplified_count()
    unload(table_name)
    connRedshift.close()


def run_rds(tmpdir):
    load_ref_data_rds(REF_DATA_URLS, connRDS)
    create_rds_tables()
    populate_tables(tmpdir)
    connRDS.close()
    # create_constraints()
    # create_indexes()


if __name__ == '__main__':
    tmpdir = tempfile.mkdtemp()
    run_redshift(tmpdir)
    run_rds(tmpdir)
    shutil.rmtree(tmpdir)
