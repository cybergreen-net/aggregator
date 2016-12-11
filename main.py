from __future__ import print_function

from datapackage import push_datapackage
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine
from os.path import dirname, join
from textwrap import dedent

import datapackage
import tempfile
import shutil
import urllib
import boto3
import json
import csv
import os

def rpath(*args):
    return join(dirname(__file__), *args)

env = json.load(open(rpath('.env.json')))
# AWS credentials
AWS_ACCESS_KEY = env['AWS_ACCESS_KEY']
AWS_ACCESS_SECRET_KEY = env['AWS_ACCESS_SECRET_KEY']

STAGE= env['STAGE']
SOURCE_S3_BUCKET = env['SOURCE_S3_BUCKET']
SOURCE_S3_KEY = join(STAGE,env['SOURCE_S3_KEY'])
DEST_S3_BUCKET = env['DEST_S3_BUCKET']
DEST_S3_KEY= join(STAGE,env['DEST_S3_KEY'])
REFERENCE_KEY = join(STAGE, env['REFERENCE_KEY'])
REDSHIFT_ROLE_ARN = env['REDSHIFT_ROLE_ARN']


# Redshift connection
connRedshift = create_engine(
    'postgres://cybergreen:{0}@cg-analytics.cqxchced59ta.eu-west-1.redshift.amazonaws.com:5439/{1}'.
        format(env['REDSHIFT_PASSWORD'], STAGE)
    ).connect()
# S3 connection
conns3 = boto3.resource('s3',
	aws_access_key_id=AWS_ACCESS_KEY,
	aws_secret_access_key=AWS_ACCESS_SECRET_KEY)
# RDS connection
connRDS = create_engine(
    'postgres://cybergreen:{0}@cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com:5432/{1}'.
        format(env['RDS_PASSWORD'], STAGE)
    ).connect()
urls = [
    'https://raw.githubusercontent.com/cybergreen-net/refdata-risk/master/datapackage.json',
    'https://raw.githubusercontent.com/cybergreen-net/refdata-country/master/datapackage.json',
    'https://raw.githubusercontent.com/cybergreen-net/refdata-asn/master/datapackage.json'
    ]


def create_manifest(datapackage,s3_bucket,s3_key):
    datapackage = json.loads(datapackage)
    manifest = {"entries": []}
    keys = (p['path'] for p in datapackage['resources'])
    for key_list in keys:
        for key in key_list:
            manifest['entries'].append({"url": join("s3://",s3_bucket,s3_key,key), "mandatory": True})
    return manifest


def upload_manifest(tmp_dir):
    tmp_manifest = join(tmp_dir,'clean.manifest')
    s3bucket = SOURCE_S3_BUCKET
    key = join(SOURCE_S3_KEY, 'datapackage.json')
    obj = conns3.Object(s3bucket, key)
    datapackage = obj.get()['Body'].read()
    manifest = create_manifest(datapackage,SOURCE_S3_BUCKET,SOURCE_S3_KEY)
    
    f = open(tmp_manifest, 'w')
    json.dump(manifest, f)
    f.close()
    
    key = join(SOURCE_S3_KEY, 'clean.manifest')
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
    role_arn = REDSHIFT_ROLE_ARN
    manifest = join('s3://', SOURCE_S3_BUCKET, SOURCE_S3_KEY,'clean.manifest')
    copycmd = dedent('''
    COPY logentry FROM '%s'
    CREDENTIALS 'aws_iam_role=%s'
    IGNOREHEADER AS 1
    DELIMITER ',' gzip
    TIMEFORMAT AS 'auto'
    MANIFEST
    ''')%(manifest, role_arn)
    print('Loading data into db ... ')
    connRedshift.execute(copycmd)
    print('Data Loaded')


def load_ref_data():
    url = 'https://raw.githubusercontent.com/cybergreen-net/refdata-risk/master/datapackage.json'
    dp = datapackage.DataPackage(url)
    risks = dp.resources[0].data
    query = dedent("""
    INSERT INTO dim_risk
    VALUES (%(id)s, %(slug)s, %(title)s, %(amplification_factor)s, %(description)s)""")
    for risk in risks:
        # description is too long and not needed here
        risk["description"]=""
        connRedshift.execute(query,risk)
    

def count_data():
	cmd = 'SELECT count(*) FROM logentry'
	connRedshift.execute(cmd)
	print(cursor.fetchone()[0])


def aggregate():
    print('Aggregating ...')
    query = dedent("""
    INSERT INTO count
    (SELECT
        date, risk, country, asn, count(*) as count, 0 as count_amplified
    FROM(
    SELECT DISTINCT (ip), date_trunc('day', date) AS date, risk, asn, country FROM logentry) AS foo
    GROUP BY date, asn, risk, country ORDER BY date DESC, country ASC, asn ASC, risk ASC)
    """)
    connRedshift.execute(query)


def update_amplified_count():
    print('Calculating Amplificated Counts ...')
    query = dedent("""
    UPDATE count
    SET count_amplified = count*amplification_factor
    FROM dim_risk WHERE risk=id
    """)
    connRedshift.execute(query)
    print('Aggregation Finished!')


def unload(table, s3path):
    role_arn = REDSHIFT_ROLE_ARN
    s3bucket = join("s3://", DEST_S3_BUCKET)
    aws_auth_args = 'aws_access_key_id=%s;aws_secret_access_key=%s'%(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY)
    s3path = join(s3bucket, s3path)
    connRedshift.execute(dedent("""
    UNLOAD('SELECT * FROM count')
    TO '%s'
    CREDENTIALS '%s'
    DELIMITER AS ','
    ALLOWOVERWRITE
    PARALLEL OFF
    """)%(s3path, aws_auth_args))
    add_extention('%s000'%(join(DEST_S3_KEY,table)))
    delete_key('%s000'%(join(DEST_S3_KEY,table)))
    print('Data Unloaded To s3')


def add_extention(key):
    copy_source = {
        'Bucket': DEST_S3_BUCKET,
        'Key': key
    }
    new_key = '%s.csv'%(key.split('0')[0])
    conns3.meta.client.copy(copy_source, DEST_S3_BUCKET, new_key)


def delete_key(key):
    conns3.Object(DEST_S3_BUCKET, key).delete()


### LOAD FROM S3 TO RDS
copy_commands = """
export PGPASSWORD={password}
psql -h \
{host} \
-U cybergreen -d {db} -p 5432 \
-c "\COPY fact_count FROM {tmp}/count.csv WITH delimiter as ',' null '' csv;"
"""


def download(tmp):
    s3bucket = DEST_S3_BUCKET
    s3paths = [
        (join(tmp,'count.csv'),join(DEST_S3_KEY,'count.csv'))
    ]
    bucket = conns3.Bucket(s3bucket)
    for path in s3paths: 
        bucket.download_file(path[1], path[0])


def load_rds_data(urls, engine):
    for url in urls:
        push_datapackage(descriptor=url,backend='sql',engine=connRDS)

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
    create_cube = dedent('''
    CREATE TABLE agg_risk_country_{time}(
        date DATE, risk INT,
        country VARCHAR(2),
        count BIGINT,
        count_amplified FLOAT
        )''')
    populate_cube = dedent('''
    INSERT INTO agg_risk_country_{time}
        (SELECT date_trunc('{time}', date) AS date, risk, country, 
        SUM(count) AS count, SUM(count_amplified) FROM fact_count
    GROUP BY CUBE(date, country, risk) ORDER BY date DESC, country)
    ''')

    connRDS.execute(create_risk)
    connRDS.execute(create_country)
    connRDS.execute(create_asn)
    connRDS.execute(create_time)
    connRDS.execute(create_count)
    connRDS.execute(update_time)
    create_or_update_cubes(create_cube)
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


if __name__ == '__main__':
    # AGGREGATION
    tmpdir = tempfile.mkdtemp()
    upload_manifest(tmpdir)
    create_redshift_tables()
    load_data()
    load_ref_data()
    count_data()
    aggregate()
    update_amplified_count()
    unload('count', unload_key)
    unload_key = join(DEST_S3_KEY,'count')
    # LOAD TO RDS
    print("Loading to RDS")
    download(tmpdir)
    load_rds_data(urls, connRDS)
    create_rds_tables()
    os.system(copy_commands.format(tmp=tmpdir, password=env['RDS_PASSWORD'], host=env['RDS_HOST'], db=STAGE))
    # create_constraints()
    # create_indexes()
    shutil.rmtree(tmpdir)
