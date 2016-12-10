from __future__ import print_function

from datapackage import push_datapackage
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine
from os.path import dirname, join
from textwrap import dedent

import datapackage
import psycopg2
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
connRedshift = psycopg2.connect(
    database=STAGE,
    user=env['REDSHIFT_USER'],
    password=env['REDSHIFT_PASSWORD'],
    host=env['REDSHIFT_HOST'],
    port=env['REDSHIFT_PORT']
)
# S3 connection
conns3 = boto3.resource('s3',
	aws_access_key_id=AWS_ACCESS_KEY,
	aws_secret_access_key=AWS_ACCESS_SECRET_KEY)
# RDS connection
connRDS = psycopg2.connect(
    database=STAGE,
    user=env['RDS_USER'],
    password=env['RDS_PASSWORD'],
    host=env['RDS_HOST'],
    port=env['RDS_PORT']
    )

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
    cursor = connRedshift.cursor()
    drop_tables(cursor, tablenames)
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
    cursor.execute(create_risk)
    cursor.execute(create_logentry)
    cursor.execute(create_count)
    connRedshift.commit()
    print('Redshift tables created')


def load_data():
    role_arn = REDSHIFT_ROLE_ARN
    manifest = join('s3://', SOURCE_S3_BUCKET, SOURCE_S3_KEY,'clean.manifest')
    cursor = connRedshift.cursor()
    copycmd = dedent('''
    COPY logentry FROM '%s'
    CREDENTIALS 'aws_iam_role=%s'
    IGNOREHEADER AS 1
    DELIMITER ',' gzip
    TIMEFORMAT AS 'auto'
    MANIFEST
    ''')%(manifest, role_arn)
    print('Loading data into db ... ')
    cursor.execute(copycmd)
    connRedshift.commit()
    print('Data Loaded')


def load_ref_data():
    url = 'https://raw.githubusercontent.com/cybergreen-net/refdata-risk/master/datapackage.json'
    dp = datapackage.DataPackage(url)
    risks = dp.resources[0].data
    cursor = connRedshift.cursor()
    query = dedent("""
    INSERT INTO dim_risk
    VALUES (%(id)s, %(slug)s, %(title)s, %(amplification_factor)s, %(description)s)""")
    for risk in risks:
        # description is too long and not needed here
        risk["description"]=""
        cursor.execute(query,risk)
    connRedshift.commit()


def count_data():
	cmd = 'SELECT count(*) FROM logentry'
	cursor = connRedshift.cursor()
	cursor.execute(cmd)
	print(cursor.fetchone()[0])
	connRedshift.commit()


def aggregate():
    print('Aggregating ...')
    cursor = connRedshift.cursor()
    query = dedent("""
    INSERT INTO count
    (SELECT
        date, risk, country, asn, count(*) as count, 0 as count_amplified
    FROM(
    SELECT DISTINCT (ip), date_trunc('day', date) AS date, risk, asn, country FROM logentry) AS foo
    GROUP BY date, asn, risk, country ORDER BY date DESC, country ASC, asn ASC, risk ASC)
    """)
    cursor.execute(query)
    connRedshift.commit()

def update_amplified_count():
    print('Calculating Amplificated Counts ...')
    cursor = connRedshift.cursor()
    query = dedent("""
    UPDATE count
    SET count_amplified = count*amplification_factor
    FROM dim_risk WHERE risk=id
    """)
    cursor.execute(query)
    connRedshift.commit()
    print('Aggregation Finished!')


def unload(table, s3path):
    cursor = connRedshift.cursor()
    role_arn = REDSHIFT_ROLE_ARN
    s3bucket = join("s3://", DEST_S3_BUCKET)
    aws_auth_args = 'aws_access_key_id=%s;aws_secret_access_key=%s'%(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY)
    s3path = join(s3bucket, s3path)
    cursor.execute(dedent("""
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
-c "\COPY risk FROM {tmp}/ref_risk.csv WITH delimiter as ',' null '' csv header;"

psql -h \
{host} \
-U cybergreen -d {db} -p 5432 \
-c "\COPY country FROM {tmp}/ref_country.csv WITH delimiter as ',' null '' csv header;"

psql -h \
{host} \
-U cybergreen -d {db} -p 5432 \
-c "\COPY country_asn FROM {tmp}/ref_country_asn.csv WITH delimiter as ',' null '' csv header;"

psql -h \
{host} \
-U cybergreen -d {db} -p 5432 \
-c "\COPY count_by_risk FROM {tmp}/risk.csv WITH delimiter as ',' null '' csv;"

psql -h \
{host} \
-U cybergreen -d {db} -p 5432 \
-c "\COPY count_by_country FROM {tmp}/country.csv WITH delimiter as ',' null '' csv;"

psql -h \
{host} \
-U cybergreen -d {db} -p 5432 \
-c "\COPY count FROM {tmp}/count.csv WITH delimiter as ',' null '' csv;"
"""


def download(tmp):
    s3bucket = DEST_S3_BUCKET
    s3paths = [
        (join(tmp,'count.csv'),join(DEST_S3_KEY,'count.csv')), 
        (join(tmp,'country.csv'),join(DEST_S3_KEY,'country.csv')), 
        (join(tmp,'risk.csv'),join(DEST_S3_KEY,'risk.csv')),
        (join(tmp,'ref_risk.csv'),join(REFERENCE_KEY,'risk.csv')),
        (join(tmp,'ref_country.csv'),join(REFERENCE_KEY,'country.csv')),
        (join(tmp,'ref_country_asn.csv'),join(REFERENCE_KEY,'asn.csv'))
    ]
    bucket = conns3.Bucket(s3bucket)
    for path in s3paths: 
        bucket.download_file(path[1], path[0])
        
def create_tables():	
    cursor = connRDS.cursor();
    tablenames = [
        'count', 'count_by_country', 'count_by_risk',
        'risk', 'country', 'country_asn'
    ]
    for tablename in tablenames:
        cursor.execute("select exists(SELECT * FROM information_schema.tables WHERE table_name='%s')"%tablename)	
        if cursor.fetchone()[0]:
            cursor.execute('DROP TABLE %s'%tablename)
    create_count = """
CREATE TABLE count
(risk int, country varchar(2), asn bigint, date date, period_type varchar(8), count int);
"""
    create_count_by_country = """
CREATE TABLE count_by_country
(risk int, country varchar(2), date date, count bigint, score real, rank int);
"""
    create_count_by_risk = """
CREATE TABLE count_by_risk
(risk int,  date date, count bigint, max bigint);
"""
    create_risk = """
CREATE TABLE risk
(id varchar(16),  risk_id int, title varchar(32), category text, description text);
"""
    create_country= """
CREATE TABLE country
(id varchar(2),name varchar(32),slug varchar(32),region varchar(32),continent varchar(32));
"""
    create_country_asn = """
CREATE TABLE country_asn
(country varchar(2),  asn varchar(10), date date);
"""
    cursor.execute(create_risk)
    cursor.execute(create_country)
    cursor.execute(create_country_asn)
    cursor.execute(create_count)
    cursor.execute(create_count_by_country)
    cursor.execute(create_count_by_risk)
    connRDS.commit();

def create_indexes():
	cursor = connRDS.cursor()
	idx_dict = {
		# Index to speedup /api/v1/count
		"idx_total_count": "CREATE INDEX idx_total_count ON count (date, country, risk, asn, period_type);",
		"idx_all_desc": "CREATE INDEX idx_all_date_desc on count (date DESC, country, risk, asn, period_type);",
		# Index to speedup /api/v1/count when asn is given
		"idx_asn": "CREATE INDEX idx_asn ON count (asn);",
		"idx_country": "CREATE INDEX idx_country ON count(country);",
		"idx_date": "CREATE INDEX idx_date ON count(date);",
		"idx_date_cbc": "CREATE INDEX idx_date_cbc ON count_by_country(date);",
		"idx_risk_cbc": "CREATE INDEX idx_risk_cbc ON count_by_country(risk);",
		"idx_country_cbc": "CREATE INDEX idx_country_cbc ON count_by_country(country);",
		"idx_risk_cbr": "CREATE INDEX idx_risk_cbr ON count_by_risk(risk);",
		"idx_date_cbc": "CREATE INDEX idx_date_cbr ON count_by_risk(date);",
		}
	for idx in idx_dict:
		cursor.execute(idx_dict[idx])
	connRDS.commit()

def drop_tables(cursor, tables):
    for tablename in tables:
        cursor.execute("DROP TABLE IF EXISTS %(table)s CASCADE",{"table": AsIs(tablename)})


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
    unload_key = join(DEST_S3_KEY,'count')
    unload('count', unload_key)
    print("Unloading datata to S3")
    # LOAD TO RDS
    print("Loading to RDS")
    download(tmpdir)
    create_tables()
    os.system(copy_commands.format(tmp=tmpdir, password=env['RDS_PASSWORD'], host=env['RDS_HOST'], db=STAGE))
    create_indexes()
    shutil.rmtree(tmpdir)
