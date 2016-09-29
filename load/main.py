from __future__ import print_function

import json
import urllib
import psycopg2
import boto

env = json.load(open('../.env.json'))

tablename = 'logentry'

create_table_name = '''
CREATE TABLE %s (
   date timestamp,
   ip   varchar(30),
   risk int,
   asn  bigint,
   place varchar(3)
   )
''' % tablename


connection = psycopg2.connect(
    database=env['REDSHIFT_DBNAME'],
    user=env['REDSHIFT_USER'],
    password=env['REDSHIFT_PASSWORD'],
    host=env['REDSHIFT_HOST'],
    port=env['REDSHIFT_PORT']
)
print('Connecting ...')

def create_table():
	#CREATE REDSHIFT TABLE WHEN CSV FILE UPLOADED
	cursor = connection.cursor();
	cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (tablename,))

	if (cursor.fetchone()[0]):
		print('Table already exists')
		return
	else:
		cursor.execute(create_table_name)
		connection.commit();

def load_data():
	role_arn = 'arn:aws:iam::635396214416:role/RedshiftCopyUnload'
	manifest = 's3://private-bits-cybergreen-net/dev/clean/clean.manifest'
	cursor = connection.cursor()
	copycmd = '''
COPY %s FROM '%s'
CREDENTIALS 'aws_iam_role=%s'
IGNOREHEADER AS 1
DELIMITER ',' gzip
TIMEFORMAT AS 'auto'
MANIFEST;
'''%(tablename, manifest, role_arn)
	print('Loading data into db ... ')
	cursor.execute(copycmd)
	connection.commit()
	print('Data Loaded')

def count_data():
	cmd = 'SELECT count(*) FROM %s' % tablename
	cursor = connection.cursor();
	cursor.execute(cmd)
	print(cursor.fetchone()[0])
	connection.commit()

def unload(table, s3path):
	cursor = connection.cursor();
	role_arn = 'arn:aws:iam::635396214416:role/RedshiftCopyUnload'
	s3bucket = 's3://bits.cybergreen.net'
	aws_auth_args = 'aws_access_key_id=%s;aws_secret_access_key=%s'%(env['AWS_ACCESS_KEY'], env['AWS_ACCESS_SECRET_KEY'])
	s3path = s3bucket + s3path
	print('Unloading datata to S3')
	cursor.execute("""
UNLOAD('SELECT * FROM %s')
TO '%s'
CREDENTIALS '%s'
DELIMITER AS ','
ALLOWOVERWRITE
PARALLEL OFF;
"""%(table, s3path, aws_auth_args))
	print('Unload Successfully')

def create_count():
    tablename = 'count'
    copytable = 'logentry'
    cursor = connection.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (tablename,))
    if (cursor.fetchone()[0]):
        cursor.execute("DROP TABLE %s"%(tablename))
        print('Drop table %s'%(tablename))
    create = """
CREATE TABLE %s (
id BIGSERIAL PRIMARY KEY,
risk int,
country varchar(2),
asn  bigint,
date varchar(16),
period_tipe varchar(8),
count int
)
""" % (tablename)
    cursor.execute(create)
    connection.commit()
    query = """
    INSERT INTO %s(risk, country, asn, date, period_tipe, count)
    (SELECT risk, place as country, asn, TO_CHAR(date_trunc('week', date), 'YYYY-MM-DD') AS "date", 'monthly', COUNT(*) AS count
    FROM %s GROUP BY TO_CHAR(date_trunc('week', date), 'YYYY-MM-DD'), asn, risk, place)
    """%(tablename, copytable)
    cursor.execute(query)
    connection.commit()
    print('%s table created'%(tablename))

def create_count_by_country():
	tablename = 'count_by_country'
	copytable = 'count'
	cursor = connection.cursor()
	cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (tablename,))
	if (cursor.fetchone()[0]):
		  cursor.execute("DROP TABLE %s"%(tablename))
	create = """
CREATE TABLE %s (
	risk int,
	country varchar(2),
	date varchar(16),
	count bigint,
	score real,
	rank int
)
"""%(tablename)
	cursor.execute(create)
	connection.commit()
	query = """
INSERT INTO %s
(SELECT risk, country, date, SUM(count) AS count, 0, 0
FROM %s GROUP BY date, risk, country)
"""%(tablename, copytable)
	cursor.execute(query)
	connection.commit()
	print('%s table created'%(tablename))

def create_count_by_risk():
	tablename = 'count_by_risk'
	copytable = 'count_by_country'
	cursor = connection.cursor()
	cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (tablename,))
	if (cursor.fetchone()[0]):
			cursor.execute("DROP TABLE %s"%(tablename))
	create = """
CREATE TABLE %s (
	risk int,
	date varchar(16),
	count bigint,
	max bigint
	)
"""%(tablename)
	cursor.execute(create)
	connection.commit()
	query = """
INSERT INTO %s
(SELECT risk, date, SUM(count), max(count)
FROM %s GROUP BY date, risk)
"""%(tablename, copytable)
	cursor.execute(query)
	connection.commit()
	print('%s table created'%(tablename))

def update_with_scores():
	cursor = connection.cursor()
	risktable = 'count_by_risk'
	countrytable = 'count_by_country'
	query = """
UPDATE {0}
SET score = 100 * ( LOG({0}.count) / LOG({1}.max) )
FROM {1}
WHERE {0}.risk = {1}.risk AND {0}.date = {1}.date;
""".format(countrytable, risktable)
	cursor.execute(query)
	connection.commit()

def get_manifest():
	
	conn = boto.connect_s3(
		aws_access_key_id = env['AWS_ACCESS_KEY'],
		aws_secret_access_key = env['AWS_ACCESS_SECRET_KEY']
		)
	
	s3bucket = 'private-bits-cybergreen-net'
	key = 'dev/clean/datapackage.json'
	bucket = conn.get_bucket(s3bucket)
	key = bucket.get_key(key)
	datapackage = key.get_contents_as_string()
	datapackage = json.loads(datapackage)
	manifest = {"entries": []}
	keys = (p['path'] for p in datapackage['resources'])
	for key_list in keys:
		for key in key_list:
			manifest['entries'].append({"url": "s3://private-bits-cybergreen-net/dev/clean/"+key, "mandatory": True})
	f = open('clean.manifest', 'w')
	json.dump(manifest, f)
	f.close()
	
	k = boto.s3.key.Key(bucket)
	k.key = 'dev/clean/clean.manifest'
	k.set_contents_from_filename('clean.manifest')
	
if __name__ == '__main__':
	get_manifest()
	create_table()
	load_data()
	count_data()
	create_count()
	create_count_by_country()
	create_count_by_risk()
	update_with_scores()
	# this needs to be automated 
	table_keys = {
	'count': '/stats/latest/count',
	'count_by_country': '/stats/latest/country',
	'count_by_risk': '/stats/latest/risk'
	}
	for table in table_keys:
		unload(table, table_keys[table])

