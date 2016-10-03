import boto3
import json
import psycopg2
import os
import csv

env = json.load(open('../.env.json'))

AWS_ACCESS_KEY = env['AWS_ACCESS_KEY']
AWS_ACCESS_SECRET_KEY = env['AWS_ACCESS_SECRET_KEY']
PASSWORD = env['RDS_PASSWORD']

conns3 = boto3.resource('s3',
	aws_access_key_id=AWS_ACCESS_KEY,
	aws_secret_access_key=AWS_ACCESS_SECRET_KEY)

connRDS = psycopg2.connect(
	database='frontend',
	user='cybergreen',
	password=PASSWORD,
	host='cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com',
	port=5432
	)

def download():
	s3bucket = 'bits.cybergreen.net'
	s3paths = [
		('tmp/count.csv','stats/latest/count000'), 
		('tmp/country.csv','stats/latest/country000'), 
		('tmp/risk.csv','stats/latest/risk000')
	]
	bucket = conns3.Bucket(s3bucket)
	for path in s3paths: 
		bucket.download_file(path[1], path[0])
		
def create_tables():	
	cursor = connRDS.cursor();
	tablenames = ['count', 'count_by_country', 'count_by_risk']
	for tablename in tablenames:
		cursor.execute("select exists(SELECT * FROM information_schema.tables WHERE table_name='%s')"%tablename)	
		if cursor.fetchone()[0]:
			cursor.execute('DROP TABLE %s'%tablename)
	create_count = """
CREATE TABLE count
(id SERIAL PRIMARY KEY, risk int, country varchar(2), asn bigint, date date, period_type varchar(8), count int);
"""
	create_count_by_country = """
CREATE TABLE count_by_country
(risk int, country varchar(2), date date, count bigint, score real, rank int);
"""
	create_count_by_risk = """
CREATE TABLE count_by_risk
(risk int,  date date, count bigint, max bigint);
"""
	cursor.execute(create_count)
	cursor.execute(create_count_by_country)
	cursor.execute(create_count_by_risk)
	connRDS.commit();

def create_indexes():
	cursor = connRDS.cursor()
	idx_dict = {
		"idx_risk": "CREATE INDEX idx_risk ON count(risk);",
		"idx_country": "CREATE INDEX idx_country ON count(country);",
		"idx_asn": "CREATE INDEX idx_asn ON count(asn);",
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
		
if __name__ == '__main__':
	download()
	create_tables()
	create_indexes()