import unittest
import psycopg2
import json
import datetime
import math

env = json.load(open('../.env.json'))

conn = psycopg2.connect(
    database=env['REDSHIFT_DBNAME'],
    user=env['REDSHIFT_USER'],
    password=env['REDSHIFT_PASSWORD'],
    host=env['REDSHIFT_HOST'],
    port=env['REDSHIFT_PORT']
)

class TestProcess(unittest.TestCase):
	def test_a(self):
		'''Test connecton
		'''
		self.assertEqual(conn.closed, 0)
		self.assertEqual(conn.status, 1)
		
	def test_b(self):
		'''Test table creation
		'''
		tablename = 'tmptable'
		existstable = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)"
		createtable = "CREATE TABLE %s (date datetime, ip varchar(30), risk int, asn  bigint, place varchar(2))"%(tablename)
		cursor = conn.cursor()
		
		cursor.execute(existstable, (tablename,))
		self.assertFalse(cursor.fetchone()[0])
		
		cursor.execute(createtable)
		conn.commit()
		cursor.execute(existstable, (tablename,))
		self.assertTrue(cursor.fetchone()[0])
		
	def test_c(self):
		'''Test data loaded
		'''
		tablename = 'tmptable'
		role_arn = 'arn:aws:iam::635396214416:role/RedshiftCopyUnload'
		manifest = 's3://private-bits-cybergreen-net/dev/tests/test.manifest'
		count = "SELECT COUNT(*) FROM %s"%(tablename)
		select = "SELECT * FROM %s"%(tablename)
		copycmd = '''
COPY %s FROM '%s'
CREDENTIALS 'aws_iam_role=%s'
IGNOREHEADER AS 1
DELIMITER ',' gzip
TIMEFORMAT AS 'auto'
MANIFEST;
'''%(tablename, manifest, role_arn)
		
		cursor = conn.cursor()
		cursor.execute(count)
		self.assertEquals(cursor.fetchone()[0], 0)
		
		cursor.execute(copycmd)
		conn.commit()
		
		cursor.execute(count)
		rows = cursor.fetchone()[0]
		self.assertEquals(rows, 300)
		
		cursor.execute(select)
		row = cursor.fetchone()
		self.assertEquals(type(row[0]), datetime.datetime)
		self.assertEquals(type(row[1]), str)
		self.assertEquals(type(row[2]), int)
		self.assertEquals(type(row[3]), long)
		self.assertEquals(type(row[4]), str)

	def test_d(self):
		'''test aggregation of entries
		'''
		tablename = 'tmpentries'
		copytable = 'tmptable'
		existstable = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)"
		select = "SELECT * FROM %s"%(tablename)
		select_sum = "SELECT sum(count) FROM %s"%(tablename)	
		create = """
CREATE TABLE %s (
   id BIGINT IDENTITY(0,1),
   risk int,
   country varchar(2),
   asn  bigint,
   date varchar(16),
   period_tipe varchar(8),
   count int
   )
"""%(tablename)
		query = """
INSERT INTO %s(risk, country, asn, date, period_tipe, count)
(SELECT risk, place as country, asn, TO_CHAR(date_trunc('week', date), 'YYYY-MM-DD') AS "date", 'weekly', COUNT(*) AS count
FROM %s GROUP BY TO_CHAR(date_trunc('week', date), 'YYYY-MM-DD'), asn, risk, place)
"""%(tablename, copytable)
		
		cursor = conn.cursor()
		cursor.execute(existstable, (tablename,))
		self.assertFalse(cursor.fetchone()[0])
		cursor.execute(create)
		conn.commit()
		
		cursor.execute(select)
		rows = cursor.fetchall()
		self.assertTrue(len(rows) == 0)
		
		cursor = conn.cursor()
		cursor.execute(existstable, (tablename,))
		self.assertTrue(cursor.fetchone()[0])
			
		cursor.execute(query)
		conn.commit()
		
		cursor.execute(select)
		row = cursor.fetchone()
		rows = cursor.fetchall()
		self.assertTrue(len(rows) > 0)
		self.assertEquals(len(row), 7)

		cursor.execute(select_sum)
		row = cursor.fetchone()
		self.assertEquals(row[0], 300)
	
	def test_e(self):
		'''Test aggregation of places
		'''
		tablename = 'tmpplaces'
		copytable = 'tmpentries'
		existstable = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)"
		select = "SELECT * FROM %s"%(tablename)
		select_sum = "SELECT sum(count) FROM %s"%(tablename)
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
		query = """
INSERT INTO %s
(SELECT risk, country, date, SUM(count) AS count, 0, 0
FROM %s GROUP BY date, risk, country)
"""%(tablename, copytable)

		cursor = conn.cursor()
		
		cursor.execute(existstable, (tablename,))
		self.assertFalse(cursor.fetchone()[0])

		cursor.execute(create)
		conn.commit()		
		cursor.execute(existstable, (tablename,))
		self.assertTrue(cursor.fetchone()[0])
		cursor.execute(select)
		rows = cursor.fetchall()
		self.assertTrue(len(rows) == 0)
		
		cursor.execute(query)
		conn.commit()
		cursor.execute(select)
		rows = cursor.fetchall()
		self.assertTrue(len(rows) > 0)
		cursor.execute(select_sum)
		row = cursor.fetchone()
		self.assertEquals(row[0], 300)
	
	def test_f(self):
		'''Test aggregation of risks
		'''
		tablename = 'tmprisks'
		copytable = 'tmpplaces'
		existstable = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)"
		select = "SELECT * FROM %s"%(tablename)
		select_sum = "SELECT sum(count) FROM %s"%(tablename)
		create = """
CREATE TABLE %s (
   risk int,
   date varchar(16),
   count bigint,
   max bigint
   )
"""%(tablename)
		query = """
INSERT INTO %s
(SELECT risk, date, SUM(count), max(count)
FROM %s GROUP BY date, risk)
"""%(tablename, copytable)

		cursor = conn.cursor()
		
		cursor.execute(existstable, (tablename,))
		self.assertFalse(cursor.fetchone()[0])

		cursor.execute(create)
		conn.commit()
		
		cursor.execute(existstable, (tablename,))
		self.assertTrue(cursor.fetchone()[0])
		cursor.execute(select)
		rows = cursor.fetchall()
		self.assertTrue(len(rows) == 0)
		
		cursor.execute(query)
		conn.commit()
		cursor.execute(select)
		rows = cursor.fetchall()
		self.assertTrue(len(rows) > 0)
		cursor.execute(select_sum)
		row = cursor.fetchone()
		self.assertEquals(row[0], 300)
	
	def test_g(self):
		'''Test score computing
		'''
		risktable = 'tmprisks'
		placetable = 'tmpplaces'
		select = "SELECT score FROM %s WHERE country='BR' and risk=4"%(placetable)
		query = """
UPDATE {0}
SET score = (LOG({1}.max) - LOG({0}.count))/(LOG({1}.max))*100
FROM {1}
WHERE {0}.risk = {1}.risk AND {0}.date = {1}.date;
""".format(placetable, risktable)
		cursor = conn.cursor()

		cursor.execute(select)
		row = cursor.fetchone()
		self.assertEquals(row[0], 0)
		
		cursor.execute(query)
		conn.commit()
		
		cursor.execute(select)
		row = cursor.fetchone()
		self.assertEquals(round(row[0], 4), round((math.log(20)-math.log(5))/math.log(20) *100, 4))
		
	def test_h(self):
		'''Test teardown
		'''
		existstable = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)"
		table_names = ['tmptable', 'tmpentries', 'tmpplaces', 'tmprisks'] 
		cursor = conn.cursor()
		for table in table_names:
			cursor.execute('DROP TABLE %s'%(table))	
			cursor.execute(existstable, (table,))
			self.assertFalse(cursor.fetchone()[0])
			conn.commit()
		
if __name__ == '__main__':
    unittest.main()
