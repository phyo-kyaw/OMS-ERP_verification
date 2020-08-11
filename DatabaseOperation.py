import sys 
import csv
import mysql.connector 
from mysql.connector.constants import ClientFlag 
from mysql.connector.cursor import MySQLCursor

class DatabaseOperation:
	def __init__(self):
		pass
		
	def getConnected(self):		
		for tryLoop in range(5):
				try:
					self.cnx = mysql.connector.connect(user='philip.sai', host='copycat.zalora.com', ssl_ca='C:\\Users\\user\\Desktop\\ca.crt', ssl_cert='C:\\Users\\user\\Desktop\\philip.sai.crt', ssl_key='C:\\Users\\user\\Desktop\\nyancat_private_key.ppk') 
					self.cnx.database='oms_live_sg'
					self.curA = self.cnx.cursor(buffered=True)
					#print 'connected to OMS database.'
				except mysql.connector.Error, omsErr:
					print omsErr
					continue
				break
		else:
			print 'OMS LOGIN failed 5 retries!!!!!'
	

		
	def getConnectedDB(self, DB):
		self.cnx = mysql.connector.connect(user='philip.sai', host='copycat.zalora.com', ssl_ca='C:\\Users\\user\\Desktop\\ca.crt', ssl_cert='C:\\Users\\user\\Desktop\\philip.sai.crt', ssl_key='C:\\Users\\user\\Desktop\\nyancat_private_key.ppk') 
		self.cnx.database=DB
		self.curA = self.cnx.cursor(buffered=True)
		
	def getDisonnected(self):
		
		for tryLoop in range(5):
				try:
					self.cnx.close()
				except mysql.connector.Error, omsErr:
					print omsErr
					continue
				break
		else:
			print 'OMS LOGOUT failed 5 retries!!!!!'
		
	def getRecords(self, query, date_1, date_2, divider, fileName):
		f = open(fileName,"ab")
		c = csv.writer(f)
		#print query
		for sub_no in range(0, divider):
			print date_1,  date_2, divider, sub_no
			self.curA.execute(query, { 'd1' : date_1, 'd2' : date_2, 'div' : divider, 'remainder' : sub_no })
			results = self.curA.fetchall()
			for result in results:
				c.writerow(result)
		f.close()
		
	def getCount(self, query, date_1, date_2):

		countQuery =  "select count(*) from ( " + query + " )AAA"
		self.curA.execute(countQuery, { 'd1' : date_1, 'd2' : date_2, 'div' : 1, 'remainder' : 0 })
		count = self.curA.fetchone()
		print date_1,  date_2, count
		return count[0]
		
	def getCountQ(self, query):

		countQuery =  "select count(*) from ( " + query + " )AAA"
		self.curA.execute(countQuery)
		count = self.curA.fetchone()
		#print date_1,  date_2, count
		return count[0]
	
	def executeQuery(self, query):
		result = self.curA.execute(query,  { 'db' : 'oms_live_sg.', } )
		print self.curA.statement
		result = self.curA.fetchall()
		return 	result

		
	def fetchRecords(self, query, divider, fileName):
		f = open(fileName,"wb")
		c = csv.writer(f)
		for sub_no in range(0, divider):
			print str(sub_no)
			Request =   query + str(sub_no) 
			print Request
			self.curA.execute(Request)
			
			results = self.curA.fetchmany(size=500)
			while len(results) > 0 :
				for result in results:
					c.writerow(result)
				results = self.curA.fetchmany(size=500)
				print '500'
			
		f.close()
		
	def getAndWriteRecords(self, query, fileName):
		f = open(fileName,"ab")
		c = csv.writer(f)

		self.curA.execute(query)
		print 'gotten results'
		results = self.curA.fetchmany(size=500)
		while len(results) > 0 :
			for result in results:
				c.writerow(result)
			results = self.curA.fetchmany(size=500)
			print '500'
			
		f.close()
		
	#def __del__(self):
		#self.cnx.close()
		#curA.close() 