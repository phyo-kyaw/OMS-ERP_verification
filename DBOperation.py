import sys 
import MySQLdb 
from mysql.connector.constants import ClientFlag 

class DBOperation:
	def __init__(self):
		self.cnx = MySQLdb.connect(user='philip.sai', host='copycat.zalora.com', ssl_ca='C:\\Users\\user\\Desktop\\ca.crt', ssl_cert='C:\\Users\\user\\Desktop\\philip.sai.crt', ssl_key='C:\\Users\\user\\Desktop\\nyancat_private_key.ppk') 
		self.cnx.database='oms_live_sg'
		self.curA = self.cnx.cursor(buffered=True)
		print 'connected to OMS database.'
		
	def GetPurchaseOrders(self,query):
		self.query=query
		self.curA.execute(query)
		return self.curA
		
	

		
	#def __del__(self):
		#self.cnx.close()
		#curA.close() 