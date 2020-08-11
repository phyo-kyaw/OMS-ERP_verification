import sys
from suds.client import Client, WebFault
#import csv
#from chunks import *
#from datetime import timedelta, datetime
#import threading
#from copy import deepcopy
#from itertools import repeat

class NetSuiteOperation:

	def __init__(self):
	
		self.url =  'https://webservices.netsuite.com/wsdl/v2013_2_0/netsuite.wsdl'
		self.client = Client(self.url, faults=False, timeout=500)
		
	def login(self):
		self.passport = self.client.factory.create('ns4:Passport')
		self.passport.email = 'duhita.sk@zalora.com'
		self.passport.password = 'Netsuite+1'
		self.passport.account = '3797307'
		loginResult = self.client.service.login(self.passport)

		if loginResult.status._isSuccess is False:
			print  loginResult
			sys.exit()
	
	def loginWithID(self, loginId, pw ):
		self.passport = self.client.factory.create('ns4:Passport')
		self.passport.email = loginId
		self.passport.password = pw
		self.passport.account = '3797307'
		loginResult = self.client.service.login(self.passport)

		
	def logout(self):
		loginResult = self.client.service.logout()
	'''
	def GetRecord(self, singleTSB, recordsFile):

		
		file1 = open(recordsFile, 'rb')
		reader = csv.reader(file1)
		rows_list = []
		updated_rows_list = []
		for row in reader:
			  rows_list.append(row)
		file1.close()   # <---IMPORTANT

		f = open(recordsFile,"wb")
		c = csv.writer(f)
		for row in rows_list:
			singleTSB.tranId.searchValue = row[0]
			print row[0]
			if row[0] <> '' :
				resultWS = self.client.service.search(singleTSB)
				if resultWS.status._isSuccess is False:
					print 'fail to get result'
					row1 = [row[0], row[1], 'Failed']
				else:
					print resultWS
					if resultWS.totalRecords == 0 :
						row1 = [row[0], row[1], 'Not found' ]
					elif resultWS.totalRecords == 1 :
						retDate = resultWS.recordList.record[0].tranDate.isoformat(' ')			
						row1 = [row[0], row[1], retDate ]
					else:
						row1 = [row[0], row[1],'Many found' ]
						updated_rows_list.append(row1)
			else :
				row = [row[0], row[1], 'Nothing']
				updated_rows_list.append(row1)	
			c.writerow(row1)
		f.close()
		'''
	def searchRecords(self, TSB ): # fileOMS, TSB, fileNS):
		#print '\nstart a search'
		return self.client.service.search(TSB)

		'''
		write_lock = threading.Lock()
		file1 = open(fileOMS, 'rb')
		reader = csv.reader(file1)
		rows_list = []
		for row in reader:
			  rows_list.append(row)
		file1.close()   # <---IMPORTANT
		
		LORFs = [[] for i in repeat(None,2)]
		localTSB = [deepcopy(TSB) for i in repeat(None,2)]
		
		#if len(rows_list) < 400:
		#	for i in range(0,len(rows_list)):
		#		LORFs.append(self.client.factory.create( 'ns4:ListOrRecordRef' ))			
		#else :
		NS4 = self.client.factory.create( 'ns4:ListOrRecordRef' )
		for i in range(0,400):
			LORFs[0].append(deepcopy(NS4))
			LORFs[1].append(deepcopy(NS4))	
			
		listTotal = list(chunks(rows_list, 800))
		
		print 'started at ', datetime.now().isoformat(' ')

		for chunk in listTotal:
			j = 0
			for item in chunk:
				
				if j < 400 :
					LORFs[0][j]._externalId = item[0]
				else :
					LORFs[1][j]._externalId = item[0]
				j = j + 1
			
			
			localTSB[0].externalId.searchValue = LORFs[0]
			localTSB[1].externalId.searchValue = LORFs[1]
			resultWS = self.client.service.search(TSB)
			
			with write_lock:
				f = open(fileNS,"wb")
				c = csv.writer(f)			
				if resultWS.status._isSuccess is True:
					i = 0
					while i <  resultWS.totalRecords :
							row = resultWS.recordList.record[i]
							row1 = [ row._externalId, row.tranDate]
							#print row1
							c.writerow(row1)
							i = i + 1
				print datetime.now().isoformat(' ')
				f.close()

	def getRecordsFile( self, recordsFile, TSB, stringToSearch):
		resultWS = self.client.service.search(TSB)
		print resultWS
		if resultWS.status._isSuccess is True:
		
			f = open(recordsFile,"wb")
			c = csv.writer(f)
			print resultWS.totalRecords
			if resultWS.totalRecords > 0 :

				i = 0
				if resultWS.totalPages > 1 :
					loopEnd = 1000
				else :
					loopEnd = resultWS.totalRecords
				while i <  loopEnd :
					row = resultWS.recordList.record[i]
					if stringToSearch in str(row) :
						row1 = [ row._externalId, row.tranDate]
						print row1
						c.writerow(row1)
					i = i + 1

			
				if resultWS.totalPages > 1 :
					for x in range ( 2, ( resultWS.totalPages + 1 ) ) :
						resultWS = self.client.service.searchMoreWithId ( resultWS.searchId, x )
						
						if x < resultWS.totalPages :
							loopEnd = 1000
						else :
							loopEnd = resultWS.totalRecords % 1000
						i = 0
						while i < loopEnd :
							row = resultWS.recordList.record[i]
							if stringToSearch in str(row) :
								print [ row._externalId, row.tranDate]
								row1 = [ row._externalId, row.tranDate]
								print row1
								c.writerow(row1)
							i = i + 1
			
			f.close()			

		'''
		
		