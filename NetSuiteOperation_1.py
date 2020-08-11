import sys
from suds.client import Client



def ResultIter(cur, arraysize=1000):
	'An iterator that uses fetchmany to keep memory usage down'
	while True:
		results = cur.fetchmany(arraysize)
		if not results:
			break
		for result in results:
			yield result

url =  'https://webservices.netsuite.com/wsdl/v2014_1_0/netsuite.wsdl'
client = Client(url)

passport = client.factory.create('ns4:Passport')

passport.email = 'philip.sai@zalora.com'
passport.password = 'Phy0ky6w1309&'
passport.account = '3797307'

loginResult = client.service.login(passport)

if loginResult.status._isSuccess <> 'True':
	print  loginResult
	sys.exit()
else:
	TSB = client.factory.create( 'ns2:TransactionSearchBasic' )
	TSB.type.searchValue = '_salesOrder'
	TSB.type._operator = 'anyOf'
	TSB.externalIdString.searchValue = 'SO_ZSG_206962146'
	TSB.externalIdString._operator = 'is'
	
	resultWS = client.service.search(TSB)
	if resultWS.status._isSuccess <> 'True':
		print 'fail to get result'
	else:
		print resultWS.status._isSuccess 