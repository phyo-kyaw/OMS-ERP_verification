from __future__ import generators
import os
import csv
from DatabaseOperation import *
from datetime import timedelta, datetime
from NetSuiteOperation import *
from FileOperation import *
import readchar
import threading
from copy import deepcopy
#from itertools import repeat
import time
from chunks import *
from multiprocessing import Process, Queue, current_process, Semaphore, Lock
from glob import glob


runOMS = True
runNS = True
runDiff = True

date_1 = datetime(2015,3,01)
date_2 = datetime(2015,4,1)
no_days = date_2 - date_1

OMS = None
sessionNS = None
FO = None
singleTSB = None
waitMain = True
localTSB = None
listTotal = None
NumberOfRecords = 300
NoOfProcess = 6
LORFs = [ None for x in range(NumberOfRecords) ]

writeSemaphore = Semaphore()
pPrintLock = Lock()

query = {}


query['PO'] = ( "select distinct (concat('PO_ZSG_',po.po_number)) OMS_PO_Nbr, po.created_at as ca, po.created_at "
	"from ims_purchase_order po "
	"where po.created_at >= %(d1)s " 
	"and po.created_at <  %(d2)s " 
	"and po.po_number not like 'MP%' "
	"and MOD(id_purchase_order,  %(div)s ) = %(remainder)s " )

query['SO'] =  ( "select distinct (concat('SO_ZSG_',iso.order_nr)), iso.imported_at, iso.created_at "
	"from ims_sales_order iso "
	"inner join "
	"ims_sales_order_item soi "
	"on iso.id_sales_order = soi.fk_sales_order "
	"where iso.imported_at >=  %(d1)s " 
	"and iso.imported_at <  %(d2)s "
	"and iso.created_at >= date_sub( %(d1)s, INTERVAL 30 day) " 
	"and iso.created_at <  %(d2)s "
	"and soi.is_marketplace <> 1 " 
	"and MOD(id_sales_order,  %(div)s ) = %(remainder)s " 
	"AND iso.order_nr like '%HK' " )
	
query['CD'] = ( " select distinct (concat('CD_ZSG_',iso.order_nr)) OMS_Order_Nbr, iso.imported_at, iso.created_at "
	"from oms_live_sg.ims_sales_order iso "
	"inner join oms_live_sg.ims_sales_order_item soi "
	"on iso.id_sales_order = soi.fk_sales_order "
	"inner join oms_live_sg.ims_sales_order_item_status_history soih "
	"on soi.id_sales_order_item = soih.fk_sales_order_item "
	"where soih.fk_sales_order_item_status = 67 "
	"and soih.updated_at >= %(d1)s " 
	"and soih.updated_at < %(d2)s " 
	"and iso.created_at >=  date_sub( %(d1)s, INTERVAL 60 day) " 
	"and iso.created_at <  %(d2)s " 
	"and iso.payment_method not in ('CashOnDelivery','PaidRemote_CashOnDelivery','SevenEleven','COD_NS45Voucher') "
	"and soi.is_marketplace <> 1 "
	"and iso.grand_total <> 0 "
	"and MOD(id_sales_order, %(div)s ) = %(remainder)s "
	"AND iso.order_nr like '%HK' " )	
	
	
query['IV'] =    ( "select distinct "
	"concat('IV_','ZSG','_',op.package_number) as Pkg_id, "
	"concat('IF_','ZSG','_',op.package_number) as Inv_id, "
	"TransDate "
	"from "
	"(SELECT distinct fk_package, "
	"created_at as TransDate, "
	"fk_package_status "
	"FROM oms_package_status_history "
		"where 1=1 "
		"and fk_package_status = 4 "
		"and created_at >= %(d1)s " 
		"and created_at < %(d2)s " 
	") ops "
	"left join oms_package as op ON(ops.fk_package = op.id_package) "
	"inner join oms_package_item as opi ON (op.id_package=opi.fk_package) "
	"inner join ims_sales_order_item as isoi  on (isoi.id_sales_order_item = opi.fk_sales_order_item) "
	"inner join ims_sales_order iso on (isoi.fk_sales_order=iso.id_sales_order) "
	"where isoi.is_marketplace <> 1 "
	"and iso.created_at >= date_sub( %(d1)s, INTERVAL 60 day) " 
	"and iso.created_at <  %(d2)s " 
	"and MOD(id_package,  %(div)s ) = %(remainder)s " 
	"AND iso.order_nr like '%HK' " )
	

query['CP'] = ( "select distinct "
	"concat('CP_','ZSG','_',op.package_number) as Payment_id, "
	"TransDate, TransDate as  TranDate "
	"from "
	"(  SELECT distinct fk_package,created_at as TransDate "
		"FROM oms_package_status_history "
		"where 1=1 "
		"and fk_package_status = 6 "
		"and created_at >=  %(d1)s " 
		"and created_at <  %(d2)s " 
	") ops "
	"left join oms_package as op ON(ops.fk_package = op.id_package) "
	"inner join oms_package_item as opi ON (op.id_package=opi.fk_package) "
	"inner join ims_sales_order_item as isoi  on (isoi.id_sales_order_item = opi.fk_sales_order_item) "
	"inner join ims_sales_order iso on (isoi.fk_sales_order=iso.id_sales_order) "
	"and isoi.is_marketplace <> 1 "
	"and iso.payment_method in ('CashOnDelivery','PaidRemote_CashOnDelivery','SevenEleven','COD_NS45Voucher') "
	"and iso.created_at >= date_sub( %(d1)s, INTERVAL 90 day) " 
	"and iso.created_at < %(d2)s " 
	"and MOD(id_package,   %(div)s ) = %(remainder)s " 
	"AND iso.order_nr like '%HK' " )

query['RAi'] = ( "select distinct concat('RA_ZSG_',rt.id_return_ticket) as RA_Id, concat('SO_','ZSG','_', iso.order_nr ), TranDate "
	"from ims_sales_order_item isoi "
	"inner join ims_sales_order iso "
	"on iso.id_sales_order = isoi.fk_sales_order "
	"inner join "
	"( 	SELECT distinct rt.fk_sales_order_item, rt.id_return_ticket, "
		"MAX(rtsh.created_at) as TranDate, opi.fk_package, opi.fk_sales_order_item as pkg_item "
		"FROM oms_return_ticket rt "
		"left join oms_package_item opi "
		"on (rt.fk_sales_order_item = opi.fk_sales_order_item) "
		"left join oms_return_ticket_status_history rtsh "
		"on(rt.id_return_ticket = rtsh.fk_return_ticket) "
		"left join oms_package_status_history oph "
		"ON(opi.fk_package=oph.fk_package) "
		"WHERE  rtsh.created_at >= %(d1)s  "
		"and rtsh.created_at < %(d2)s "  
		"and rtsh.fk_return_ticket_status=1 "
		"and oph.fk_package_status = 4 "
		"and opi.fk_package is not null "
		"GROUP BY  rt.fk_sales_order_item, rt.id_return_ticket  " 
	") rt "
	"on rt.fk_sales_order_item = isoi.id_sales_order_item "
	"where 1=1 "
	"AND iso.created_at >= date_sub( %(d1)s , INTERVAL 90 day) "
	"AND iso.created_at < %(d2)s  "
	"AND isoi.is_marketplace <> 1 "
	"AND MOD(id_return_ticket,   %(div)s ) = %(remainder)s " 
	"AND iso.order_nr like '%HK' " )
	
query['RAp'] =	( "select distinct concat('RA_ZSG_',op.package_number,'_',isoi.id_sales_order_item) as RA_Id, "
	"op.status_changed_at, op.status_changed_at as sca "
	"from ims_sales_order_item isoi "
	"inner join ims_sales_order iso "
	"on iso.id_sales_order = isoi.fk_sales_order "
	"inner join "
	"(  SELECT opi.fk_sales_order_item, opi.fk_package, op.package_number, oph.created_at as  status_changed_at, oph.fk_package_status " 
		"FROM oms_package_item opi "
		"inner join oms_package op "
		"ON(opi.fk_package=op.id_package) "
		"inner join oms_package_status_history oph "
		"ON(opi.fk_package=oph.fk_package) "
		"WHERE oph.created_at >= %(d1)s  "
		"AND oph.created_at < %(d2)s  "
		"AND oph.fk_package_status = 5 "
	")op "
	"ON(op.fk_sales_order_item = isoi.id_sales_order_item) "
	"inner join "
	"( SELECT distinct fk_package, fk_package_status, created_at FROM oms_package_status_history opsh1 "
		"where 1=1 "
		"AND opsh1.created_at >= date_sub( %(d1)s , INTERVAL 90 day) "
		"AND opsh1.created_at < %(d2)s  "
		"and fk_package_status = 4 "
	") o_ps "
	"ON(op.fk_package = o_ps.fk_package) "
	"and  o_ps.created_at < op.status_changed_at "
	"where 1=1 "
	"AND iso.created_at >=  date_sub( %(d1)s , INTERVAL 90 day) "
	"AND iso.created_at <  %(d2)s  "
	"AND isoi.is_marketplace <> 1 "
	"AND iso.payment_method not in ('CashOnDelivery','PaidRemote_CashOnDelivery','SevenEleven','COD_NS45Voucher') "
	"AND MOD(isoi.id_sales_order_item,  %(div)s  ) = %(remainder)s " 
	"AND iso.order_nr like '%HK' " )
	
query['CMi'] = 	( "select distinct  concat('CM_','ZSG','_',id_return_ticket ), concat('SO_','ZSG','_', iso.order_nr ), isoish.updated_at "
	"from ims_sales_order_item isoi "
	"inner join ims_sales_order iso "
	"on iso.id_sales_order = isoi.fk_sales_order "
	"inner join "
	"(  select * from ims_sales_order_item_status_history isoish1 "
		"where 1=1 "
		"AND isoish1.fk_sales_order_item_status = 55 "
		"AND isoish1.updated_at >= %(d1)s "
		"AND isoish1.updated_at < %(d2)s "
	")isoish "
	"on(isoi.id_sales_order_item=isoish.fk_sales_order_item) "
	"inner join "
	"(   SELECT rt.fk_sales_order_item, rt.id_return_ticket "
		"FROM oms_return_ticket rt "
		"left join oms_return_ticket_status_history rtsh "
		"on(rt.id_return_ticket = rtsh.fk_return_ticket) "
		"WHERE  rtsh.created_at >= date_sub( %(d1)s , INTERVAL 90 day) "
		"and rtsh.created_at < %(d2)s "
		"group by rt.fk_sales_order_item, rt.id_return_ticket "
	")rt "
	"on rt.fk_sales_order_item = isoish.fk_sales_order_item "
	"where 1=1 "
	"AND iso.created_at >= date_sub( %(d1)s , INTERVAL 90 day) "
	"AND iso.created_at <  %(d2)s "
	"AND isoi.is_marketplace <> 1 "
	"AND MOD(id_return_ticket,   %(div)s  ) = %(remainder)s " 
	"AND iso.order_nr like '%HK' " )
	
query['CMp'] =	( "select distinct concat('CM_','ZSG','_', op.package_number, '_',isoi.id_sales_order_item ), "
	"concat('SO_','ZSG','_', iso.order_nr ), isoish.updated_at, op.status_changed_at "
	"from ims_sales_order_item isoi "
	"inner join ims_sales_order iso "
	"on iso.id_sales_order = isoi.fk_sales_order "
	"inner join "
	"(  SELECT opi.fk_sales_order_item, opi.fk_package, op.package_number, oph.created_at as  status_changed_at, oph.fk_package_status " 
		"FROM oms_package_item opi "
		"inner join oms_package op "
		"ON(opi.fk_package=op.id_package) "
		"inner join oms_package_status_history oph "
		"ON(opi.fk_package=oph.fk_package) "
		"WHERE oph.created_at >= date_sub( %(d1)s , INTERVAL 30 day) "
		"AND oph.created_at < %(d2)s  "
		"AND oph.fk_package_status = 5 "
	")op "
	"ON(op.fk_sales_order_item = isoi.id_sales_order_item) "
	"inner join "
	"( SELECT distinct fk_package, fk_package_status, created_at FROM oms_package_status_history opsh1 "
		"where 1=1 "
		"and opsh1.created_at >= date_sub( %(d1)s , INTERVAL 90 day) "
		"AND opsh1.created_at < %(d2)s  "
		"and fk_package_status = 4 "
	") o_ps "
	"ON(op.fk_package = o_ps.fk_package) "
	"and  o_ps.created_at < op.status_changed_at "
	"inner join "
	"(   select * from ims_sales_order_item_status_history isoish1 "
		"where 1=1 "
		"AND isoish1.fk_sales_order_item_status = 55 "
		"AND isoish1.updated_at >= %(d1)s "
		"AND isoish1.updated_at < %(d2)s  "
	")isoish "
	"ON(isoi.id_sales_order_item=isoish.fk_sales_order_item) "
	"and  o_ps.created_at < op.status_changed_at "
	"where 1=1 "
	"AND iso.created_at >=  date_sub( %(d1)s , INTERVAL 90 day) "
	"AND iso.created_at <  %(d2)s  "
	"AND isoi.is_marketplace <> 1 "
	"AND iso.payment_method not in ('CashOnDelivery','PaidRemote_CashOnDelivery','SevenEleven','COD_NS45Voucher') "
	"AND MOD(id_sales_order_item,   %(div)s  ) = %(remainder)s " 
	"AND iso.order_nr like '%HK' " )
	

fileOMS = {}
fileOMS['PO'] = 'omsPO.csv'
fileOMS['POi'] = 'omsPO.csv'
fileOMS['SO'] = 'omsSO.csv'
fileOMS['CD'] = 'omsCD.csv'
fileOMS['IV'] = 'omsIV.csv'
fileOMS['CP'] = 'omsCP.csv'
fileOMS['RAi'] = 'omsRAi.csv'
fileOMS['RAp'] = 'omsRAp.csv'
fileOMS['CMi'] = 'omsCMi.csv'
fileOMS['CMp'] = 'omsCMp.csv'

Divider = {}
Divider['PO'] = 2
Divider['SO'] = 10
Divider['CD'] = 10
Divider['IV'] = 10
Divider['CP'] = 10
Divider['RAi'] = 5
Divider['RAp'] = 1
Divider['CMi'] = 5
Divider['CMp'] = 1
	

fileNS = {}
fileNS['PO'] = 'nsPO.csv'
fileNS['SO'] = 'nsSO.csv'
fileNS['CD'] = 'nsCD.csv'
fileNS['IV'] = 'nsIV.csv'
fileNS['CP'] = 'nsCP.csv'
fileNS['RAi'] = 'nsRAi.csv'
fileNS['RAp'] = 'nsRAp.csv'
fileNS['CMi'] = 'nsCMi.csv'
fileNS['CMp'] = 'nsCMp.csv'


fileDiff = {}
fileDiff['PO'] = 'diffPO.csv'
fileDiff['SO'] = 'diffSO.csv'
fileDiff['CD'] = 'diffCD.csv'
fileDiff['IV'] = 'diffIV.csv'
fileDiff['CP'] = 'diffCP.csv'
fileDiff['RAi'] = 'diffRAi.csv'
fileDiff['RAp'] = 'diffRAp.csv'
fileDiff['CMi'] = 'diffCMi.csv'
fileDiff['CMp'] = 'diffCMp.csv'

typeSearchValue = {}
typeSearchValue['PO'] = '_purchaseOrder'
typeSearchValue['SO'] = '_salesOrder'
typeSearchValue['CD'] = '_customerDeposit'
typeSearchValue['IV'] = '_invoice'
typeSearchValue['CP'] = '_customerPayment'
typeSearchValue['RAi'] = '_returnAuthorization'
typeSearchValue['RAp'] = '_returnAuthorization'
typeSearchValue['CMi'] = '_creditMemo'
typeSearchValue['CMp'] = '_creditMemo'



stringToSearch = {}
stringToSearch['PO'] = '_externalId = "PO_Z'
stringToSearch['SO'] = '_externalId = "SO_Z'
stringToSearch['CD'] = '_externalId = "CD_Z'
stringToSearch['IV'] = '_externalId = "IV_Z'
stringToSearch['CP'] = '_externalId = "CP_Z'
stringToSearch['RAi'] = '_externalId = "RA_Z'
stringToSearch['RAp'] = '_externalId = "RA_Z'
stringToSearch['CMi'] = '_externalId = "CM_Z'
stringToSearch['CMp'] = '_externalId = "CM_Z'
 
def ceildiv(a, b):
    return -(-a // b)
	
def sleepWait():
	time.sleep(.3)
	
def sleepWait1(sec):
	time.sleep(sec)
	
def workerNS(input, output):
	for func, args in iter(input.get, 'STOP'):
		startFunction(func, args)
		output.put("Done")
	logoutNS(True)

def startFunction(func, args):
	#print args
	func(*args)

def initializeNS(Dummy):

	#print 'another process started'
	global sessionNS
	global pPrintLock
	writeSemaphore.acquire()
	print 'Initialise NetSuite connection in ', current_process().name 
	writeSemaphore.release()
	sessionNS = NetSuiteOperation()
	
	loginNS()
	
	global LORFs
	for i in range(NumberOfRecords):
		if i % 50 == 0 :
			logoutNS()
			loginNS()	
		for tryLoop in range(20):
			try:	
				LORFs[i] = sessionNS.client.factory.create( 'ns4:ListOrRecordRef' )
			except suds.WebFault, webErr:
				print webErr
				continue
			break
		else:
			print 'ListOrRecordRef creation failed 20 retries!!!!!'

	global FO 
	FO = FileOperation()		
	
	global singleTSB
	for tryLoop in range(20):
		try:
			logoutNS()
			loginNS()		
			singleTSB =  sessionNS.client.factory.create( 'ns2:TransactionSearchBasic' )
		except suds.WebFault, webErr:
			print webErr
			continue
		break
	else:
		print 'TransactionSearchBasic creation failed 20 retries!!!!!'
	
	singleTSB.type._operator = 'anyOf'
	singleTSB.externalId._operator = 'anyOf'

def logoutNS(statusNS):
	global sessionNS
	if statusNS :	
		
		for tryLoop in range(5):
			try:
				sessionNS.logout()
			except suds.WebFault, webErr:
				print webErr
				continue
				if 'SESSION_TIME_OUT' in webErr :
					break
				else:
					continue
			break
		else:
			print 'LOGOUT failed 5 retries!!!!!'

def loginNS( ):
	global sessionNS
	time.sleep(1)
	if current_process().name == 'MainProcess' or current_process().name == 'Process-1':
	
		for tryLoop in range(10):
			try:
				sessionNS.loginWithID('philip.sai@zalora.com', 'Phy0ky6w1309&')
			except suds.WebFault, webErr:
				print webErr
				time.sleep(5)
				continue
			break
		else:
			print 'LOGIN failed 5 retries!!!!!'
		
	else:
		for tryLoop in range(10):
			try:
				sessionNS.loginWithID('duhita.sk@zalora.com', 'Netsuite+1')
			except suds.WebFault, webErr:
				print webErr
				time.sleep(5)
				continue
			break
		else:
			print 'LOGIN failed 5 retries!!!!!'	
		
		
def searchRecordsOnNS( item, chunk):

	global sessionNS, singleTSB, LORFs
	print current_process().name ,  'started at ', datetime.now().isoformat(' ')
	j = 0
	for ex_id in chunk:
		LORFs[j]._externalId = ex_id[0]
		j = j + 1
					
	singleTSB.type.searchValue = typeSearchValue[item]
	singleTSB.externalId.searchValue = LORFs
	for tryLoop in range(5):
		try:
			logoutNS()
			loginNS()		
			resultWS = sessionNS.searchRecords(singleTSB)
		except suds.WebFault, webErr:
			print webErr
			continue
		break
	else:
		print 'SEARCH failed 5 retries!!!!!'
		
	if resultWS.status._isSuccess is True:
		i = 0
		#writeSemaphore.acquire()
		f = open(current_process().name + "x" + fileNS[item],"ab")
		c = csv.writer(f)			
		while i <  resultWS.totalRecords :
				row = resultWS.recordList.record[i]
				row1 = [ row._externalId, row.tranDate]
				c.writerow(row1)
				i = i + 1
				time.sleep(0.01)
		f.close
	else:
		print 'FAILED RESPONSE', resultWS
		#writeSemaphore.release()


def omsTask(item):	

	if os.path.isfile(fileOMS[item]) :
		os.remove(fileOMS[item])
	date_start = date_1 
	
	print fileOMS[item]
	
	for i in range(0,no_days.days) :
		#print i
		date_end = date_start + timedelta(days=1)
		print 'oms', date_start, date_end, no_days.days
		OMS.getConnected()
		countRec = OMS.getCount(query[item], date_start.isoformat(' '), date_end.isoformat(' '))
		if countRec <> 0 :
			dividerForDay = ceildiv(int(countRec), 150)
			OMS.getRecords(query[item], date_start.isoformat(' '), date_end.isoformat(' '), dividerForDay, fileOMS[item])
		OMS.getDisonnected()
		date_start = date_end
		
def	getDifference(item, delayMin) :
	global FO
	FO = FileOperation()
	print current_process().name ,  'waited for GetDiff at ', datetime.now().isoformat(' ')
	time.sleep(delayMin*60)
	print current_process().name ,  'started for GetDiff at ', datetime.now().isoformat(' ')
	FO.GetDifference(fileOMS[item], fileNS[item],fileDiff[item])
	print current_process().name ,  'ended for GetDiff at ', datetime.now().isoformat(' ')
	
def	getDifferenceInP(item, delayMin) :

	Process(target=getDifference, args=(item, delayMin)).start()
	
	#pgetD.join()


if __name__ == '__main__':	



	global waitMain, listTotal
	
	if runOMS:
		global OMS 
		OMS = DatabaseOperation()
		
	if runDiff :
		global FO
		FO = FileOperation()
		
	if runNS :
	
		task_queue = Queue()
		done_queue = Queue()
		
		
		p = [ None for x in range(NoOfProcess) ]
		for i in range(NoOfProcess):
			task_queue.put((initializeNS, (2,)))
			p[i] = Process(target=workerNS, args=(task_queue, done_queue))	
			p[i].start()
			time.sleep(1)
			
		
	loopChoices = [ "SO", "CD", "IV", "CP" , "RAi", "RAp", "CMi", "CMp" ]
	#loopChoices = [  "IV", "CP" ]
	dValue = [ None for i in range(len(loopChoices)) ]
	listTotal = dict( zip(loopChoices, dValue) )
	for item in loopChoices:
		print("\nContinue with NS - %s :" %(item) )
		keyIn = 'y' #readchar.readchar()
		if keyIn <> u'\x1b':
			
			if runOMS:
				omsTask(item)
			
			if runNS :
				print 'NS preparation started at ', datetime.now().isoformat(' ')		
				file1 = open(fileOMS[item], 'rb')
				reader = csv.reader(file1)
				rows_list = []
				for row in reader:
					  rows_list.append(row)
				file1.close()   # <---IMPORTANT
				
				listTotal[item] = list(chunks(rows_list, NumberOfRecords))
				
				#if os.path.isfile(fileNS[item]) :
				for filename in glob('*' + fileNS[item]):
					os.remove(filename)

				for chunk in listTotal[item]:
					task_queue.put((searchRecordsOnNS, (item, chunk)))
				print 'NS preparation ended at ', datetime.now().isoformat(' ')	
				
				if runDiff and runNS :
					task_queue.put((getDifferenceInP, (item, 5)))
			
			if runDiff and not(runNS) :
				getDifferenceInP(item, 0)
					
	if runNS :		
		for i in range(NoOfProcess):		
			#task_queue.put((logoutNS, (True,)))	
			task_queue.put('STOP')


	#if runNS :	
		#p.join()
		#sessionNS[0].logout()
		#sessionNS[1].logout()

	 