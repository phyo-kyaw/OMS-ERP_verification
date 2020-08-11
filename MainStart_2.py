from __future__ import generators
import os
import csv
from DatabaseOperation import *
from datetime import timedelta, datetime
from NetSuiteOperation import *
from FileOperation import *
import readchar



date_1 = datetime(2015,4,01)
date_2 = datetime(2015,4,28)
no_days = date_2 - date_1
divider =  30
date_SO = date_1 - timedelta(days=30)
date_SO_created = date_1 - timedelta(days=90)
date_CP = date_1 - timedelta(days=90)
date_3 = date_1 - timedelta(days=30)


			
OMS = DatabaseOperation()
#OMS.getConnected()

query = {}

query['POi'] = ( "select distinct (concat('PO_ZSG_',po.po_number)) OMS_PO_Nbr, po.created_at, po.created_at "
	"from ims_purchase_order po "
	"where po.created_at >= %s " 
	"and po.created_at <  %s " 
	"and po.po_number not like 'MP%' "
	"and MOD(id_purchase_order,  %s ) = %s " )

query['PO'] = ( "select distinct (concat('PO_ZSG_',po.po_number)) OMS_PO_Nbr, po.created_at, po.created_at "
	"from ims_purchase_order po "
	"where po.created_at >= %(d1)s " 
	"and po.created_at <  %(d2)s " 
	"and po.po_number not like 'MP%' "
	"and MOD(id_purchase_order,  %(div)s ) = %(remainder)s " )
	
query['PO1'] = ( "select distinct (concat('PO_ZSG_',po.po_number)) OMS_PO_Nbr, po.created_at, po.created_at "
	"from ims_purchase_order po "
	"where po.created_at >= '" + date_1.isoformat(' ') + 
	"' and po.created_at < '" + date_2.isoformat(' ') + 
	"' and po.po_number not like 'MP%' "
	"and MOD(id_purchase_order, " + str(divider) + ") =  " )

query['SO1'] =  ( "select distinct (concat('SO_ZSG_',so.order_nr)), so.imported_at, so.created_at "
	"from ims_sales_order so "
	"inner join "
	"ims_sales_order_item soi "
	"on so.id_sales_order = soi.fk_sales_order "
	"where so.imported_at >= '" + date_1.isoformat(' ') + 
	"' and so.imported_at < '" + date_2.isoformat(' ') + 
	"' and soi.is_marketplace <> 1 " 
	"and MOD(id_sales_order, " + str(divider) + ") =  " )
	
query['SO2'] =  ( "select distinct (concat('SO_ZSG_',so.order_nr)), so.imported_at, so.created_at "
	"from ims_sales_order so "
	"inner join "
	"ims_sales_order_item soi "
	"on so.id_sales_order = soi.fk_sales_order "
	"where so.imported_at >=  %(date_1)s " 
	" and so.imported_at <  %(date_2)s "
	" and soi.is_marketplace <> 1 " 
	"and MOD(id_sales_order,  %(divider)s ) =  " )
	
query['SOi'] =  ( "select distinct (concat('SO_ZSG_',so.order_nr)), so.imported_at, so.created_at "
	"from ims_sales_order so "
	"inner join "
	"ims_sales_order_item soi "
	"on so.id_sales_order = soi.fk_sales_order "
	"where so.imported_at >=  %s " 
	"and so.imported_at <  %s "
	"and so.created_at >  '" + date_SO.isoformat(' ') + "' "
	"and so.created_at <= '" + date_2.isoformat(' ') + "' "
	"and soi.is_marketplace <> 1 " 
	"and MOD(id_sales_order,  %s ) = %s " )

query['SO'] =  ( "select distinct (concat('SO_ZSG_',so.order_nr)), so.imported_at, so.created_at "
	"from ims_sales_order so "
	"inner join "
	"ims_sales_order_item soi "
	"on so.id_sales_order = soi.fk_sales_order "
	"where so.imported_at >=  %(d1)s " 
	"and so.imported_at <  %(d2)s "
	"and so.created_at >= date_sub( %(d1)s, INTERVAL 30 day) " 
	"and so.created_at <  %(d2)s "
	"and soi.is_marketplace <> 1 " 
	"and MOD(id_sales_order,  %(div)s ) = %(remainder)s " 
	"AND so.order_nr not like '%HK' " )
	
query['CD'] = ( " select distinct (concat('CD_ZSG_',so.order_nr)) OMS_Order_Nbr, so.imported_at, so.created_at "
	"from oms_live_sg.ims_sales_order so "
	"inner join oms_live_sg.ims_sales_order_item soi "
	"on so.id_sales_order = soi.fk_sales_order "
	"inner join oms_live_sg.ims_sales_order_item_status_history soih "
	"on soi.id_sales_order_item = soih.fk_sales_order_item "
	"where soih.fk_sales_order_item_status = 67 "
	"and soih.updated_at >= %(d1)s " 
	"and soih.updated_at < %(d2)s " 
	"and so.created_at >=  date_sub( %(d1)s, INTERVAL 60 day) " 
	"and so.created_at <  %(d2)s " 
	"and so.payment_method not in ('CashOnDelivery','PaidRemote_CashOnDelivery','SevenEleven','COD_NS45Voucher') "
	"and soi.is_marketplace <> 1 "
	"and so.grand_total <> 0 "
	"and MOD(id_sales_order, %(div)s ) = %(remainder)s "
	"AND so.order_nr not like '%HK' " )	
	

	
query['CDi'] = ( " select distinct (concat('CD_ZSG_',so.order_nr)) OMS_Order_Nbr, so.imported_at, so.created_at "
	"from oms_live_sg.ims_sales_order so "
	"inner join oms_live_sg.ims_sales_order_item soi "
	"on so.id_sales_order = soi.fk_sales_order "
	"inner join oms_live_sg.ims_sales_order_item_status_history soih "
	"on soi.id_sales_order_item = soih.fk_sales_order_item "
	"where soih.fk_sales_order_item_status = 67 "
	"and soih.updated_at >= %s " 
	"and soih.updated_at < %s " 
	"and so.created_at >= '" + date_SO_created.isoformat(' ') + "' "
	"and so.created_at <= '" + date_2.isoformat(' ') + "' "
	"and so.payment_method not in ('CashOnDelivery','PaidRemote_CashOnDelivery','SevenEleven','COD_NS45Voucher') "
	"and soi.is_marketplace <> 1 "
	"and so.grand_total <> 0 "
	"and MOD(id_sales_order,  %s ) = %s " 
	"AND iso.order_nr not like '%HK' " )
	
	
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
	"AND iso.order_nr not like '%HK' " )
	

query['CP'] = ( "select distinct "
	"concat('CP_','ZSG','_',op.package_number) as Payment_id, "
	"TransDate, TransDate "
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
	"AND iso.order_nr not like '%HK' " )
	
query['CPi'] = ( "select distinct "
	"concat('CP_','ZSG','_',op.package_number) as Payment_id, "
	"TransDate, TransDate "
	"from "
	"(  SELECT distinct fk_package,created_at as TransDate "
		"FROM oms_package_status_history "
		"where 1=1 "
		"and fk_package_status = 6 "
		"and created_at >=  %s " 
		"and created_at <  %s " 
	") ops "
	"left join oms_package as op ON(ops.fk_package = op.id_package) "
	"inner join oms_package_item as opi ON (op.id_package=opi.fk_package) "
	"inner join ims_sales_order_item as isoi  on (isoi.id_sales_order_item = opi.fk_sales_order_item) "
	"inner join ims_sales_order iso on (isoi.fk_sales_order=iso.id_sales_order) "
	"and isoi.is_marketplace <> 1 "
	"and iso.payment_method in ('CashOnDelivery','PaidRemote_CashOnDelivery','SevenEleven','COD_NS45Voucher') "
	"and iso.created_at >= '" + date_SO_created.isoformat(' ') + "' "
	"and iso.created_at <= '" + date_2.isoformat(' ') + "' "
	"and MOD(id_package,   %s ) = %s " 
	"AND iso.order_nr not like '%HK' " )

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
	"AND iso.order_nr not like '%HK' " )
	
query['RAp'] =	( "select distinct concat('RA_ZSG_',op.package_number,'_',isoi.id_sales_order_item) as RA_Id, "
	"op.status_changed_at, op.status_changed_at "
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
	"AND iso.order_nr not like '%HK' " )
	
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
	"AND iso.order_nr not like '%HK' " )
	
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
	"AND iso.order_nr not like '%HK' " )
	
#	"AND so.order_nr not like '%HK' "
print 'Getting results'

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
Divider['SO'] = 20
Divider['CD'] = 20
Divider['IV'] = 30
Divider['CP'] = 20
Divider['RAi'] = 10
Divider['RAp'] = 1
Divider['CMi'] = 10
Divider['CMp'] = 1

loopChoices = [ "PO", "SO", "CD", "IV", "CP" , "RAi", "RAp", "CMi", "CMp" ]
#loopChoices = [ "RAi", "CMp" ]
for item in loopChoices:
	print("\nContinue with OMS - %s :" %(item) )
	keyIn = 'y' #readchar.readchar()
	if keyIn <> u'\x1b':
		#print item, '\n', fileOMS[item], '\n',  query[item], 
		print item
		
		if os.path.isfile(fileOMS[item]) :
			os.remove(fileOMS[item])
		date_start = date_1 
		
		for i in range(0,no_days.days) :
			print i
			date_end = date_start + timedelta(days=1)
			print date_start, date_end, no_days.days
			OMS.getConnected()
			OMS.GetRecords(query[item], date_start.isoformat(' '), date_end.isoformat(' '), Divider[item], fileOMS[item])
			OMS.getDisonnected()
			date_start = date_end
		'''
		######
		if os.path.isfile(fileOMS[item]) :
			os.remove(fileOMS[item])
		for sub_no in range(0, divider):
			print str(sub_no)
			Request =   query[item] + str(sub_no) 
			print Request
			OMS.getConnected()
			OMS.getAndWriteRecords(Request, fileOMS[item])
			OMS.getDisonnected()
		'''
	#OMS.getDisonnected()
		

print 'Initialise NetSuite connection!'
NS = NetSuiteOperation()
#NS.logout()
NS.login()
#NS.GetRecord("test_1.csv", 'SO')

TSB = NS.client.factory.create( 'ns2:TransactionSearchBasic' )
TSB.type._operator = 'anyOf'
TSB.tranDate.searchValue2 = date_3
TSB.tranDate._operator = 'within'

singleTSB =  NS.client.factory.create( 'ns2:TransactionSearchBasic' )
singleTSB.type._operator = 'anyOf'

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

FO = FileOperation()

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

for item in loopChoices:
	print("\nContinue with NS - %s :" %(item) )
	keyIn = 'y' #readchar.readchar()
	if keyIn <> u'\x1b':
		print item, '\n', fileNS[item], '\n',  typeSearchValue[item] 
		
		
		TSB.type.searchValue = typeSearchValue[item]
		
		'''
		if  item == 'PO' or item == 'SO' or item == 'IV' :
			TSB.tranDate.searchValue = date_early
			print fileNS[item], typeSearchValue[item], stringToSearch[item]
			NS.getRecordsFile(fileNS[item], TSB, stringToSearch[item])
			FO.GetDifference(fileOMS[item], fileNS[item],fileDiff[item])
			
			singleTSB.type.searchValue = typeSearchValue[item]
			singleTSB.tranId._operator = 'is'
			print singleTSB
			NS.GetRecord(singleTSB, fileDiff[item])
			
		else:
			#TSB.tranDate.searchValue = date_early
			#NS.getRecordsFile(fileNS[item], TSB, stringToSearch[item])
			#FO.GetDifference(fileOMS[item], fileNS[item],fileDiff[item])
		'''	
		######### 
		singleTSB.externalId._operator = 'anyOf'
		singleTSB.type._operator = 'anyOf'
		singleTSB.type.searchValue = typeSearchValue[item]
		NS.searchRecords(fileOMS[item], singleTSB, fileNS[item]) 
		FO.GetDifference(fileOMS[item], fileNS[item],fileDiff[item])
		

NS.logout()
 