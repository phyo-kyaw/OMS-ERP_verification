from DatabaseOperation import *
from datetime import timedelta, datetime
OMS = OMS = DatabaseOperation()

date_1 = datetime(2015,4,01)
date_2 = datetime(2015,4,30)
no_days = date_2 - date_1

query = 'select * from ims_supplier s where LEFT(trim(s.`name`), 82) like \'%Mellooi Creation Sdn Bhd%\' '

ListODb = [ 'oms_live_hk', 'oms_live_id', 'oms_live_my', 'oms_live_ph', 'oms_live_sg', 'oms_live_th', 'oms_live_tw', 'oms_live_vn' ]
for db in  ListODb:
	OMS.getConnectedDB(db)
	countRec = OMS.getCountQ(query)
	print db, countRec
	OMS.getDisonnected()
