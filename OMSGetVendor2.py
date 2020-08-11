from DatabaseOperation import *
from datetime import timedelta, datetime
OMS = OMS = DatabaseOperation()

date_1 = datetime(2015,4,01)
date_2 = datetime(2015,4,30)
no_days = date_2 - date_1

query = ( " SELECT "
	"addressee, "
	"if ( length(ifnull(contact_phone,'-')) < 8 , rpad(replace(ifnull(contact_phone,'-'),' ','-'),8,'-'), if(length(contact_phone) > 21 , substring(contact_phone,1,21), contact_phone ))  as contact_phone, "
	"address1, "
	"address2, "
    "city, "
    "postcode, "
    "ctry_name "
"from "
"( "
"SELECT distinct "
	"contact_name as addressee, "
	"replace(CONVERT(contact_phone USING ASCII), '?', '-' )  as contact_phone, "
	"address1, "
	"address2, "
    "city, "
    "postcode, "
    	"case when c.`name` = 'United States' then 'US'  "
             "when c.`name` = 'Canada' then 'CA'  "
	    "else c.`name` "
        "end  as ctry_name "
"FROM supplier_address sa "
"inner join "
"(select fk_supplier, max(updated_at) as updated_at  "
"FROM supplier_address " 
"where fk_supplier in "
"(select id_supplier from %(db)s.ims_supplier s " 
"where LEFT(trim(s.`name`), 82)like \'%Mellooi Creation Sdn Bhd%\') "
"group by fk_supplier) l_add "
"on l_add.fk_supplier = sa.fk_supplier "
"and l_add.updated_at = sa.updated_at "
"left join "
"country c "
"on c.id_country = sa.fk_country "
"where l_add.fk_supplier in "
"(select id_supplier from %(db)s.ims_supplier s "
"where LEFT(trim(s.`name`), 82) like \'%Mellooi Creation Sdn Bhd%\') "
")BB " )

query1 = ( "select * from oms_live_ph.ims_supplier s "
"where LEFT(trim(s.`name`), 82) like \'%Mellooi Creation Sdn Bhd%\' #%(db)s" )

#ListODb = [ 'oms_live_hk', 'oms_live_id', 'oms_live_my', 'oms_live_ph', 'oms_live_sg', 'oms_live_th', 'oms_live_tw', 'oms_live_vn' ]
#for db in  ListODb:
OMS.getConnectedDB('oms_live_sg')
Rec = OMS.executeQuery(query1)
print Rec
OMS.getDisonnected()
