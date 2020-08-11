from __future__ import generators
import csv
from DatabaseOperation import *


def ResultIter(cur, arraysize=1000):
	'An iterator that uses fetchmany to keep memory usage down'
	while True:
		results = cur.fetchmany(arraysize)
		if not results:
			break
		for result in results:
			yield result
				
OMS = DatabaseOperation()
query = "select concat('SO_ZSG_', order_nr), created_at, MOD(bob_id_customer, 10)  as row_no from ims_sales_order where created_at > '2015-04-08 00:00:00' and  where created_at > '2015-04-09 00:00:00' and row_no = %d "
print 'Getting results'
c = csv.writer(open("test.csv","wb"))
cursor = OMS.GetPurchaseOrders(query)
print 'Writting results'
for result in ResultIter(cursor):

	c.writerow(result)

	 