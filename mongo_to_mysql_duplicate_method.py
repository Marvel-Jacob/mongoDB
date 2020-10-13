import os
import pandas as pd
from pymongo import MongoClient
import pymysql
import sys
import warnings
import time
from datetime import datetime

warnings.filterwarnings("ignore")
st = time.time()
con = pymysql.connect(host= 'localhost', user= 'admin1', password= 'Pass@123', db = 'db')
host= 'localhost'
print('Connected to MySQL-DB: {}'.format(host), flush = True)
cursor = con.cursor()



client=MongoClient(port=27017)
db=client.digitory
recs = db.metrics.find({'tenantId':'100'})

insert_count = 0
mysql_data_count = 0
update_count = 0

for rec in recs:
	Date = rec['date']
	#date1 = datetime.strptime(Date, '%Y-%m-%d %HH:%MM:%SS')
	#Date_string = str(datetime.strftime(Date, '%d-%b'))
	Date_string = str(rec['date']).split('-')[2].split(' ')[0]
	Session = rec['session'].capitalize()
	if 'edited' in rec:
		Edited = str(rec['edited']).capitalize()
	else:
		Edited = 'False'
	if (Edited == 'True'):
		entryCheck = 'Entry'
	else:
		entryCheck = 'No Entry'
	for item,measures in rec['items'].items():
		Items = item
		if (('actual' in measures) and ('predicted' in measures) and ('rmse' in measures)):
			actual_value = measures['actual']
			predicted_value = measures['predicted']
			rmse_value = measures['rmse']
			if('estimated' in measures):
				estimated_value = measures['estimated']
			else:
				estimated_value = measures['predicted']
			appendstate = "INSERT INTO metrics VALUES ({},'{}','{}','{}','{}',{},{},{},{},'{}','{}', '{}') ON DUPLICATE KEY UPDATE actual = {},predicted = {},rmse = {},edited = '{}', entry_check = '{}'".format(tenantId,restaurantId,Date,Session,Items,actual_value,predicted_value,estimated_value,rmse_value,Edited, Date_string, entryCheck,actual_value,predicted_value,rmse_value,Edited,entryCheck)
			#print(appendstate)
			cursor.execute(appendstate)
			con.commit()


		
con.close()
print("Data is updated to My-SQL Database.........",flush = True)
print('Time taken to update SQL Database: %0.2f seconds'%(time.time() - st),flush = True)

