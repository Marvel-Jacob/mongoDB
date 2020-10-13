from pymongo import MongoClient
import sys
from datetime import datetime
import numpy as np
from pprint import pprint
import pandas as pd
dformat = ('%Y-%m-%d')

try:
	# lower_date = datetime(int(sys.argv[1].split('/')[0]),int(sys.argv[1].split('/')[1]),int(sys.argv[1].split('/')[2]))
	lower_date = datetime.strptime(sys.argv[1], dformat)
	# upper_date = datetime(int(sys.argv[2].split('/')[0]),int(sys.argv[2].split('/')[1]),int(sys.argv[2].split('/')[2]))
	upper_date = datetime.strptime(sys.argv[2], dformat)

except:
	print('Please specify date range and give the date format: yyyy/mm/dd')


# collection = input('\nEnter collection name: ')
metrics = MongoClient(port = 27017)['db']['metrics']
recs = metrics.find({'Id':'1000','date':{'$gte':lower_date,'$lte':upper_date}})
print(recs.count())
for rec in recs:
	items = rec['items']
	for itemName, values in items.items():
		# items[itemName]['predicted'] = 0
		items[itemName]['predicted'] = 0.0
	metrics.update({'_id':rec['_id']},{'$set' : {'items':items}}, upsert = False, multi = False)