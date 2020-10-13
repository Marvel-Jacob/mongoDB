import pandas as pd
import numpy as np
from pymongo import MongoClient

items = pd.read_csv('/home/u73/workspace/nandha_item_hash.csv')
items = items.drop_duplicates()
item_list ={}
for itemcode, itemname in list(zip(items['itemcode'], items['itemname'])):
	item_list.update({str(itemcode):itemname})
# print(item_list)

client = MongoClient(port = 27017)
db = client.digitory

metrics = db['metrics']

recs = db.metrics.find()

print('********* Updating '+str(len(item_list))+' items in metrics ********')
new_list = {}
metric_list = {}
for rec in recs:
	for itemcode, itemname, measures in zip(item_list.keys(), item_list.values(), rec['items'].values()):
		if 'actual' in measures:
			metric_list.update({'actual':measures['actual']})
			if 'estimated' in measures:
				metric_list.update({'estimated':measures['estimated']})
				if 'predicted' in measures:
					metric_list.update({'predicted':measures['predicted']})
					if 'rmse' in measures:
						metric_list.update({'rmse':measures['rmse']})
		new_list.update({itemcode:metric_list})
	metrics.update(rec, {'$set':{'item':new_list}}, upsert = True) 
print('*************** Updation done **************')


