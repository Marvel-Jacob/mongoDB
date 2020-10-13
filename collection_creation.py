import pandas as pd
import numpy as np
from pymongo import MongoClient

client = MongoClient(port = 27017)

inventorymaster = MongoClient('localhost', 27017)['DB']['inventorymasters']
inventory = MongoClient('localhost', 27017)['DB']['inventories']
inventory_dup = MongoClient('localhost', 27017)['DB']['inventories_dup']
inc = 0
for inm_rec in inventorymaster.find({'tenantId' : '100002'}):
	inc = inc + 1
	data_dict = {}
	if 'digiId' not in inm_rec:
		digi_id = 'DI000'+str(int(7+inc))
	itn = inm_rec['itemName']
	itc = inm_rec['itemCode']
	if 'rate' not in inm_rec:
		if 'price' not in inm_rec:
			price = 0	
	else:
		price = inm_rec['rate']
	if 'finalRate' not in inm_rec: 
		FR = 0
	else: 
		FR = inm_rec['finalRate']
	if 'inStock' not in inm_rec:
		iS = 0
	else:
		iS = inm_rec['inStock']
	if 'inKitchen' not in inm_rec:
		iK = 0
	else:
		iK = inm_rec['inKitchen']
	y = inm_rec['yield']
	if 'optimumStock' not in inm_rec:
		oS = 0
	else:
		oS = inm_rec['optimumStock']
	uom = inm_rec['uom']
	status = {}
	statusHistory = []
	modTs = inm_rec['modTs']
	if 'modUser' in inm_rec:
		modUr = inm_rec['modUser']
	else:
		modUr = None
	if 'leadTime' not in inm_rec:
		lt = None
	else:
		lt = inm_rec['leadTime']
	data_dict = {'digiId':digi_id,'itemName':itn,'itemCode':itc,'price':price,
	'finalRate':FR,'inStock':iS,'inKitchen':iK,'yield':y,'optimumStock':oS,'uom':uom,
	'status':status,'statusHistory':statusHistory,'modTs':modTs,'modUser':modUr,'leadTime':lt}
	inventory.update({'_id':inm_rec['_id']}, {'$set':data_dict}, upsert = True, multi = False)

