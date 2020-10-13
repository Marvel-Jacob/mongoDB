from pymongo import MongoClient
import csv
from pprint import pprint
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

class Record() :
	date = ''
	sbu = ''
	sbu_name = ''
	ABV = ''
	ACS = ''
	ASP = ''
	ce = ''
	ipcm = ''
	sales = ''
	qty = ''
	bills = ''
	session= ''
	item = ''
	actual_value = ''
	predicted_value = ''
	estimated_value = ''
	rmse_value = ''

	def p(self, toprint):
		# print("---")
		if(toprint) :
			print(sbu_measure, "-", sbu_value)
	def no_sbu(self):
		sbu_name = ' '
		ABV = 0
		ACS = 0
		sales = 0
		qty = 0
		ce = 0
		bills = 0
		ipcm = 0
		ASP = 0

client=MongoClient(port=27017)
db=client.digitory

recs = db.metrics.find()

arr_metrics = [[Date','Session','Item','Actual','Predicted','Estimated','RMSE']]
# arr_sbu = [['Tenant_ID','Restaurant_ID','Date','Session','SBU_Name','ABV','ACS','sales','qty','ce','bills','ipcm','ASP']]
r = Record()

for rec in recs:
	#process items

	r.date = rec['date']
	r.session = rec['session']
	for item,measures in rec['items'].items():
	# print (item,measures,"|")
		r.item = item
		# print(item,' - ',measures)
		# print('--------')
		if 'actual' in measures:
			r.actual_value = measures['actual']
			if 'estimated' in measures:
				r.estimated_value = measures['estimated']
				if 'predicted' in measures:
					r.predicted_value = measures['predicted']
					if 'rmse' in measures:
						r.rmse_value = measures['rmse']
		arr_metrics.append([r.date, r.session, r.item, r.actual_value, r.predicted_value, r.estimated_value, r.rmse_value])

	process sbu
	if 'sbu' in rec:
		for sbu_name,sbu_measures in rec['sbu'].items():
			r.sbu_name = sbu_name
			# print(sbu_name)
			if 'ABV' in sbu_measures:
					r.ABV = sbu_measures['ABV']
					r.ACS = sbu_measures['ACS']
					r.sales = sbu_measures['sales']
					r.qty = sbu_measures['qty']
					r.ce = sbu_measures['ce']
					r.bills = sbu_measures['bills']
					r.ipcm = sbu_measures['ipcm']
					r.ASP = sbu_measures['ASP']
					r.p(False)
					arr_sbu.append([r.date, r.session, r.sbu_name, r.ABV, r.ACS, r.sales, r.qty, r.ce, r.bills, r.ipcm, r.ASP])

	else:
		present_date = rec['date']
		before_week = present_date + timedelta(days = -7)
		after_week = present_date + timedelta(days = 7)
		for rec in recs:
			if before_week == rec['date']:
				if 'sbu' in rec:
					before_sbu = rec['sbu']
					# pprint(before_sbu, ' - ')
			if after_week == rec['date']:
				if 'sbu' in rec:
						after_sbu = rec['sbu']
		# print(before_sbu,' /// ',after_sbu)
		for before_sbu_name, before_sbu_measures in before_sbu.items():
			for after_sbu_name, after_sbu_measures in after_sbu.items():
				if before_sbu_name == after_sbu_name:
					r.ABV = float((before_sbu_measures['ABV'] + after_sbu_measures['ABV'])/2)
					r.ACS = float((before_sbu_measures['ACS'] + after_sbu_measures['ACS'])/2)
					r.sales = float((before_sbu_measures['sales'] + after_sbu_measures['sales'])/2)
					r.qty = float((before_sbu_measures['qty'] + after_sbu_measures['qty'])/2)
					r.ce = float((before_sbu_measures['ce'] + after_sbu_measures['ce'])/2)
					r.bills = float((before_sbu_measures['bills'] + after_sbu_measures['bills'])/2)
					r.ipcm = float((before_sbu_measures['ipcm'] + after_sbu_measures['ipcm'])/2)
					r.ASP = float((before_sbu_measures['ASP'] + after_sbu_measures['ASP'])/2)

		arr_sbu.append([r.date, r.session, r.ABV, r.ACS, r.sales, r.qty, r.ce, r.bills, r.ipcm, r.ASP])
	

import pymysql
conn = pymysql.connect(host = 'localhost', user = 'u73', password = 'u73', db = 'digitory')
curr = conn.cursor()
print(curr.execute('select * from metrics'))
curr.close()

with open('new_data_transformed_metrics.csv', 'w') as writeFile:
    writer = csv.writer(writeFile)
    writer.writerows(arr_metrics)

with open('new_data_transformed_sbu.csv','w') as writefile:
	writer = csv.writer(writefile)
	writer.writerows(arr_sbu)
 
    
abv = pd.DataFrame(arr_sbu, columns=arr_sbu[0]).drop(0,axis=0)
c=0
for a in abv['ABV']:
	if a != 0:
		c = c+1
print(abv['ABV'].sum()/c)
