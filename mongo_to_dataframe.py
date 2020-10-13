from pymongo import MongoClient
import pandas as pd

client=MongoClient(port=27017)
db=client.digitory
recs = db.metrics.find()

metrics=pd.DataFrame(columns=['Date','Session'])

for rec in recs:
	date = rec['date']
	session = rec['session'] 
	met={'Tenant_ID':tenantId,'Restaurant_ID':restaurantId,'Date':date,'Session':session}
	metrics=metrics.append(met,ignore_index=True)
metrics.to_csv('Metrics.csv',index=False)
