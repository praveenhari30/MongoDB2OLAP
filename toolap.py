# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 17:31:18 2018

@author: Praveen
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os as os
from sqlalchemy import create_engine

mc = MongoClient('localhost', 27017)

start = datetime(2014, 2, 2)
end = datetime(2014, 2, 24)

sales = mc.get_database('BIProject').get_collection('SalesTrx').find({'TransDate':{'$lt': end, '$gte': start}},{'_id':0})
itemattr = mc.get_database('BIProject').get_collection('ItemAttribute').find({},{'_id':0})
customer = mc.get_database('BIProject').get_collection('Customer').find({},{'_id':0})
item = mc.get_database('BIProject').get_collection('ItemList').find({},{'_id':0})

#convert it to a dataframe
salesdf= pd.DataFrame(list(sales))
print("sales : " + str(len(salesdf)))
salesdf.head(n=2)

itemattrdf= pd.DataFrame(list(itemattr))
print("itemattr : " + str(len(itemattrdf)))
itemattrdf.head(n=2)

customerdf= pd.DataFrame(list(customer))
print("customer : "+str(len(customerdf)))
customerdf.head(n=2)

itemdf= pd.DataFrame(list(item))
print("item : "+ str(len(itemdf)))
itemdf.head(n=2)

customersinsales = pd.merge(customerdf, salesdf, left_on='LoyaltyCardNum', right_on='LoyaltyCardNumber',how = 'inner')
print("customersinsales : " + str(len(customersinsales)))

itemsinsales = pd.merge(itemdf, salesdf, left_on='UPC', right_on='UPC',how = 'inner')
print("itemsinsales : " + str(len(itemsinsales)))

itemattrinitems = pd.merge(itemattrdf, itemdf, left_on='UPC', right_on='UPC',how = 'inner')
print("itemattrinitems : " + str(len(itemattrinitems)))

engine = create_engine('mysql+mysqlconnector://username:password@localhost/NoSQLtoOLAP')

engine.execute('delete from DateDim')
engine.execute('Delete from TimeDim')
engine.execute('delete from ItemListDim')
engine.execute('Delete from ItemJunkDim')
engine.execute('delete from ItemHierarchyDim')
engine.execute('Delete from StoreJunkDim')
engine.execute('delete from StoreLocationDim')
engine.execute('Delete from SalesJunkDim')
engine.execute('delete from CustomerDim')
engine.execute('Delete from ItemAttributesDim')
engine.execute('delete from trans_fact')
engine.execute('Delete from Holiday')
engine.execute('delete from DateDim_Holiday')

customerdf[['LoyaltyCardNum','MemberFavStore','City','State','ZipCode']].to_sql('CustomerDim', engine, if_exists='append', index=False)

temp = pd.DatetimeIndex(salesdf['TransDate'])
salesdf['year'] = temp.year
salesdf['mn'] = temp.month
salesdf['dy'] = temp.day
print(salesdf)
