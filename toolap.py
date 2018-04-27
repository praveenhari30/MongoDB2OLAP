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

mc = MongoClient('localhost', 27017)

#note, everything is case sensitive
#get the mongo object
#recs = mc.get_database('BIProject').get_collection('SalesTrx').find({"StoreNum" : 560})

start = datetime(2014, 2, 2)
end = datetime(2014, 2, 22)

recs = mc.get_database('BIProject').get_collection('SalesTrx').find({'TransDate':{'$lt': end, '$gte': start}})

#db.SalesTrx.find({'TransDate':ISODate("2014-02-25T00:00:00Z")}).pretty()

#convert it to a dataframe
df= pd.DataFrame(list(recs))
df

