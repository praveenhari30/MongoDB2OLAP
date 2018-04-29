#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 28 11:37:54 2018

@author: Praveen, Rushan
"""

import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from sqlalchemy import create_engine
#import mysql.connector

def connectToMongo(hostname,port):
    """
    This function estalishes connection to Mongo DB and returns connection object
    """
    mc = MongoClient(host='127.0.0.1',port=27017)
    return mc

def getCollection(database, collection, mongo_conn):
    """
    This function gets data for Store Location
    """
    db = mongo_conn.get_database(database)
    lists = db.get_collection(collection).find({},{'_id':0})
    return lists

def getSalesTrx(sDate,eDate,store,database, collection, mongo_conn):
    """
    This function gets data for Store Location
    """
    db = mongo_conn.get_database(database)
    lists = db.get_collection(collection).find({'StoreNum':store,'TransDatetime(Local)':{'$lt': eDate, '$gte': sDate}},{'_id':0})
    return lists

def convertToDF(lists):
    """
    This function converts lists of data extracted from mongo to Data Frames.
    """
    df= pd.DataFrame(list(lists))
    print("Number of rows : " + str(len(df)))
    return df

def connectMySQL(username, password, hostname, db):
    """
    This function estalishes connection to MySQL and returns connection object
    """
    #mysql+mysqlconnector://[username]:[password]@localhost/[database]
    link = 'mysql+mysqlconnector://'+username+':'+password+'@'+hostname+'/'+db
    engine = create_engine(link)
    return engine

def performHousekeeping(engine):
    """
    This function deletes all data records from the database table
    """
    list1 = ['DateDim','TimeDim','ItemListDim','ItemJunkDim','ItemHierarchyDim','StoreJunkDim',
             'StoreLocationDim','SalesJunkDim','CustomerDim','ItemAttributesDim','trans_fact']
    for i in list1:
        engine.execute('delete from '+i)

if __name__ == '__main__':
    
    conn_obj = connectToMongo(hostname='127.0.0.1',port=27017)
    
    #Get data from Mongo
    storeDF = convertToDF(getCollection('BIProject','StoreLocation',conn_obj))
    print(storeDF.dtypes)
    print(storeDF.isna().sum()) 
    
    itemAttrDF = convertToDF(getCollection('BIProject','ItemAttribute',conn_obj))
    print(itemAttrDF.dtypes)
    print(itemAttrDF.isna().sum()) 
    
    itemListDF = convertToDF(getCollection('BIProject','ItemList',conn_obj))
    print(itemListDF.dtypes)
    print(itemListDF.isna().sum()) 
    
    customerDF = convertToDF(getCollection('BIProject','Customer',conn_obj))
    print(customerDF.dtypes)
    print(customerDF.isna().sum()) 
    
    start = '2014-02-22 00:00:00'
    end = '2014-02-23 00:00:00'
    store = 562
    saleDF = convertToDF(getSalesTrx(start, end,store,'BIProject','SalesTrx',conn_obj))
    print(saleDF.dtypes)
    print(saleDF.isna().sum())
    
    # Create a mysql database connection
    engine = connectMySQL('root', 'password', 'localhost', 'sls_tran_sch1')
    
    #Delete all data from all tables in the database
    performHousekeeping(engine)
    
    """
    Inserting data values into mysql database
    """
    #Merging customerDF with saleDF to get only those customers who are in our sales transaction file
    cust_sale = pd.merge(customerDF, saleDF, left_on='LoyaltyCardNum', right_on='LoyaltyCardNumber',how = 'inner')
    
    #Inserting data into CustomerDim
    CustomerDim = cust_sale[['LoyaltyCardNum','HouseholdNum','MemberFavStore','City','State','ZipCode']].drop_duplicates(keep='first')
    CustomerDim.to_sql('CustomerDim', engine, if_exists='append', index=False)
    
    #Inserting data into StoreJunkDim
    storeDF[['StoreNum','StoreName','ActiveFlag','SqFoot','ClusterName']].to_sql('StoreJunkDim', engine, if_exists='append', index=False)
    
    #Inserting data into StoreLocationDim
    storeDF[['Region','StateCode','City','ZipCode','AddressLine1']].drop_duplicates(keep='first').to_sql('StoreLocationDim', engine, if_exists='append', index=False)

    #Merging itemListDF and saleDF to get only those items which are in our sales
    item_sale = pd.merge(itemListDF, saleDf, left_on=['UPC','ItemID']), right_on=['UPC','ItemID'], how ='inner')

    #Inserting data into ItemListDim
    itemListDF[['UPC','ItemID','LongDes','ShortDes','ExtraDes']].drop_duplicates(keep='first').to_sql('ItemListDim', engine, if_exists='append', index=False)
    
    #Inserting data into 
    itemListDF[['DepartmentCode','FamilyCode','FamilyDes','CategoryCode','CategoryDes','ClassCode','ClassDes']].drop_duplicates(keep='first').to_sql('ItemHierarchyDim', engine, if_exists='append', index=False)
    
    #Inserting data into ItemJunkDim
    itemListDF[['StoreBrand','Status']].drop_duplicates(keep='first').to_sql('ItemJunkDim', engine, if_exists='append', index=False)

