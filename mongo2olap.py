#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 28 11:37:54 2018

@author: Praveen, Rushan, Tahrima
"""

import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine
import calendar
#import mysql.connector
import json
from bson import json_util, ObjectId
from pandas.io.json import json_normalize

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
             'StoreLocationDim','SalesJunkDim','CustomerDim','ItemAttributesDim','StoreServiceDim',
             'trans_fact']
    for i in list1:
        engine.execute('delete from '+i)
        
def getEmbedded(mongo_data):
    """
    This function is to convert nested documents from mongoDB to DataFrames
    """
    sanitized = json.loads(json_util.dumps(mongo_data))
    normalized = json_normalize(sanitized)
    df = pd.DataFrame(normalized)
    return df

if __name__ == '__main__':
    #Creating a mongoDB connection object
    conn_obj = connectToMongo(hostname='127.0.0.1',port=27017)
    
    """
    Get data from Mongo
    """
    #Collect data from StoreLocation collection at mongoDB
    storeDF = convertToDF(getCollection('BIProject','StoreLocation',conn_obj))
    print(storeDF.dtypes)
    print(storeDF.isna().sum()) 
    
    #Collect data from ItemAttribute collection at mongoDB
    itemAttrDF = convertToDF(getCollection('BIProject','ItemAttribute',conn_obj))
    print(itemAttrDF.dtypes)
    print(itemAttrDF.isna().sum()) 
    
    #Collect data from ItemList collection at mongoDB
    itemListDF = convertToDF(getCollection('BIProject','ItemList',conn_obj))
    print(itemListDF.dtypes)
    print(itemListDF.isna().sum()) 
    
    #Collect data from Customer collection at mongoDB
    customerDF = convertToDF(getCollection('BIProject','Customer',conn_obj))
    print(customerDF.dtypes)
    print(customerDF.isna().sum()) 
    
    #Collect data from SalesTrx collection at mongoDB between specific transaction dates at a particular store
    start = '2014-02-22 00:00:00'
    end = '2014-02-23 00:00:00'
    store = 562
    saleDF = convertToDF(getSalesTrx(start, end,store,'BIProject','SalesTrx',conn_obj))
    print(saleDF.dtypes)
    print(saleDF.isna().sum())
    
    #Extracting scraped data at mongoDB
    DF = getEmbedded(getCollection('BIProject','StoreScraped', conn_obj))

    scrapedDF = DF[['Service.Alcohol','Service.Amarillo National Bank','Service.Angus Beef',
                  'Service.Bakery','Service.Bill Pay','Service.Boars Head','Service.Bulk Foods',
                  'Service.Check Cashing','Service.City Bank','Service.Clear Talk','Service.Coffee Shop',
                  'Service.Concierge','Service.DMV Registration','Service.Deli','Service.Dish Gift Center',
                  'Service.First Financial Bank','Service.Floral','Service.Full Service Seafood','Service.Herring National Bank',
                  'Service.Hot Deli','Service.Keva Juice','Service.Living Well Dept','Service.Lottery','Service.Meals For Two','Service.Meat Market',
                  'Service.Red Box','Service.Restaurant','Service.Rug Doctor','Service.Salad Bar','Service.Sushi','Service.Team Spirit Shop','Service.Ticket Sales','Service.Walk-in Clinic',
                  'Service.Wells Fargo Bank','Service.Western Union','StoreId','StoreName']]

    scrapedDF.rename(columns={'Service.Alcohol':'Alcohol','Service.Amarillo National Bank':'AmarilloNationalBank','Service.Angus Beef':'AngusBeef',
                  'Service.Bakery':'Bakery','Service.Bill Pay':'BillPay','Service.Boars Head':'BoarsHead','Service.Bulk Foods':'BulkFoods',
                  'Service.Check Cashing':'CheckCashing','Service.City Bank':'CityBank','Service.Clear Talk':'ClearTalk','Service.Coffee Shop':'CoffeeShop',
                  'Service.Concierge':'Concierge','Service.DMV Registration':'DMVregistration','Service.Deli':'Deli','Service.Dish Gift Center':'DishGiftCenter',
                  'Service.First Financial Bank':'FirstFinancialBank','Service.Floral':'Floral','Service.Full Service Seafood':'FullServiceSeafood','Service.Herring National Bank':'HerringNationalBank',
                  'Service.Hot Deli':'HotDeli','Service.Keva Juice':'KevaJuice','Service.Living Well Dept':'LivingWellDept','Service.Lottery':'Lottery','Service.Meals For Two':'MealsForTwo','Service.Meat Market':'MeatMarket',
                  'Service.Red Box':'RedBox','Service.Restaurant':'Restaurant','Service.Rug Doctor':'RugDoctor','Service.Salad Bar':'SaladBar','Service.Sushi':'Sushi','Service.Team Spirit Shop':'TeamSpiritShop','Service.Ticket Sales':'TicketSales','Service.Walk-in Clinic':'WalkInClinic',
                  'Service.Wells Fargo Bank':'WellsFargoBank','Service.Western Union':'WesternUnion','StoreId':'StoreNum','StoreName':'StoreType'},inplace=True)

    
    """
    Create a mysql database connection
    """
    engine = connectMySQL('oishee', '123456', 'localhost', 'sls_tran_sch1')
    
    #Delete all data from all tables in the database
    performHousekeeping(engine)
    
    """
    Inserting data values into Dimensions of the mysql database
    """
    #Merging customerDF with saleDF to get only those customers who are in our sales transaction file
    cust_sale = pd.merge(customerDF, saleDF, left_on='LoyaltyCardNum', right_on='LoyaltyCardNumber',how = 'inner')
    
    #Inserting data into CustomerDim
    CustomerDim = cust_sale[['LoyaltyCardNum','HouseholdNum','MemberFavStore','City','State','ZipCode']].drop_duplicates(keep='first')
    CustomerDim.to_sql('CustomerDim', engine, if_exists='append', index=False)
    
    #Inserting data into StoreJunkDim
    storeDF[['StoreNum','StoreName','ActiveFlag','SqFoot','ClusterName']].to_sql('StoreJunkDim', engine, if_exists='append', index=False)
    
    #Inserting data into StoreLocationDim
    StoreLocationDim = storeDF[['Region','StateCode','City','ZipCode','AddressLine1']].drop_duplicates(keep='first')
    StoreLocationDim.to_sql('StoreLocationDim', engine, if_exists='append', index=False)

    #Merging itemListDF and saleDF to get only those items which are in our sales
    item_sale = pd.merge(itemListDF, saleDF, left_on=['UPC','ItemID'], right_on=['UPC','ItemID'], how ='inner')

    #Inserting data into ItemListDim
    ItemListDim = item_sale[['UPC','ItemID','LongDes','ShortDes','ExtraDes']].drop_duplicates(keep='first')
    ItemListDim.to_sql('ItemListDim', engine, if_exists='append', index=False)
    
    #Inserting data into 
    ItemHierarchyDim = item_sale[['DepartmentCode','FamilyCode','FamilyDes','CategoryCode','CategoryDes','ClassCode','ClassDes']].drop_duplicates(keep='last').astype(str)
    ItemHierarchyDim.to_sql('ItemHierarchyDim', engine, if_exists='append', index=False)
    
    #Inserting data into ItemJunkDim
    ItemJunkDim = item_sale[['StoreBrand','Status']].drop_duplicates(keep='first')
    ItemJunkDim.to_sql('ItemJunkDim', engine, if_exists='append', index=False)
    
    #Inserting into scraped StoreServicesDim
    scrapedDF.to_sql('StoreServiceDim', engine, if_exists='append', index=False)
    
    # Date Dimension 
    temp = pd.DatetimeIndex(saleDF['TransDatetime(GMT)'])
    saleDF['Date'] = saleDF['TransDatetime(GMT)']
    saleDF['Year_int'] = temp.year
    saleDF['Month_int'] = temp.month
    saleDF['Month_abbr'] = saleDF['Month_int'].apply(lambda x: calendar.month_abbr[x])
    saleDF['Day_int'] = temp.day
    saleDF['DayOfWeek_int'] = temp.dayofweek
    saleDF['DayOfWeek_char'] = saleDF['DayOfWeek_int'].astype(str)
    saleDF['DayOfYear_int'] = temp.dayofyear
    
    saleDF[['Date','Year_int','Month_int','Month_abbr','Day_int','DayOfWeek_int','DayOfWeek_char',
            'DayOfYear_int']].drop_duplicates(keep='first').to_sql('DateDim', engine, if_exists='append', index=False)

    #Time Dimension 
    saleDF['Time'] = temp.time.astype(str)
    saleDF['Hour_24_int'] = temp.hour
    saleDF['Minute_int'] = temp.minute
    saleDF['Second_int'] = temp.second
    temp_12hour = saleDF['TransDatetime(GMT)'] + timedelta(hours=12)
    saleDF['Hour_12_int'] = pd.DatetimeIndex(temp_12hour).hour
    #print(saleDF)
   
    saleDF[['Time','Hour_24_int','Minute_int','Second_int','Hour_12_int']].drop_duplicates(keep='first').to_sql('TimeDim', engine, if_exists='append', index=False)
    
    


