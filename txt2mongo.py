# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 18:40:43 2018

@author: Vinod, Rushan
"""

import pandas as pd
from pymongo import MongoClient

def cleanSalesTrx(path,filename):
    """
    Sales Transaction file has few junk characters at the start  of every row and empty column at the end.
    This function removes the junk characters and empty column and creates a cleaner version of the file.
    Return full file name to calling program.
    """
    messyFile = open(path+filename,encoding='latin_1')
    tidyFile = open(path+'SalesTrxCln.txt','a')
    for line in messyFile.readlines():
        tidyFile.write(line[4:]+'\n')
    tidyFile.close()
    print('Sales Transaction File is cleaned')
    return(tidyFile.name)

def extractSalesTrx(filename):
    """
    This function extracts the sales transaction data from cleaned Sales file to python
    It returns the sales transaction data frame
    """
    salesCols = ['StoreNum','Register','TransNum','TransDate','TransTime','BusDate','UPC','Item_Id','DeptNum','ItemQuantity',
             'WeightAmt','SalesAmt','CostAmt','CashierNum','PriceType','ServiceType','TenderType','LoyaltyCardNumber'] 
    salesTrx = pd.read_csv(filename,sep='|',
                     header=None,
                     names=salesCols,
                     converters={6: str},
                     parse_dates= [3,5])
    salesTrx.name = 'SalesTrx'
    print(salesTrx.dtypes)
    print('Number of NA values in each column of Data Frame \n')
    print(salesTrx.isna().sum())
    return(salesTrx)

def extractItemAttr(path,filename):
    """
    This function extracts the sales transaction data from Item Attribute file to python
    It returns the Item Atrribute data frame
    """
    item_attr = pd.read_csv(path + filename,sep = '|',skiprows=3,header = None, converters={0: str})
    col_names = ["UPC","ItemPosDes","ItemAttributeDes","ItemAttributeValue","AttributeStartDate","AttributeEndDate"]
    item_attr.columns = col_names
    item_attr['AttributeStartDate'] =  pd.to_datetime(item_attr['AttributeStartDate'], format='%Y-%M-%d')
    item_attr['AttributeEndDate'] =  pd.to_datetime(item_attr['AttributeEndDate'], format='%Y-%m-%d')
    print(item_attr.dtypes)
    return (item_attr)

    
def extractCustomer(path,filename):
    """
    This function extracts the sales transaction data from Customer file to python
    It returns the Customer data frame
    """
    cust_list = pd.read_csv(path + filename,sep = '|',header = None,encoding='latin_1')
    col_names = ["LoyaltyCardNum","HouseholdNum","MemberFavStore","City","State","ZipCode","ec"]
    cust_list.columns = col_names
    cust_list = cust_list.drop(['ec'],axis = 1)
    cust_list['LoyaltyCardNum'] = cust_list['LoyaltyCardNum'].fillna(-999).astype(int)
    cust_list['HouseholdNum'] = cust_list['HouseholdNum'].fillna(-999).astype(int)
    cust_list['MemberFavStore'] = cust_list['MemberFavStore'].fillna(-999).astype(int)
    print(cust_list.dtypes)
    print('Number of NA values in each column of Data Frame \n')
    print(cust_list.isna().sum())
    return (cust_list)

def extractItemList(path,filename):
    item_list = pd.read_csv(path+filename,sep = '|',header = None,encoding='latin1', converters={0: str})
    col_names = ["UPC","Item_Id","Status","LongDes","ShortDes","ClassCode","ClassDes","CategoryCode","CategoryDes",
             "FamilyCode","FamilyDes","DepartmentCode","StoreBrand","ExtraDes","ec"]
    item_list.columns = col_names
    item_list = item_list.drop(['ec'],axis = 1)
    print(item_list.dtypes)
    return(item_list)
    
def extractStoreInfo(path,filename):
    store_info = pd.read_csv(path+filename,sep = '|',header = 1 ,encoding='latin1')
    col_names = ["store_nbr", "store_nm", "actv_rec_ind", "store_addr_line_1", "store_city_nm",
                 "store_state_prv_Cd", "store_post_cd", "store_sq_feet", "store_rgn_desc", "store_clst_desc","ec"]
    store_info.columns = col_names
    store_info = store_info.drop(['ec'],axis = 1)
    store_info['store_nbr'] = store_info['store_nbr'].str.extract('(\d+)')
    store_info['store_nbr'] = store_info['store_nbr'].fillna(-999).astype(int)
    store_info['store_post_cd'] = store_info['store_post_cd'].fillna(-999).astype(int)
    store_info['store_sq_feet'] = store_info['store_sq_feet'].fillna(-999).astype(int)    
    print('Number of NA values in each column of Data Frame \n')
    print(store_info.isna().sum())
    print(store_info.dtypes)
    return(store_info)

def connectToMongo(hostname,port):
    """
    This function estalishes connection to Mongo DB and returns connection object
    """
    mongoConnect = MongoClient(host='127.0.0.1',port=27017)
    return mongoConnect

def insertIntoMongo(database,collection,mongo_conn,df):
    """
    This function insert the data into appropriate collection in mongo DB
    """
    db = mongo_conn.get_database(database)
    db.get_collection(collection).insert_many(df.to_dict(orient='record'))

if __name__ == '__main__':
    
    PATH = '/home/rusanshakya/Dropbox/BusinessIntelligence/Project/DataFiles/'
    SalesFile = 'sls_dtl.txt'
    ItemAttrFile = 'Item_Attr.txt'
    CustFile = 'customer_List.txt'
    ItemListFile = 'Item_List.txt'
    StoreFile = 'store_list.txt'
    
    SalesTrxClean = cleanSalesTrx(PATH,SalesFile)
    salesDF = extractSalesTrx(SalesTrxClean)
    itemAttrDF = extractItemAttr(PATH,ItemAttrFile)
    custDF = extractCustomer(PATH,CustFile)
    itemListDF = extractItemList(PATH,ItemListFile)
    storeDF = extractStoreInfo(PATH,StoreFile)

    
    conn_obj = connectToMongo(hostname='127.0.0.1',port=27017)
    insertIntoMongo('BIProject','SalesTrx',conn_obj,salesDF)
    insertIntoMongo('BIProject','ItemAttribute',conn_obj,itemAttrDF)
    insertIntoMongo('BIProject','Customer',conn_obj,custDF)
    insertIntoMongo('BIProject','ItemList',conn_obj,itemListDF)
    insertIntoMongo('BIProject','StoreInfo',conn_obj,storeDF)
    
    
    
    
    





