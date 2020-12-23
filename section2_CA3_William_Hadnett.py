#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: williamhadnett D00223305
"""

import pymongo
import os


os.chdir('/Users/williamhadnett/Documents/Data_Science/Data_Science_CA3_William_Hadnett')
import atlasCredentials
# =============================================================================
# Connect to MongoDB
# =============================================================================

connection = "mongodb+srv://"+atlasCredentials.username+":"+atlasCredentials.password+"@cluster0.gh4kb.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection)

mydb = client['test']

shopcol = mydb['websiteshop']

# =============================================================================
# Product Association
# =============================================================================

#Find top ten products for association analysis based on quantity purchased.
unwind = {'$unwind':'$Basket'}
group = {'$group': {'_id': '$Basket.StockCode', 'count': {'$sum': 1}}}
sort={'$sort':{'count':-1}}
limit={'$limit': 10}
top10 = list(shopcol.aggregate([unwind,group,sort,limit]))
print(top10)
# [{'_id': '85123A', 'count': 320}, {'_id': '22423', 'count': 211}, 
# {'_id': '22469', 'count': 182}, {'_id': '22834', 'count': 162}, 
# {'_id': '22111', 'count': 160}, {'_id': '22961', 'count': 160}, 
# {'_id': '21485', 'count': 155}, {'_id': '22470', 'count': 152}, 
# {'_id': '22113', 'count': 146}, {'_id': '22112', 'count': 143}]

# =============================================================================
# Product Association - Product 85123A - Product 21212 - (Benchmark)
# =============================================================================

group = {'$group': {'_id': 0, 'total': {'$sum': 1}}}
totalDocs = list(shopcol.aggregate([group]))
print(totalDocs)

# Support(x) =  # of transactions in which x appears/total transactions

query =  {'Basket.StockCode': '85123A'}
support85123A = shopcol.count_documents(query) / totalDocs[0]['total']
print(support85123A)
# Support 85123A bought:  0.153 

query =  {'Basket.StockCode': {'$all': ['85123A', '21212']}}
supportBoth = shopcol.count_documents(query) / totalDocs[0]['total']
print(supportBoth)
# Support Both bought: 0.0105

#Confidence that 21212 will be bought when 85123A is bought.
#Conf(85123A -> 21212) = supp(85123A and 21212)/ supp(85123A)

conf = supportBoth / support85123A
print(conf)
# conf: 0.06862745098039216
 
# Lift 
query =  {'Basket.StockCode': '21212'}
support21212 = shopcol.count_documents(query) / totalDocs[0]['total']
print(support21212)
# 0.056

#Life(85123A -> 21212) = supp(85123A and 21212)/ supp(85123A) * supp(21212)
lift = (supportBoth / support85123A) * support21212
print(lift)
# Lift: 0.003843137254901961
# So the support for 21212 is 0.004% more likely to bough if the basket contains
# product 85123A than in general.

# =============================================================================
# Product Association - Generalize formula for top 10
# =============================================================================

# The above support, confidence and lift will act as a bench mark to ensure that the 
# calculates for the top ten are carried out correctly. 
def findAssoication(mongoResponse):
    
    pairs = findPairs(mongoResponse)
    
    group = {'$group': {'_id': 0, 'total': {'$sum': 1}}}
    totalDocs = list(shopcol.aggregate([group]))
    
    for i in pairs:
        # Support(x) =  # of transactions in which x appears/total transactions
        query =  {'Basket.StockCode': i[0]['_id']}
        supportItem1 = shopcol.count_documents(query) / totalDocs[0]['total']
        print('Support for Item ',i[0]['_id'],': ',supportItem1)

        query =  {'Basket.StockCode': {'$all': [i[0]['_id'], i[1]['_id']]}}
        supportBoth = shopcol.count_documents(query) / totalDocs[0]['total']
        print('Support Both: ',supportBoth)
        
        #Confidence that Item 1 will be bought when Item 2 is bought.
        #Conf(Item 1 -> Item 2) = supp(Item 1 and Item 2)/ supp(Item 1)
        
        conf = supportBoth / supportItem1
        print('Confidence: ',conf)
        # conf: 0.06862745098039216
         
        # Lift 
        query =  {'Basket.StockCode': i[1]['_id']}
        supportItem2 = shopcol.count_documents(query) / totalDocs[0]['total']
        print('Support Item ',i[1]['_id'],': ',supportItem2)
        # 0.056
        
        #Lift(Item 1 -> Item 2) = supp(Item 1 and Item 2)/ supp(Item1) * supp(Item2)
        lift = supportBoth / (supportItem1 * supportItem2)
        print('Lift ',i[0]['_id'],' -> ',i[1]['_id'],': ',lift)
        
        print("\n")


def findPairs(mongoResponse):
    
    it = iter(mongoResponse)
    pairs = list(zip(it, it))
    return pairs


findAssoication(top10)







