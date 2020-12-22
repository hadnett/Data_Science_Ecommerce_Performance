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
group = {'$group': {'_id': '$Basket.StockCode', 'count': {'$sum': '$Basket.Quantity'}}}
sort={'$sort':{'count':-1}}
limit = {'$limit':10}
result = list(shopcol.aggregate([unwind,group,sort,limit]))
print(result)
# [{'_id': '85123A', 'count': 8508}, {'_id': '21212', 'count': 6246}, 
# {'_id': '84077', 'count': 6051}, {'_id': '85099B', 'count': 4048}, 
# {'_id': '22834', 'count': 3914}, {'_id': '22469', 'count': 3857}, 
# {'_id': '22492', 'count': 3600}, {'_id': '22197', 'count': 3425}, 
# {'_id': '21108', 'count': 3303}, {'_id': '84879', 'count': 3280}]

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

#Life(85123A -> 21212) = supp(85123A and 21212)/ supp(85123A) * supp(21212)
lift = (supportBoth / support85123A) * support21212
print(lift)
# Lift: 0.003843137254901961
# So the support for 21212 is 0.004% more likely to bough if the basket contains
# product 85123A than in general.
















