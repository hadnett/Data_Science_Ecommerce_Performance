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

