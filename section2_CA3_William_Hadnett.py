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

#Find top ten products for association analysis
unwind = {'$unwind':'$Basket'}
group = {'$group': {'_id': '$Basket.StockCode', 'count': {'$sum': 1}}}
sort={'$sort':{'count':-1}}
limit = {'$limit':10}
result = list(shopcol.aggregate([unwind,group,sort,limit]))
print(result)
# [{'_id': '85123A', 'count': 320}, {'_id': '22423', 'count': 211}, 
# {'_id': '22469', 'count': 182}, {'_id': '22834', 'count': 162}, 
# {'_id': '22961', 'count': 160}, {'_id': '22111', 'count': 160}, 
# {'_id': '21485', 'count': 155}, {'_id': '22470', 'count': 152}, 
# {'_id': '22113', 'count': 146}, {'_id': '22112', 'count': 143}]
