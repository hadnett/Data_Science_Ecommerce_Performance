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

$group: {
         _id : '$name',
         name : { $first: '$name' },
         age : { $first: '$age' },
         sex : { $first: '$sex' },
         province : { $first: '$province' },
         city : { $first: '$city' },
         area : { $first: '$area' },
         address : { $first: '$address' },
         count : { $sum: 1 },
      }

unwind = {'$unwind':'$Basket'}
group = {'$group': {'_id': '$Basket.StockCode', 'count': {'$sum': 1}}}
sort={'$sort':{'count':-1}}
limit = {'$limit':10}
result = list(shopcol.aggregate([unwind,group,sort,limit]))
print(result)