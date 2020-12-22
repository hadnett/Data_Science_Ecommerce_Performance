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

amazoncol = mydb['amazonshop']
ebaycol = mydb['ebayshop']

# =============================================================================
# Question 1 - Number of Visits by Gender
# =============================================================================

def mapper(collIn):
    for doc in collIn:
        yield (doc['Customer']['Gender'], 1)
        

def reducer(mapDataIn):
    out = {}
    for word in mapDataIn:
        if word[0] in out.keys():
            out[word[0]] = out[word[0]] + word[1]
        else:
            out[word[0]] = word[1]
            
    return out


def reducerCols(reduceCol1, reduceCol2):
    out = reduceCol1
    for key, value in reduceCol2.items():
        out[key] = out.get(key, 0) + value
    return out


result1 = list(amazoncol.find({}))
result2 = list(ebaycol.find({}))
mapRes1 = mapper(result1)
mapRes2 = mapper(result2)

reduceCol1 = reducer(mapRes1)
reduceCol2 = reducer(mapRes2)

out = reducerCols(reduceCol1, reduceCol2)
# Number of visits across amazon and ebay by gender:
# {'Male': 120, 'Female': 279}

# =============================================================================
# Question 2 - Maximum Total Basket Price Across Both Stores and Customers
# =============================================================================

def mapper(collIn):
    for doc in collIn:
        yield (doc['Customer']['Gender'], 1)
        

def reducer(mapDataIn):
    out = {}
    for word in mapDataIn:
        if word[0] in out.keys():
            out[word[0]] = out[word[0]] + word[1]
        else:
            out[word[0]] = word[1]
            
    return out


def reducerCols(reduceCol1, reduceCol2):
    out = reduceCol1
    for key, value in reduceCol2.items():
        out[key] = out.get(key, 0) + value
    return out


result1 = list(amazoncol.find({}))
result2 = list(ebaycol.find({}))
mapRes1 = mapper(result1)
mapRes2 = mapper(result2)

reduceCol1 = reducer(mapRes1)
reduceCol2 = reducer(mapRes2)

out = reducerCols(reduceCol1, reduceCol2)
# Number of visits across amazon and ebay by gender:
# {'Male': 120, 'Female': 279}





